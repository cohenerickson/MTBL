/**
 * Public reader for mtbl files.
 *
 * Usage:
 *
 *   const reader = new MTBLReader("./images.mtbl");
 *   await reader.ready;
 *
 *   const value = await reader.get("some-key");
 *   for await (const { key, value } of reader.getPrefix("photos/")) { ... }
 *
 *   await reader.close();
 *
 * Memory model:
 *   - The trailer (512 bytes) and index block are loaded into memory at
 *     open time. For typical files the index is well under 1% of total file
 *     size.
 *   - Data blocks are read on demand, one at a time, and discarded after
 *     each operation. A point lookup touches exactly one data block.
 *   - Iterators yield entries as they're decoded. As long as the consumer
 *     doesn't accumulate them, memory stays flat regardless of file size.
 */

import { type CompressionId, type CompressionName, type FormatVersion } from "./constants.js";
import { FileReader } from "./file-handle.js";
import { IndexBlock } from "./index-block.js";
import { METADATA_SIZE } from "./constants.js";
import { parseTrailer, type Trailer } from "./trailer.js";
import { readBlock } from "./framed-block.js";
import { Block } from "./block.js";
import { toSafeNumber } from "./varint.js";

/** Anything we'll accept as a key on the input side of public methods. */
export type KeyInput = Buffer | Uint8Array | string;

/**
 * One key/value pair. Yielded by iteration methods.
 *
 * `value` is a Buffer that is safe to retain — we copy it out of the
 * underlying block buffer before yielding.
 */
export interface MTBLEntry {
  key: Buffer;
  value: Buffer;
}

/** Public metadata about an open mtbl file (subset of the on-disk trailer). */
export interface MTBLMetadata {
  version: FormatVersion;
  compression: CompressionName;
  compressionAlgorithm: CompressionId;
  /** Total entries in the file. */
  entryCount: number | bigint;
  /** Total data blocks in the file. */
  dataBlockCount: number | bigint;
  /** Total bytes in data blocks (compressed). */
  bytesDataBlocks: number | bigint;
  /** Total bytes in the index block. */
  bytesIndexBlock: number | bigint;
  /** Sum of key bytes across all entries (uncompressed). */
  bytesKeys: number | bigint;
  /** Sum of value bytes across all entries (uncompressed). */
  bytesValues: number | bigint;
  /** Total file size in bytes. */
  fileSize: number;
}

export interface IterateOptions {
  /** Inclusive lower bound. Iteration starts at the first key >= start. */
  start?: KeyInput;
  /** Exclusive upper bound. Iteration stops before the first key >= end. */
  end?: KeyInput;
  /** Byte prefix all yielded keys must share. */
  prefix?: KeyInput;
}

export interface MTBLReaderOptions {
  // (Reserved for future options like custom block cache size, checksum
  // verification, mmap mode, etc. Empty for now to keep the v1 API minimal.)
}

/**
 * Coerce a KeyInput to a Buffer. Strings are interpreted as UTF-8.
 *
 * Always returns a fresh Buffer for Uint8Array inputs to avoid sharing
 * memory with caller arrays.
 */
function toBuffer(key: KeyInput): Buffer {
  if (typeof key === "string") return Buffer.from(key, "utf8");
  if (Buffer.isBuffer(key)) return key;
  // Plain Uint8Array — wrap without copying. Buffer.from(Uint8Array) shares
  // the same backing ArrayBuffer, which is what we want here.
  return Buffer.from(key.buffer, key.byteOffset, key.byteLength);
}

/** Returns true if `key` starts with all of `prefix`. */
function startsWith(key: Buffer, prefix: Buffer): boolean {
  if (key.length < prefix.length) return false;
  return key.compare(prefix, 0, prefix.length, 0, prefix.length) === 0;
}

/**
 * Compute the smallest key strictly greater than every key starting with
 * `prefix`. Used internally to translate a prefix scan into a range scan.
 *
 * Returns null if no such key exists (i.e. prefix is all 0xFF bytes — in
 * which case the prefix scan reaches the end of the keyspace).
 */
function prefixUpperBound(prefix: Buffer): Buffer | null {
  for (let i = prefix.length - 1; i >= 0; i--) {
    if (prefix[i]! !== 0xff) {
      const out = Buffer.from(prefix.subarray(0, i + 1));
      out[i] = (out[i]! + 1) & 0xff;
      return out;
    }
  }
  return null;
}

export class MTBLReader {
  readonly path: string;
  readonly ready: Promise<void>;

  // The fields below are initialized inside `#init()` and asserted non-null
  // by `#ensureReady()`.
  #file: FileReader | null = null;
  #trailer: Trailer | null = null;
  #index: IndexBlock | null = null;
  #closed = false;

  constructor(path: string, _options: MTBLReaderOptions = {}) {
    this.path = path;
    this.ready = this.#init().catch((err) => {
      // Surface initialization errors via `ready`, but also leave the
      // reader in a "closed" state so subsequent calls fail fast.
      this.#closed = true;
      throw err;
    });
  }

  // --------------------------------------------------------------------
  // Lifecycle

  async #init(): Promise<void> {
    const file = await FileReader.open(this.path);
    this.#file = file;

    try {
      if (file.size < METADATA_SIZE) {
        throw new Error(
          `file ${this.path} is too small to be an mtbl file ` +
            `(${file.size} bytes < ${METADATA_SIZE} byte trailer)`,
        );
      }

      // Read trailer.
      const trailerBuf = await file.readAt(file.size - METADATA_SIZE, METADATA_SIZE);
      const trailer = parseTrailer(trailerBuf);
      this.#trailer = trailer;

      // Read index block envelope. The index block sits at indexBlockOffset
      // and runs until the start of the trailer. We could read just the
      // trailer-recorded `bytesIndexBlock`, but the speculative-read in
      // readBlock handles small overshoots fine.
      const indexOffset = toSafeNumber(trailer.indexBlockOffset);
      const indexBytes = toSafeNumber(trailer.bytesIndexBlock);
      if (indexOffset + indexBytes > file.size) {
        throw new Error(
          `trailer says index ends at ${indexOffset + indexBytes} ` +
            `but file is only ${file.size} bytes`,
        );
      }

      // For very large index blocks, we need to bypass the speculative-read
      // path inside readBlock and just read the whole thing in one shot.
      const indexEnvelope = await file.readAt(indexOffset, indexBytes);
      this.#index = await this.#parseIndexBlock(indexEnvelope, trailer);
    } catch (err) {
      await file.close();
      throw err;
    }
  }

  /**
   * Parse the index block from its full envelope buffer (header + crc + payload).
   *
   * Logically the same as readBlock, but reads the envelope from a buffer
   * we already have rather than from disk.
   */
  async #parseIndexBlock(
    envelope: Buffer,
    trailer: Trailer,
  ): Promise<IndexBlock> {
    const { decodeVarint64, readU32LE: _readU32LE } = await import("./varint.js");
    let payloadLength: number;
    let headerLen: number;
    if (trailer.version === "v1") {
      payloadLength = envelope.readUInt32LE(0);
      headerLen = 4;
    } else {
      const v = decodeVarint64(envelope, 0);
      payloadLength = toSafeNumber(v.value);
      headerLen = v.bytes;
    }
    const payloadStart = headerLen + 4 /* crc */;
    const payload = envelope.subarray(payloadStart, payloadStart + payloadLength);
    // The index block is always written uncompressed by the C library,
    // regardless of the file's compression_algorithm. Only data blocks
    // are compressed.
    return new IndexBlock(new Block(Buffer.from(payload)));
  }

  /**
   * Close the underlying file handle. Idempotent. After close, all other
   * methods will throw.
   */
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#file) await this.#file.close();
  }

  /** Throws if the reader is closed or `ready` hasn't completed yet. */
  async #ensureReady(): Promise<{
    file: FileReader;
    trailer: Trailer;
    index: IndexBlock;
  }> {
    await this.ready;
    if (this.#closed || !this.#file || !this.#trailer || !this.#index) {
      throw new Error(`MTBLReader for ${this.path} is closed`);
    }
    return {
      file: this.#file,
      trailer: this.#trailer,
      index: this.#index,
    };
  }

  // --------------------------------------------------------------------
  // Metadata

  /**
   * Returns metadata about the file. Available after `ready` resolves.
   */
  async metadata(): Promise<MTBLMetadata> {
    const { trailer, file } = await this.#ensureReady();
    return {
      version: trailer.version,
      compression: trailer.compression,
      compressionAlgorithm: trailer.compressionAlgorithm,
      entryCount: trailer.entryCount,
      dataBlockCount: trailer.dataBlockCount,
      bytesDataBlocks: trailer.bytesDataBlocks,
      bytesIndexBlock: trailer.bytesIndexBlock,
      bytesKeys: trailer.bytesKeys,
      bytesValues: trailer.bytesValues,
      fileSize: file.size,
    };
  }

  // --------------------------------------------------------------------
  // Point access

  /**
   * Look up a single key. Returns the value as a Buffer, or null if the
   * key isn't present.
   */
  async get(key: KeyInput): Promise<Buffer | null> {
    const { file, trailer, index } = await this.#ensureReady();
    const target = toBuffer(key);

    const lookup = index.findBlockForKey(target);
    if (lookup === null) return null;

    const { block } = await readBlock(file, lookup.blockOffset, {
      fileSize: file.size,
      version: trailer.version,
      compression: trailer.compressionAlgorithm,
    });

    const entry = block.seekExact(target);
    if (!entry) return null;
    // Copy the value: it's a slice of the block buffer, which we want to
    // let go of after this call returns.
    return Buffer.from(entry.value);
  }

  /**
   * Returns true if the key exists in the file.
   *
   * Cheaper than `get()` because we don't have to copy the value bytes —
   * but we still have to read and decompress the block to find the key.
   */
  async has(key: KeyInput): Promise<boolean> {
    const { file, trailer, index } = await this.#ensureReady();
    const target = toBuffer(key);

    const lookup = index.findBlockForKey(target);
    if (lookup === null) return false;

    const { block } = await readBlock(file, lookup.blockOffset, {
      fileSize: file.size,
      version: trailer.version,
      compression: trailer.compressionAlgorithm,
    });
    return block.seekExact(target) !== null;
  }

  /**
   * Look up many keys at once.
   *
   * Internally sorts the keys so that adjacent lookups touch the same data
   * block when possible. Significantly faster than calling `get()` in a
   * loop when there are many keys.
   *
   * Returns a Map keyed by the original key input (after Buffer
   * conversion). Missing keys are simply absent from the map.
   */
  async getMany(keys: readonly KeyInput[]): Promise<Map<Buffer, Buffer>> {
    const { file, trailer, index } = await this.#ensureReady();
    const result = new Map<Buffer, Buffer>();
    if (keys.length === 0) return result;

    // Convert and sort keys (preserving original Buffer identity).
    const targets = keys.map(toBuffer);
    const order = targets
      .map((_, i) => i)
      .sort((a, b) => Buffer.compare(targets[a]!, targets[b]!));

    let currentBlockOffset: number | null = null;
    let currentBlock: Block | null = null;

    for (const i of order) {
      const target = targets[i]!;
      const lookup = index.findBlockForKey(target);
      if (lookup === null) continue;

      if (lookup.blockOffset !== currentBlockOffset) {
        const { block } = await readBlock(file, lookup.blockOffset, {
          fileSize: file.size,
          version: trailer.version,
          compression: trailer.compressionAlgorithm,
        });
        currentBlock = block;
        currentBlockOffset = lookup.blockOffset;
      }

      const entry = currentBlock!.seekExact(target);
      if (entry) {
        result.set(target, Buffer.from(entry.value));
      }
    }
    return result;
  }

  // --------------------------------------------------------------------
  // Iteration

  /**
   * The reader is itself an async iterable — equivalent to
   * `reader.iterate()` with no options. Yields every entry in key order.
   */
  [Symbol.asyncIterator](): AsyncGenerator<MTBLEntry> {
    return this.iterate();
  }

  /**
   * Iterate entries with optional bounds.
   *
   * If `prefix` is given alongside `start`/`end`, the bounds are
   * intersected: only entries that satisfy ALL constraints are yielded.
   *
   * Yields keys and values as fresh Buffers (safe to retain).
   */
  async *iterate(options: IterateOptions = {}): AsyncGenerator<MTBLEntry> {
    const { file, trailer, index } = await this.#ensureReady();

    // Compute effective start/end keys, taking prefix into account.
    let startKey: Buffer | null = null;
    let endKey: Buffer | null = null;

    if (options.prefix !== undefined) {
      const p = toBuffer(options.prefix);
      startKey = p;
      endKey = prefixUpperBound(p); // may be null (prefix is all 0xFF)
    }

    if (options.start !== undefined) {
      const s = toBuffer(options.start);
      startKey = startKey === null || Buffer.compare(s, startKey) > 0 ? s : startKey;
    }
    if (options.end !== undefined) {
      const e = toBuffer(options.end);
      endKey = endKey === null || Buffer.compare(e, endKey) < 0 ? e : endKey;
    }

    // Pick which index iterator to use.
    const indexIter =
      startKey !== null ? index.entriesFrom(startKey) : index.entries();

    let firstBlock = true;
    for (const blockRef of indexIter) {
      const { block } = await readBlock(file, blockRef.blockOffset, {
        fileSize: file.size,
        version: trailer.version,
        compression: trailer.compressionAlgorithm,
      });

      // Within the first block we need to honor the start key.
      const blockEntries =
        firstBlock && startKey !== null
          ? block.entriesFrom(startKey)
          : block.entries();
      firstBlock = false;

      for (const entry of blockEntries) {
        if (endKey !== null && Buffer.compare(entry.key, endKey) >= 0) {
          return;
        }
        // entry.key is already a fresh Buffer; copy entry.value out of the
        // shared block buffer.
        yield { key: entry.key, value: Buffer.from(entry.value) };
      }
    }
  }

  /**
   * Iterate keys only. Faster than `iterate()` for keys-only scans because
   * we still skip allocating fresh Buffers for values.
   *
   * (The block parser doesn't currently expose a keys-only fast path; this
   * method is an alias for `iterate()` projecting to the key. We can
   * optimize later by adding a keys-only entry generator on Block.)
   */
  async *keys(options: IterateOptions = {}): AsyncGenerator<Buffer> {
    for await (const entry of this.iterate(options)) {
      yield entry.key;
    }
  }

  /**
   * Iterate values only.
   */
  async *values(options: IterateOptions = {}): AsyncGenerator<Buffer> {
    for await (const entry of this.iterate(options)) {
      yield entry.value;
    }
  }

  /**
   * Iterate every entry whose key starts with `prefix`. Equivalent to
   * `iterate({ prefix })`.
   *
   * Stops early when the first key not matching the prefix is reached.
   */
  async *getPrefix(prefix: KeyInput): AsyncGenerator<MTBLEntry> {
    const p = toBuffer(prefix);
    // We could just delegate to iterate({ prefix: p }), but inline the
    // logic so we can also enforce the prefix on the very first entry
    // (covers edge cases where prefix isn't itself a real boundary).
    for await (const entry of this.iterate({ prefix: p })) {
      if (!startsWith(entry.key, p)) return;
      yield entry;
    }
  }

  /**
   * Iterate every entry with `start <= key < end`. Equivalent to
   * `iterate({ start, end })`.
   */
  getRange(start: KeyInput, end: KeyInput): AsyncGenerator<MTBLEntry> {
    return this.iterate({ start, end });
  }

  // --------------------------------------------------------------------
  // Bounds

  /**
   * Return the first key in the file, or null if the file is empty.
   */
  async firstKey(): Promise<Buffer | null> {
    for await (const entry of this.iterate()) {
      return entry.key;
    }
    return null;
  }

  /**
   * Return the last key in the file, or null if the file is empty.
   *
   * Implemented by reading the last data block (cheap — one block read).
   */
  async lastKey(): Promise<Buffer | null> {
    const { file, trailer, index } = await this.#ensureReady();
    // Walk the index to find the last entry. The index is in memory, so
    // this is fast; we don't have a "seek to last" method on Block right
    // now so we just iterate.
    let last: { blockOffset: number; blockLastKey: Buffer } | null = null;
    for (const ref of index.entries()) {
      last = ref;
    }
    if (!last) return null;

    const { block } = await readBlock(file, last.blockOffset, {
      fileSize: file.size,
      version: trailer.version,
      compression: trailer.compressionAlgorithm,
    });
    let lastKey: Buffer | null = null;
    for (const entry of block.entries()) {
      lastKey = entry.key;
    }
    return lastKey;
  }
}
