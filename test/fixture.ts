/**
 * Generates spec-compliant mtbl files for testing the reader.
 *
 * Implements just enough of the writer to round-trip the reader: NONE and
 * ZLIB compression, V2 framing only. The block format mirrors LevelDB /
 * the reference C library precisely.
 *
 * NOT intended as a production writer — no concurrency, no checksums, no
 * tunable block size beyond what we expose for tests.
 */

import { writeFileSync } from "node:fs";
import { deflateSync } from "node:zlib";

const MTBL_MAGIC_V2 = 0x4d54424c;
const METADATA_SIZE = 512;

// Compression algorithms (subset).
export const COMPRESSION_NONE = 0;
export const COMPRESSION_ZLIB = 2;

export interface FixtureOptions {
  blockSize?: number;
  blockRestartInterval?: number;
  compression?: typeof COMPRESSION_NONE | typeof COMPRESSION_ZLIB;
}

interface PendingEntry {
  key: Buffer;
  value: Buffer;
}

/** Encode an unsigned 32-bit varint to a byte array. */
function encodeVarint32(value: number, out: number[]): void {
  while (value >= 0x80) {
    out.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  out.push(value & 0x7f);
}

/** Encode an unsigned 64-bit varint (we use plain numbers; safe for our sizes). */
function encodeVarint64(value: number, out: number[]): void {
  // For our test fixtures, offsets fit in 32 bits — use the same routine.
  encodeVarint32(value, out);
}

function writeU32LE(value: number): Buffer {
  const b = Buffer.allocUnsafe(4);
  b.writeUInt32LE(value >>> 0, 0);
  return b;
}

function writeU64LE(value: number): Buffer {
  const b = Buffer.alloc(8);
  // Safe-integer values only.
  const lo = value >>> 0;
  const hi = Math.floor(value / 0x1_0000_0000);
  b.writeUInt32LE(lo, 0);
  b.writeUInt32LE(hi, 4);
  return b;
}

/**
 * Build a single block payload (the LevelDB-style entries + restart array).
 */
function buildBlock(
  entries: PendingEntry[],
  restartInterval: number,
): { payload: Buffer; lastKey: Buffer } {
  const body: number[] = [];
  const restarts: number[] = [];
  let prevKey: Buffer | null = null;

  for (let i = 0; i < entries.length; i++) {
    const { key, value } = entries[i]!;

    let shared = 0;
    // Reset prefix sharing every restartInterval entries.
    if (i % restartInterval === 0) {
      restarts.push(body.length);
      prevKey = null;
    }
    if (prevKey !== null) {
      const max = Math.min(prevKey.length, key.length);
      while (shared < max && prevKey[shared] === key[shared]) shared++;
    }
    const nonShared = key.length - shared;

    encodeVarint32(shared, body);
    encodeVarint32(nonShared, body);
    encodeVarint32(value.length, body);
    for (let j = shared; j < key.length; j++) body.push(key[j]!);
    for (let j = 0; j < value.length; j++) body.push(value[j]!);
    prevKey = key;
  }

  // Append restart array (u32 LE each) + num_restarts (u32 LE).
  const restartArr = Buffer.allocUnsafe(restarts.length * 4 + 4);
  for (let i = 0; i < restarts.length; i++) {
    restartArr.writeUInt32LE(restarts[i]!, i * 4);
  }
  restartArr.writeUInt32LE(restarts.length, restarts.length * 4);

  const bodyBuf = Buffer.from(body);
  const payload = Buffer.concat([bodyBuf, restartArr]);
  return {
    payload,
    lastKey: entries[entries.length - 1]!.key,
  };
}

/**
 * Frame a block payload for on-disk storage: [varint64 length][u32 crc][payload].
 *
 * We write a zero CRC — the reference reader only verifies CRCs when
 * `verify_checksums` is enabled, which our reader doesn't expose.
 */
function frameBlock(payload: Buffer): Buffer {
  const lenBytes: number[] = [];
  encodeVarint64(payload.length, lenBytes);
  const crc = Buffer.alloc(4); // zero
  return Buffer.concat([Buffer.from(lenBytes), crc, payload]);
}

/** Build the 512-byte trailer. */
function buildTrailer(meta: {
  indexBlockOffset: number;
  dataBlockSize: number;
  compressionAlgorithm: number;
  countEntries: number;
  countDataBlocks: number;
  bytesDataBlocks: number;
  bytesIndexBlock: number;
  bytesKeys: number;
  bytesValues: number;
}): Buffer {
  const fields = Buffer.concat([
    writeU64LE(meta.indexBlockOffset),
    writeU64LE(meta.dataBlockSize),
    writeU64LE(meta.compressionAlgorithm),
    writeU64LE(meta.countEntries),
    writeU64LE(meta.countDataBlocks),
    writeU64LE(meta.bytesDataBlocks),
    writeU64LE(meta.bytesIndexBlock),
    writeU64LE(meta.bytesKeys),
    writeU64LE(meta.bytesValues),
  ]);
  const buf = Buffer.alloc(METADATA_SIZE);
  fields.copy(buf, 0);
  buf.writeUInt32LE(MTBL_MAGIC_V2, METADATA_SIZE - 4);
  return buf;
}

/**
 * Write an mtbl file from a sorted array of entries.
 *
 * @param path     output path
 * @param entries  entries in sorted key order, no duplicates
 * @param options  block size / restart interval / compression
 */
export function writeMtblFile(
  path: string,
  entries: PendingEntry[],
  options: FixtureOptions = {},
): void {
  const blockSize = options.blockSize ?? 4096;
  const restartInterval = options.blockRestartInterval ?? 16;
  const compression = options.compression ?? COMPRESSION_NONE;

  // Sanity check: keys must be sorted, no duplicates.
  for (let i = 1; i < entries.length; i++) {
    const cmp = Buffer.compare(entries[i - 1]!.key, entries[i]!.key);
    if (cmp >= 0) {
      throw new Error(
        `entries must be sorted with no duplicates: ` +
          `entries[${i - 1}] >= entries[${i}]`,
      );
    }
  }

  // Group entries into data blocks. Use a simple greedy size threshold:
  // accumulate entries until estimated payload size exceeds blockSize, then
  // emit. This matches the reference writer's behavior closely enough for
  // testing.
  const dataBlocks: { offset: number; lastKey: Buffer; framedSize: number }[] = [];
  const fileChunks: Buffer[] = [];
  let cursor = 0;
  let bytesKeys = 0;
  let bytesValues = 0;

  let pending: PendingEntry[] = [];
  let pendingApprox = 0;

  const flushBlock = () => {
    if (pending.length === 0) return;
    const { payload, lastKey } = buildBlock(pending, restartInterval);

    // Compress the block if needed.
    let onDisk = payload;
    if (compression === COMPRESSION_ZLIB) {
      onDisk = deflateSync(payload);
    }

    const framed = frameBlock(onDisk);
    fileChunks.push(framed);
    dataBlocks.push({
      offset: cursor,
      lastKey,
      framedSize: framed.length,
    });
    cursor += framed.length;
    pending = [];
    pendingApprox = 0;
  };

  for (const e of entries) {
    pending.push(e);
    pendingApprox += e.key.length + e.value.length + 6 /* varint headers */;
    bytesKeys += e.key.length;
    bytesValues += e.value.length;
    if (pendingApprox >= blockSize) {
      flushBlock();
    }
  }
  flushBlock();

  // Build the index block. Each index entry's key is a data block's lastKey
  // and value is a varint64-encoded offset.
  const indexEntries: PendingEntry[] = dataBlocks.map((db) => {
    const valueBytes: number[] = [];
    encodeVarint64(db.offset, valueBytes);
    return {
      key: db.lastKey,
      value: Buffer.from(valueBytes),
    };
  });

  const indexBlockOffset = cursor;
  const { payload: indexPayload } = buildBlock(indexEntries, restartInterval);
  // mtbl always compresses the index with the file's compression algorithm.
  let indexOnDisk = indexPayload;
  if (compression === COMPRESSION_ZLIB) {
    indexOnDisk = deflateSync(indexPayload);
  }
  const indexFramed = frameBlock(indexOnDisk);
  fileChunks.push(indexFramed);
  cursor += indexFramed.length;

  // Trailer.
  const trailer = buildTrailer({
    indexBlockOffset,
    dataBlockSize: blockSize,
    compressionAlgorithm: compression,
    countEntries: entries.length,
    countDataBlocks: dataBlocks.length,
    bytesDataBlocks: dataBlocks.reduce((s, d) => s + d.framedSize, 0),
    bytesIndexBlock: indexFramed.length,
    bytesKeys,
    bytesValues,
  });
  fileChunks.push(trailer);

  writeFileSync(path, Buffer.concat(fileChunks));
}
