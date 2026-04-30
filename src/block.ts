/**
 * Parser for an individual decompressed mtbl block.
 *
 * Block layout (LevelDB-style, used by both data blocks and the index block):
 *
 *   [entries...]              variable-length, prefix-compressed entries
 *   [restart array]           u32 (or u64) offsets into the entries region
 *                             — these are "restart points" where prefix
 *                             compression resets (shared = 0)
 *   [u32 LE num_restarts]     count of entries in the restart array
 *
 * Each entry has the shape:
 *
 *   [varint shared]           bytes shared with the previous key
 *   [varint non_shared]       suffix length unique to this key
 *   [varint value_length]     length of the value bytes
 *   [non_shared bytes]        key suffix
 *   [value_length bytes]      value bytes
 *
 * Restart points let you binary-search the block. mtbl's writer emits a
 * restart every 16 entries by default. At a restart point, `shared` is
 * always 0 — meaning the full key is materialized and binary search
 * comparisons can be done without first reconstructing prior keys.
 *
 * Restart-array width:
 *   The default is u32 entries. If the writer detects that the entries
 *   region is larger than UINT32_MAX, it switches to u64 entries. This is
 *   effectively dead code for normal data blocks (which are 8 KB by
 *   default) but the index block CAN be huge and may use u64 restarts.
 *   We mirror the C library's heuristic: assume u32, and if the inferred
 *   restart_offset > UINT32_MAX, re-parse as u64.
 */

import { decodeVarint32, readU32LE } from "./varint.js";

const UINT32_MAX = 0xffff_ffff;

export interface BlockEntry {
  /** Owned Buffer (safe to retain across block reads). */
  key: Buffer;
  /** Slice of block data — must be COPIED if retained beyond next read. */
  value: Buffer;
}

interface EntryHeader {
  shared: number;
  nonShared: number;
  valueLength: number;
  /** Byte offset in block data where the key suffix starts. */
  suffixOffset: number;
}

/**
 * Wraps a decompressed block buffer and provides random-access lookups.
 *
 * Construct one of these per block read. They're cheap to make (no copying
 * of the underlying buffer) and cheap to throw away.
 */
export class Block {
  readonly #data: Buffer;
  /** Byte offset of the start of the restart array. */
  readonly #restartsOffset: number;
  /** Number of restart points. */
  readonly #numRestarts: number;
  /** Bytes per restart entry: 4 for u32, 8 for u64. */
  readonly #restartWidth: 4 | 8;

  /**
   * @param data  decompressed block bytes
   */
  constructor(data: Buffer) {
    if (data.length < 4) {
      throw new Error(`block too small (${data.length} bytes)`);
    }
    this.#data = data;

    // Last 4 bytes = num_restarts (u32 LE).
    const numRestarts = readU32LE(data, data.length - 4);
    this.#numRestarts = numRestarts;
    if (numRestarts === 0) {
      throw new Error("block has zero restart points");
    }

    // Try u32 restart array first.
    let restartsOffset = data.length - 4 - numRestarts * 4;
    if (restartsOffset < 0) {
      throw new Error("block restart array overruns block");
    }

    if (restartsOffset > UINT32_MAX) {
      // Writer would have used u64 restarts. Recompute.
      restartsOffset = data.length - 4 - numRestarts * 8;
      if (restartsOffset < 0) {
        throw new Error("block u64 restart array overruns block");
      }
      this.#restartWidth = 8;
    } else {
      this.#restartWidth = 4;
    }

    this.#restartsOffset = restartsOffset;
  }

  /** Number of restart points in this block. */
  get numRestarts(): number {
    return this.#numRestarts;
  }

  /** Read the offset of the i-th restart point (an offset into entry-space). */
  #getRestart(i: number): number {
    const base = this.#restartsOffset + i * this.#restartWidth;
    if (this.#restartWidth === 4) {
      return readU32LE(this.#data, base);
    }
    // u64 restart. Restarts can't legally exceed Number.MAX_SAFE_INTEGER
    // for any practical block, so coerce.
    const lo = this.#data.readUInt32LE(base);
    const hi = this.#data.readUInt32LE(base + 4);
    if (hi > 0x1fffff) {
      throw new Error("restart offset exceeds safe integer range");
    }
    return hi * 0x1_0000_0000 + lo;
  }

  /**
   * Decode the entry header at `offset`, returning its shared/non_shared/value
   * lengths and the byte position where the key suffix starts.
   */
  #decodeEntryHeader(offset: number): EntryHeader {
    const data = this.#data;
    // Fast path: if all three header bytes are < 128, they're each a 1-byte
    // varint. The reference C library uses this same shortcut.
    if (offset + 3 <= data.length) {
      const a = data[offset]!;
      const b = data[offset + 1]!;
      const c = data[offset + 2]!;
      if ((a | b | c) < 128) {
        return {
          shared: a,
          nonShared: b,
          valueLength: c,
          suffixOffset: offset + 3,
        };
      }
    }
    // Slow path: full varint decode.
    const s = decodeVarint32(data, offset);
    const ns = decodeVarint32(data, offset + s.bytes);
    const vl = decodeVarint32(data, offset + s.bytes + ns.bytes);
    return {
      shared: s.value,
      nonShared: ns.value,
      valueLength: vl.value,
      suffixOffset: offset + s.bytes + ns.bytes + vl.bytes,
    };
  }

  /**
   * Iterate every key/value pair in this block in order.
   *
   * The yielded `key` is a fresh Buffer (safe to retain). The yielded
   * `value` is a SLICE of the underlying block data — copy it if you need
   * to retain it beyond the next iteration of the consumer's loop, since
   * the next block read will replace the underlying buffer.
   */
  *entries(): Generator<BlockEntry> {
    const data = this.#data;
    const end = this.#restartsOffset;
    let pos = 0;
    let key = Buffer.alloc(0);

    while (pos < end) {
      const h = this.#decodeEntryHeader(pos);
      const suffix = data.subarray(h.suffixOffset, h.suffixOffset + h.nonShared);
      const valueStart = h.suffixOffset + h.nonShared;
      const valueEnd = valueStart + h.valueLength;
      const value = data.subarray(valueStart, valueEnd);

      // Reconstruct full key: shared prefix from previous key + this suffix.
      // We always allocate a fresh Buffer for the key because consumers
      // routinely retain keys (e.g. as Map keys in getMany).
      if (h.shared === 0) {
        key = Buffer.from(suffix);
      } else {
        const merged = Buffer.allocUnsafe(h.shared + h.nonShared);
        key.copy(merged, 0, 0, h.shared);
        suffix.copy(merged, h.shared);
        key = merged;
      }

      yield { key, value };
      pos = valueEnd;
    }
  }

  /**
   * Iterate entries starting from the first key >= `startKey`.
   *
   * Same value-aliasing semantics as `entries()`.
   */
  *entriesFrom(startKey: Buffer): Generator<BlockEntry> {
    // Binary-search the restart array for the largest restart point whose
    // key is <= startKey. Linear scan from there.
    const startRestart = this.#findRestartFloor(startKey);

    let pos = this.#getRestart(startRestart);
    const end = this.#restartsOffset;
    let key = Buffer.alloc(0);
    let started = false;

    while (pos < end) {
      const h = this.#decodeEntryHeader(pos);
      const suffix = this.#data.subarray(
        h.suffixOffset,
        h.suffixOffset + h.nonShared,
      );

      if (h.shared === 0) {
        key = Buffer.from(suffix);
      } else {
        const merged = Buffer.allocUnsafe(h.shared + h.nonShared);
        key.copy(merged, 0, 0, h.shared);
        suffix.copy(merged, h.shared);
        key = merged;
      }

      const valueStart = h.suffixOffset + h.nonShared;
      const valueEnd = valueStart + h.valueLength;

      if (!started) {
        if (Buffer.compare(key, startKey) >= 0) {
          started = true;
          yield {
            key,
            value: this.#data.subarray(valueStart, valueEnd),
          };
        }
      } else {
        yield {
          key,
          value: this.#data.subarray(valueStart, valueEnd),
        };
      }
      pos = valueEnd;
    }
  }

  /**
   * Find the entry with key exactly equal to `target`.
   *
   * Returns null if no such entry exists. The returned `value` is a slice
   * of the block buffer — copy it if you need to retain it.
   */
  seekExact(target: Buffer): BlockEntry | null {
    const startRestart = this.#findRestartFloor(target);
    let pos = this.#getRestart(startRestart);
    const end = this.#restartsOffset;
    let key = Buffer.alloc(0);

    while (pos < end) {
      const h = this.#decodeEntryHeader(pos);
      const suffix = this.#data.subarray(
        h.suffixOffset,
        h.suffixOffset + h.nonShared,
      );
      if (h.shared === 0) {
        key = Buffer.from(suffix);
      } else {
        const merged = Buffer.allocUnsafe(h.shared + h.nonShared);
        key.copy(merged, 0, 0, h.shared);
        suffix.copy(merged, h.shared);
        key = merged;
      }
      const valueStart = h.suffixOffset + h.nonShared;
      const cmp = Buffer.compare(key, target);
      if (cmp === 0) {
        return {
          key,
          value: this.#data.subarray(valueStart, valueStart + h.valueLength),
        };
      }
      if (cmp > 0) return null; // overshot — sorted, so target isn't here
      pos = valueStart + h.valueLength;
    }
    return null;
  }

  /**
   * Binary-search the restart array for the largest index whose restart
   * key is <= target. If the first restart key is already > target,
   * returns 0 (we still scan from the start in that case).
   */
  #findRestartFloor(target: Buffer): number {
    let lo = 0;
    let hi = this.#numRestarts - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >>> 1;
      const cmp = this.#compareRestartKey(mid, target);
      if (cmp < 0) {
        // restart[mid].key < target → mid is a viable lower bound
        lo = mid;
      } else {
        // restart[mid].key >= target → answer is before mid
        hi = mid - 1;
      }
    }
    return lo;
  }

  /**
   * Compare the key at restart[i] against `target`.
   *
   * Restart points always have shared=0, so the key is just `non_shared`
   * suffix bytes — no need to reconstruct from a previous key.
   */
  #compareRestartKey(i: number, target: Buffer): number {
    const offset = this.#getRestart(i);
    const h = this.#decodeEntryHeader(offset);
    // shared MUST be 0 at a restart point — skip the reconstruction work.
    const aSlice = this.#data.subarray(
      h.suffixOffset,
      h.suffixOffset + h.nonShared,
    );
    return Buffer.compare(aSlice, target);
  }
}
