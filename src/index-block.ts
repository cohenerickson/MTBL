/**
 * Wrapper around the index block.
 *
 * The index block uses the same physical block layout as data blocks:
 * sorted entries with prefix compression and restart points. The keys are
 * the LAST key of each data block (so binary search resolves any query key
 * to the first data block whose last-key is >= the query). The values are
 * varint64-encoded byte offsets to those data blocks.
 *
 * For typical files the index is comfortably small. Some example numbers
 * from real-world dnstable files: a 29 GB file has a ~196 MB index (0.67%
 * of the file). For files where keeping the full index resident is too
 * much, see the discussion in README.md about sparse / on-demand index
 * strategies — those are layered on top of this class.
 */

import { Block, type BlockEntry } from "./block.js";
import { decodeVarint64, toSafeNumber } from "./varint.js";

export interface IndexLookup {
  /** Byte offset in the file where the data block starts. */
  blockOffset: number;
  /** Last key of that data block (the index entry's key). */
  blockLastKey: Buffer;
}

export class IndexBlock {
  readonly #block: Block;

  constructor(block: Block) {
    this.#block = block;
  }

  /**
   * Find the data block that may contain `target`.
   *
   * Returns the offset of the first data block whose last-key is >= target.
   * If `target` is greater than the file's last key, returns null.
   */
  findBlockForKey(target: Buffer): IndexLookup | null {
    // The Block's seekExact only finds exact matches; we want the floor
    // (smallest key >= target). Use entriesFrom for that — it yields
    // entries starting at the first key >= target.
    const iter = this.#block.entriesFrom(target);
    const first = iter.next();
    if (first.done) return null;
    return entryToLookup(first.value);
  }

  /**
   * Iterate index entries in order, yielding (lastKey, blockOffset) pairs.
   *
   * Used internally by full-file iteration to walk every data block.
   */
  *entries(): Generator<IndexLookup> {
    for (const entry of this.#block.entries()) {
      yield entryToLookup(entry);
    }
  }

  /**
   * Iterate index entries starting from the first data block whose
   * last-key is >= startKey.
   */
  *entriesFrom(startKey: Buffer): Generator<IndexLookup> {
    for (const entry of this.#block.entriesFrom(startKey)) {
      yield entryToLookup(entry);
    }
  }
}

function entryToLookup(entry: BlockEntry): IndexLookup {
  const v = decodeVarint64(entry.value, 0);
  return {
    blockOffset: toSafeNumber(v.value),
    blockLastKey: entry.key,
  };
}
