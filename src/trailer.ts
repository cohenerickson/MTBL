/**
 * Parser for the mtbl trailer (called "metadata" in the C library).
 *
 * The trailer is exactly 512 bytes at the end of the file. Layout:
 *
 *   [u64 LE] index_block_offset       offset of the index block in the file
 *   [u64 LE] data_block_size          target block size used by the writer
 *   [u64 LE] compression_algorithm    one of Compression.*
 *   [u64 LE] count_entries
 *   [u64 LE] count_data_blocks
 *   [u64 LE] bytes_data_blocks
 *   [u64 LE] bytes_index_block
 *   [u64 LE] bytes_keys
 *   [u64 LE] bytes_values
 *   [...]                             zero padding
 *   [u32 LE] magic                    MTBL_MAGIC_V2 (or MTBL_MAGIC_V1)
 */

import {
  COMPRESSION_NAME,
  type CompressionId,
  type CompressionName,
  FormatVersion,
  METADATA_SIZE,
  MTBL_MAGIC_V1,
  MTBL_MAGIC_V2,
} from "./constants";
import { readU32LE, readU64LE } from "./varint";

export interface Trailer {
  version: FormatVersion;
  /** Byte offset of the index block in the file. */
  indexBlockOffset: number | bigint;
  /** Target block size used by the writer (informational). */
  dataBlockSize: number | bigint;
  /** Numeric compression algorithm ID. */
  compressionAlgorithm: CompressionId;
  /** Human-readable compression name. */
  compression: CompressionName;
  entryCount: number | bigint;
  dataBlockCount: number | bigint;
  bytesDataBlocks: number | bigint;
  bytesIndexBlock: number | bigint;
  bytesKeys: number | bigint;
  bytesValues: number | bigint;
}

/**
 * Parse a 512-byte trailer buffer.
 *
 * @param buf  exactly METADATA_SIZE bytes
 * @throws if the magic doesn't match either V1 or V2
 */
export function parseTrailer(buf: Buffer): Trailer {
  if (buf.length !== METADATA_SIZE) {
    throw new Error(
      `trailer must be exactly ${METADATA_SIZE} bytes, got ${buf.length}`,
    );
  }

  const magic = readU32LE(buf, METADATA_SIZE - 4);
  let version: FormatVersion;
  if (magic === MTBL_MAGIC_V2) version = FormatVersion.V2;
  else if (magic === MTBL_MAGIC_V1) version = FormatVersion.V1;
  else {
    const hex = magic.toString(16).padStart(8, "0");
    throw new Error(
      `bad mtbl magic 0x${hex} ` +
        `(expected 0x${MTBL_MAGIC_V2.toString(16)} or 0x${MTBL_MAGIC_V1.toString(16)})`,
    );
  }

  const algorithmRaw = Number(readU64LE(buf, 16));
  if (
    algorithmRaw < 0 ||
    algorithmRaw > 5 ||
    !Number.isInteger(algorithmRaw)
  ) {
    throw new Error(`unknown compression algorithm ID: ${algorithmRaw}`);
  }
  const compressionAlgorithm = algorithmRaw as CompressionId;

  return {
    version,
    indexBlockOffset: readU64LE(buf, 0),
    dataBlockSize: readU64LE(buf, 8),
    compressionAlgorithm,
    compression: COMPRESSION_NAME[compressionAlgorithm],
    entryCount: readU64LE(buf, 24),
    dataBlockCount: readU64LE(buf, 32),
    bytesDataBlocks: readU64LE(buf, 40),
    bytesIndexBlock: readU64LE(buf, 48),
    bytesKeys: readU64LE(buf, 56),
    bytesValues: readU64LE(buf, 64),
  };
}
