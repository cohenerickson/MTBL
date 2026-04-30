/**
 * Format constants for the mtbl file format.
 *
 * Source of truth: https://github.com/farsightsec/mtbl
 */

// Trailer magic numbers (little-endian uint32 at end of file).
export const MTBL_MAGIC_V1 = 0x77846676;
export const MTBL_MAGIC_V2 = 0x4d54424c; // "MTBL" in ASCII

// Trailer is exactly 512 bytes.
export const METADATA_SIZE = 512;

// Compression algorithm IDs (stored as little-endian uint64 in the trailer).
export const Compression = {
  NONE: 0,
  SNAPPY: 1,
  ZLIB: 2,
  LZ4: 3,
  LZ4HC: 4,
  ZSTD: 5,
} as const;

export type CompressionId = (typeof Compression)[keyof typeof Compression];

export const COMPRESSION_NAME = {
  [Compression.NONE]: "none",
  [Compression.SNAPPY]: "snappy",
  [Compression.ZLIB]: "zlib",
  [Compression.LZ4]: "lz4",
  [Compression.LZ4HC]: "lz4hc",
  [Compression.ZSTD]: "zstd",
} as const satisfies Record<CompressionId, string>;

export type CompressionName = (typeof COMPRESSION_NAME)[CompressionId];

// Format version identifiers (we surface these as strings in the public API).
export const FormatVersion = {
  V1: "v1",
  V2: "v2",
} as const;

export type FormatVersion = (typeof FormatVersion)[keyof typeof FormatVersion];
