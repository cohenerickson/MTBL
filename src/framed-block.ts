/**
 * Reading framed blocks from disk.
 *
 * On-disk framing for both data blocks and the index block (V2):
 *
 *   [varint64 length]   compressed payload length
 *   [u32 LE crc32c]     CRC of the compressed payload (we don't verify by default)
 *   [length bytes]      compressed (or uncompressed if NONE) block payload
 *
 * V1 used a fixed u32 length instead of a varint. We support both.
 *
 * We don't know the on-disk length of a block ahead of time when reading
 * arbitrary offsets — only its starting offset (from the index). To avoid
 * making two reads (one for the length header, one for the payload), we
 * speculatively read a chunk that's hopefully big enough for both. The
 * default block size is 8 KB so reading 16 KB here covers nearly all cases
 * in one syscall.
 */

import { decompress } from "./compression.js";
import {
  type CompressionId,
  Compression,
  FormatVersion,
} from "./constants.js";
import { Block } from "./block.js";
import { FileReader } from "./file-handle.js";
import { decodeVarint64, readU32LE, toSafeNumber } from "./varint.js";

/** First-pass read size for a block, in bytes. Tunable. */
const BLOCK_SPECULATIVE_READ = 16 * 1024;

export interface ReadBlockOptions {
  /** Optional cap so we don't speculatively read past EOF. */
  fileSize: number;
  /** Whether the file is V1 (fixed u32 length) or V2 (varint64 length). */
  version: FormatVersion;
  /** Compression algorithm (from the trailer). */
  compression: CompressionId;
}

/**
 * Read and decompress a block whose envelope starts at `offset`.
 *
 * Returns the decompressed `Block` along with the total on-disk size of
 * the framed block (header + crc + payload), useful when scanning blocks
 * sequentially without an index.
 */
export async function readBlock(
  file: FileReader,
  offset: number,
  opts: ReadBlockOptions,
): Promise<{ block: Block; framedSize: number }> {
  // Read a speculative chunk that hopefully contains the entire block.
  const remaining = opts.fileSize - offset;
  const wanted = Math.min(BLOCK_SPECULATIVE_READ, remaining);
  if (wanted <= 0) {
    throw new Error(`cannot read block at offset ${offset}: past EOF`);
  }
  let buf = await file.readAt(offset, wanted);

  // Parse the length header.
  let payloadLength: number;
  let headerLen: number;
  if (opts.version === FormatVersion.V1) {
    if (buf.length < 4) {
      throw new Error("V1 block header truncated");
    }
    payloadLength = readU32LE(buf, 0);
    headerLen = 4;
  } else {
    const v = decodeVarint64(buf, 0);
    payloadLength = toSafeNumber(v.value);
    headerLen = v.bytes;
  }

  const totalLen = headerLen + 4 /* crc */ + payloadLength;

  // Speculative read missed — fetch the rest.
  if (totalLen > buf.length) {
    if (offset + totalLen > opts.fileSize) {
      throw new Error(
        `block at offset ${offset} runs past EOF ` +
          `(needs ${totalLen} bytes, file is ${opts.fileSize})`,
      );
    }
    buf = await file.readAt(offset, totalLen);
  }

  // Skip CRC for now. (We could verify with crc32c if requested via options.)
  const payloadStart = headerLen + 4;
  const payload = buf.subarray(payloadStart, payloadStart + payloadLength);

  // Decompress according to the file's compression algorithm.
  let decompressed: Buffer;
  if (opts.compression === Compression.NONE) {
    // Make a copy: payload is a slice of our speculative-read buffer, and
    // the Block will retain references to it. Detach by copying so the
    // larger buffer can be GC'd.
    decompressed = Buffer.from(payload);
  } else {
    decompressed = await decompress(opts.compression, payload);
  }

  return {
    block: new Block(decompressed),
    framedSize: totalLen,
  };
}
