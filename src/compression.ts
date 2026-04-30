/**
 * Decompression dispatch for mtbl block payloads.
 *
 * mtbl files compress each data/index block independently. The compression
 * algorithm is recorded once in the trailer and applies to every block.
 *
 *   NONE   - pass through
 *   ZLIB   - standard zlib (RFC 1950, with header). Built into Node.
 *   ZSTD   - raw zstd frame. Built into Node 22.15+ (zlib.zstdDecompress).
 *   LZ4    - 4-byte LE uncompressed size header, then raw LZ4 block payload.
 *            Requires optional `lz4-napi` peer dep.
 *   LZ4HC  - same wire format as LZ4 (different writer, same decoder).
 *   SNAPPY - raw snappy block (NOT framed). Requires optional `snappy` peer dep.
 *
 * Optional native deps are lazy-loaded so callers who never read snappy/lz4
 * files don't need them installed.
 */

import { promisify } from "node:util";
import zlib from "node:zlib";
import { Compression, type CompressionId } from "./constants.js";

const inflate = promisify(zlib.inflate);

// zstdDecompress arrived in Node 22.15. Check at module load and surface
// a useful error at decompression time if missing.
const zstdDecompress: ((buf: Buffer) => Promise<Buffer>) | null =
  typeof (zlib as unknown as { zstdDecompress?: unknown }).zstdDecompress ===
  "function"
    ? (promisify(
        (zlib as unknown as {
          zstdDecompress: (
            buf: Buffer,
            cb: (err: Error | null, result: Buffer) => void,
          ) => void;
        }).zstdDecompress,
      ) as (buf: Buffer) => Promise<Buffer>)
    : null;

interface SnappyModule {
  uncompress(buf: Buffer): Promise<Buffer>;
}

interface Lz4Module {
  uncompress?: (buf: Buffer) => Promise<Buffer>;
  uncompressSync?: (buf: Buffer) => Buffer;
  decompressFrame?: (buf: Buffer, size: number) => Promise<Buffer>;
}

let snappyMod: SnappyModule | null = null;
let snappyTried = false;
async function loadSnappy(): Promise<SnappyModule | null> {
  if (snappyTried) return snappyMod;
  snappyTried = true;
  try {
    snappyMod = (await import("snappy")) as unknown as SnappyModule;
  } catch {
    snappyMod = null;
  }
  return snappyMod;
}

let lz4Mod: Lz4Module | null = null;
let lz4Tried = false;
async function loadLz4(): Promise<Lz4Module | null> {
  if (lz4Tried) return lz4Mod;
  lz4Tried = true;
  try {
    lz4Mod = (await import("lz4-napi")) as unknown as Lz4Module;
  } catch {
    lz4Mod = null;
  }
  return lz4Mod;
}

/**
 * Decompress a raw block payload according to the file's compression type.
 *
 * @param algorithm  value from Compression
 * @param input      raw block bytes (after the framing header + crc)
 */
export async function decompress(
  algorithm: CompressionId,
  input: Buffer,
): Promise<Buffer> {
  switch (algorithm) {
    case Compression.NONE:
      return input;

    case Compression.ZLIB:
      return await inflate(input);

    case Compression.ZSTD: {
      if (!zstdDecompress) {
        throw new Error(
          "this mtbl file uses zstd compression, which requires Node 22.15+ " +
            "(zlib.zstdDecompress)",
        );
      }
      return await zstdDecompress(input);
    }

    case Compression.LZ4:
    case Compression.LZ4HC: {
      const mod = await loadLz4();
      if (!mod) {
        throw new Error(
          "this mtbl file uses lz4 compression. Install the 'lz4-napi' " +
            "peer dependency: npm install lz4-napi",
        );
      }
      // mtbl prepends a 4-byte LE uncompressed size to the LZ4 payload.
      if (input.length < 4) {
        throw new Error("lz4 block too short to contain size prefix");
      }
      const uncompressedSize = input.readUInt32LE(0);
      const payload = input.subarray(4);

      // lz4-napi exposes different APIs across versions; try in order of
      // preference.
      if (typeof mod.uncompressSync === "function") {
        return mod.uncompressSync(input);
      }
      if (typeof mod.uncompress === "function") {
        return await mod.uncompress(input);
      }
      if (typeof mod.decompressFrame === "function") {
        return await mod.decompressFrame(payload, uncompressedSize);
      }
      throw new Error(
        "lz4-napi is installed but does not expose a recognized decompress " +
          "function (uncompress / uncompressSync / decompressFrame)",
      );
    }

    case Compression.SNAPPY: {
      const mod = await loadSnappy();
      if (!mod) {
        throw new Error(
          "this mtbl file uses snappy compression. Install the 'snappy' " +
            "peer dependency: npm install snappy",
        );
      }
      return await mod.uncompress(input);
    }

    default: {
      // Exhaustiveness check.
      const _exhaustive: never = algorithm;
      throw new Error(`unknown mtbl compression algorithm: ${_exhaustive}`);
    }
  }
}
