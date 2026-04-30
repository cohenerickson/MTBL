/**
 * Varint encoding/decoding (LevelDB / protobuf style, little-endian base-128).
 *
 * Each byte uses its low 7 bits for value bits and the high bit as a
 * continuation flag (1 = more bytes follow, 0 = last byte).
 *
 * The mtbl format uses varints heavily for entry headers and block offsets,
 * so this module is on the hot path and aims to be allocation-free.
 */

export interface VarintResult32 {
  /** Decoded unsigned 32-bit value. */
  value: number;
  /** Number of bytes consumed. */
  bytes: number;
}

export interface VarintResult64 {
  /**
   * Decoded unsigned 64-bit value. A plain `number` if it fits in
   * Number.MAX_SAFE_INTEGER (2^53 - 1), otherwise a `bigint`.
   */
  value: number | bigint;
  /** Number of bytes consumed. */
  bytes: number;
}

/**
 * Decode a 32-bit unsigned varint at `offset` in `buf`.
 *
 * @throws if the encoded varint runs past the end of the buffer or exceeds 5 bytes.
 */
export function decodeVarint32(buf: Buffer, offset = 0): VarintResult32 {
  let value = 0;
  let shift = 0;
  let pos = offset;
  // 32-bit varints are at most 5 bytes (5 * 7 = 35 bits ≥ 32).
  for (let i = 0; i < 5; i++) {
    if (pos >= buf.length) {
      throw new Error("varint32: ran past end of buffer");
    }
    const byte = buf[pos++]!;
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) {
      // Force unsigned 32-bit.
      return { value: value >>> 0, bytes: pos - offset };
    }
    shift += 7;
  }
  throw new Error("varint32: more than 5 bytes");
}

/**
 * Decode a 64-bit unsigned varint at `offset` in `buf`.
 *
 * Returns a plain `number` when the value fits within
 * Number.MAX_SAFE_INTEGER, otherwise a `bigint`. mtbl uses 64-bit varints
 * for block offsets within the index — for any practical file these fit
 * easily in safe-integer range.
 *
 * @throws if the encoded varint runs past the end of the buffer or exceeds 10 bytes.
 */
export function decodeVarint64(buf: Buffer, offset = 0): VarintResult64 {
  let lo = 0;
  let hi = 0;
  let pos = offset;
  // 64-bit varints are at most 10 bytes.
  for (let i = 0; i < 10; i++) {
    if (pos >= buf.length) {
      throw new Error("varint64: ran past end of buffer");
    }
    const byte = buf[pos++]!;
    const valBits = byte & 0x7f;
    if (i < 4) {
      // Bits 0..27 fit in `lo`.
      lo |= valBits << (i * 7);
    } else if (i === 4) {
      // Bits 28..34 straddle: low 4 bits into lo, top 3 bits into hi.
      lo |= (valBits & 0x0f) << 28;
      hi |= (valBits >>> 4) & 0x07;
    } else {
      // Bits 35.. go into hi.
      hi |= valBits << (i * 7 - 32);
    }
    if ((byte & 0x80) === 0) {
      lo >>>= 0;
      hi >>>= 0;
      const value =
        hi === 0
          ? lo
          : hi < 0x200000 // (Number.MAX_SAFE_INTEGER + 1) >>> 32 === 0x200000
            ? hi * 0x1_0000_0000 + lo
            : (BigInt(hi) << 32n) | BigInt(lo);
      return { value, bytes: pos - offset };
    }
  }
  throw new Error("varint64: more than 10 bytes");
}

/**
 * Read a little-endian uint32 at `offset`. Trivial wrapper over Buffer's
 * built-in method, kept here so all binary primitives live in one module.
 */
export function readU32LE(buf: Buffer, offset = 0): number {
  return buf.readUInt32LE(offset);
}

/**
 * Read a little-endian uint64 at `offset`. Returns a plain `number` if the
 * value is within Number.MAX_SAFE_INTEGER, otherwise a `bigint`.
 *
 * mtbl trailer fields and block offsets are uint64. For any practical file
 * (< 9 PB) the `number` path will be taken.
 */
export function readU64LE(buf: Buffer, offset = 0): number | bigint {
  const lo = buf.readUInt32LE(offset);
  const hi = buf.readUInt32LE(offset + 4);
  if (hi === 0) return lo;
  if (hi < 0x200000) {
    return hi * 0x1_0000_0000 + lo;
  }
  return (BigInt(hi) << 32n) | BigInt(lo);
}

/**
 * Encode a value (number or bigint) returned by readU64LE/decodeVarint64
 * back to a plain JS number, asserting it's within safe-integer range.
 *
 * Used internally where we know the value MUST fit (e.g. block offsets in
 * a file we know is under a few TB).
 */
export function toSafeNumber(value: number | bigint): number {
  if (typeof value === "number") return value;
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(
      `value ${value} exceeds Number.MAX_SAFE_INTEGER and cannot be safely converted`,
    );
  }
  return Number(value);
}
