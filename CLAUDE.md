# Context for Claude

This file captures the design decisions and rationale behind this package. Read this before making changes — most of the choices here have specific reasoning that isn't obvious from the code alone.

## What this package is

A pure-TypeScript reader for [mtbl](https://github.com/farsightsec/mtbl) files. mtbl is a LevelDB-derived SSTable format — an immutable, sorted, on-disk key-value store. Both keys and values are arbitrary byte arrays (no schema). The format is designed for fast point lookups and range scans on huge files that don't fit in memory.

The original C library is fine; we have our own implementation because we want a JS-native dependency-free reader rather than a native addon that would complicate distribution.

## What this package is NOT

- **Not a writer.** The only writer in this repo is `test/fixture.ts`, which exists solely to produce test files. It supports a tiny subset of the format (V2, NONE/ZLIB only, no CRC, no concurrency). Don't promote it to production use without significant work.
- **Not a database.** No updates, no transactions, no merging. mtbl files are immutable.
- **Not a generic SSTable reader.** Format details (varint widths, trailer size, magic numbers) are mtbl-specific, even though the block layout is borrowed from LevelDB.

## The motivating use case

The motivating use case is a large mtbl file (hundreds of GB) containing binary values keyed by a string identifier. A manifest lists the keys needed. The dominant workflow is:

1. Open the mtbl file once
2. Look up many keys via `get()` / `getMany()`
3. Get back binary values

Iteration methods (`iterate`, `getPrefix`, `getRange`, `keys`, `values`) are secondary — useful for debugging and discovery rather than the primary workflow. Don't deprioritize them, but the perf focus should be point lookups.

## Memory model (read this before optimizing)

For a 500 GB file with default 8 KB blocks, there are ~65 million data blocks. The on-disk index is typically 0.5%–1% of file size, so 2.5–5 GB.

**Current strategy:** Load the full index block into memory at open time. Read exactly one data block per `get()`. Iteration streams blocks one at a time and yields entries as they're decoded.

**Per-operation memory:**

- Open: ~2.5–5 GB resident for the full index of a 500 GB file
- `get()`: one ~8 KB compressed block + one decompressed block (~30–50 KB)
- Iteration: one block at a time, regardless of how many entries are scanned

**The big lurking decision:** the user may not be willing to spend 2.5–5 GB on the index. Future work, in order of likely-need:

1. **Lazy decode of index entries.** Keep the raw index buffer in memory but don't materialize entries as JS objects. Use a `Uint32Array` of byte offsets and binary-search by decoding on demand. Cuts memory ~3–5x.
2. **Sparse index.** Sample every Nth entry into memory, fetch the others from disk on demand. Cuts memory by N (typical N=64 → ~64x reduction).
3. **mmap.** Currently `mmap-io` and similar are unmaintained, so we avoided this. If a maintained option appears, mmap'ing the index region would be ideal — kernel handles caching better than we can.

These would all live behind a `MTBLReaderOptions.indexStrategy` field. The `IndexBlock` class is the only module that needs to change; the rest of the reader is agnostic.

**Iteration is a footgun if consumers accumulate.** A naïve `Array.fromAsync(reader.iterate())` on any file that doesn't fit in memory will OOM. The README warns about this; if we ever add convenience helpers (e.g. `toArray()`), they should reject or refuse to run on large files.

## API design decisions

**Why a single class instead of a functional API?** The user explicitly asked for `new MTBLReader(path)` shape. It also genuinely fits — every operation needs the file handle + trailer + index, and tying them to an instance is cleaner than threading them through standalone functions.

**Why `ready: Promise<void>` instead of a static factory?** User preference. The internal methods all `await this.ready` first, so callers can technically skip the explicit `await reader.ready` and it still works — `ready` just lets you surface initialization errors early. Both patterns are supported; don't break either.

**Why is `metadata()` async?** It awaits `ready` internally. Could be a sync property, but making it async keeps the call site honest about the fact that the reader might not be initialized yet.

**Why `Buffer | Uint8Array | string` for keys?** Convenience. Strings are UTF-8. Always returns `Buffer` (callers can `.toString()`). The internal toBuffer helper aliases `Uint8Array` instead of copying — beware if you ever start mutating input keys.

**Why iterators yield `{ key, value }` objects instead of `[key, value]` tuples?** Slightly more allocation per yield, but discoverable property names make the API more pleasant. The cost is irrelevant compared to block decompression.

**Why both `getPrefix` and `getRange` when `iterate({prefix, start, end})` exists?** Discoverability. Users from web dev backgrounds reach for named methods first. They're 5-line wrappers; cost is nothing.

**Why no `count()`, `reverse()`, `limit()`, `offset()`?** Deferred to v2. None of them are zero-cost on this format and the user's primary workflow doesn't need them. `count()` would require streaming through all matching blocks; `reverse()` is doable with the existing block format but adds API surface; `limit`/`offset` users can implement themselves with `for await`.

## Format details worth remembering

**The trailer is exactly 512 bytes.** Don't change this. The magic is at `offset = file_size - 4`, not at offset 0.

**Varints are LevelDB-style** (1–10 bytes, low 7 bits per byte, high bit as continuation flag). NOT the same as protobuf signed varints, which use zigzag encoding.

**Block restart points always have `shared = 0`.** This is what makes binary search within a block possible — at a restart point, the key is fully materialized and doesn't depend on the previous entry.

**Restart array width is normally u32 but can be u64.** The C library checks `restart_offset > UINT32_MAX` and switches to u64. For 8 KB data blocks this is dead code, but the index block can be huge enough to trigger it. Block.ts handles both.

**LZ4 has a 4-byte LE uncompressed-size prefix** before the actual LZ4 payload. This is mtbl-specific framing, not standard LZ4. Don't strip it in the wrong layer.

**zlib means standard zlib (RFC 1950, with header).** Not raw deflate. Node's `zlib.inflate` handles it directly.

**zstd uses raw zstd frames** (no extra framing). Node 22.15+ has `zlib.zstdDecompress` built-in.

**Block CRCs are CRC32C (Castagnoli), not CRC32.** The reference reader only verifies CRCs when explicitly requested, and we currently don't verify at all. If we add verification, use the right polynomial.

## Testing strategy

`test/fixture.ts` is a minimal mtbl writer that produces spec-compliant files. The test suite writes fixtures and round-trips them through `MTBLReader`. This validates the format work end-to-end.

What it does NOT validate:

- Reading files written by the reference C library specifically. If you have a real mtbl file from `mtbl_create` or similar, drop it in `test/fixtures/` and add a test that reads it. This is the gold-standard validation we're missing.
- LZ4, LZ4HC, snappy, zstd compression. The fixture writer only does NONE and ZLIB. Expand it (or add real-world fixtures) when needed.
- The u64 restart-array fallback in block.ts. Would need an artificially huge index block to exercise.

## Things that are likely to break first

If a real mtbl file fails to read, here's the order to suspect things:

1. **Block parser** (`block.ts`). The restart-point binary search and prefix-key reconstruction have a lot of off-by-one opportunities. The `entriesFrom` and `seekExact` paths are particularly fiddly.
2. **Compression dispatch** (`compression.ts`). lz4-napi exposes different APIs across versions — we try several. snappy-the-package has historically renamed exports.
3. **u64 restart arrays.** Untested in the unit suite. If your file has a really large index, this branch matters.
4. **Trailer parsing** for V1 files. We claim to support V1 but only V2 has been exercised.

## Code style notes

- Strict TypeScript with `noUncheckedIndexedAccess`. Don't loosen.
- Public API uses `Buffer` rather than `Uint8Array`. Internal-only paths can use either; convert at the boundary.
- Error messages should include enough context to be debuggable in a vacuum (file path, byte offset, expected vs actual lengths).
- No external runtime dependencies for core functionality. lz4 and snappy are optional peer deps. Keep it that way.
- ESM only. `"type": "module"` everywhere.
- File imports use `.js` extensions (NodeNext convention) even though source files are `.ts`. This is correct for tsc-compiled output.

## How this code was written

Original architecture and code drafted in a long Claude conversation covering: format research, memory strategy, API design, then implementation. The format details came from reading the C source of [farsightsec/mtbl](https://github.com/farsightsec/mtbl) directly — specifically `reader.c`, `block.c`, `metadata.c`, `varint.c`, and `compression.c`. The design conversation included extensive discussion of the large-file use case which drove the memory-model decisions above.

The package was developed without being able to install npm dependencies in the dev sandbox, so the test suite hasn't been executed end-to-end. If you're reading this on first checkout: **run `npm install && npm test` before trusting any of it.** Failures are most likely in the block parser.
