# MTBL

Pure-TypeScript streaming reader for [mtbl](https://github.com/farsightsec/mtbl) (Sorted String Table) files. Memory-efficient enough for files in the hundreds of GB.

> **Vibe-coded.** This library was drafted in a long AI-assisted session. The architecture is solid and the tests pass, but real-world edge cases may surface. Fresh eyes and contributions are very welcome — please open issues or PRs.

## Requirements

Node.js 18+. zstd compression requires Node 22.15+.

## Install

```bash
npm install @cohenerickson/mtbl
```

For lz4 or snappy compressed files, also install the relevant peer dep (see [Compression support](#compression-support)).

## Usage

```ts
import { MTBLReader } from "mtbl";

const reader = new MTBLReader("./images.mtbl");
await reader.ready;

// Point lookup
const value = await reader.get("some-key"); // Buffer | null

// Check existence without fetching the value
const exists = await reader.has("some-key"); // boolean

// Bulk lookup (sorts keys internally for data block reuse)
const map = await reader.getMany(["k1", "k2", "k3"]);

// Iteration — one block in memory at a time regardless of file size
for await (const { key, value } of reader.getPrefix("photos/2024/")) {
  // key and value are Buffers, safe to retain across iterations
}

await reader.close();
```

> **Warning:** Do not accumulate all entries into an array on large files. `Array.fromAsync(reader.iterate())` will OOM on any file that doesn't fit in memory. Consume the async iterator without buffering.

### Full API

```ts
class MTBLReader {
  constructor(path: string, options?: MTBLReaderOptions);

  /** Resolves when the file is open and the index is loaded. */
  ready: Promise<void>;

  /** File metadata from the on-disk trailer. */
  metadata(): Promise<MTBLMetadata>;

  // Point access
  get(key: KeyInput): Promise<Buffer | null>;
  has(key: KeyInput): Promise<boolean>;
  getMany(keys: readonly KeyInput[]): Promise<Map<Buffer, Buffer>>;

  // Iteration
  iterate(options?: IterateOptions): AsyncGenerator<MTBLEntry>;
  keys(options?: IterateOptions): AsyncGenerator<Buffer>;
  values(options?: IterateOptions): AsyncGenerator<Buffer>;
  getPrefix(prefix: KeyInput): AsyncGenerator<MTBLEntry>;
  getRange(start: KeyInput, end: KeyInput): AsyncGenerator<MTBLEntry>;
  [Symbol.asyncIterator](): AsyncGenerator<MTBLEntry>;

  // Bounds
  firstKey(): Promise<Buffer | null>;
  lastKey(): Promise<Buffer | null>;

  close(): Promise<void>;
}
```

`KeyInput` is `Buffer | Uint8Array | string` (strings are UTF-8 encoded). All returned keys and values are `Buffer`.

#### `IterateOptions`

```ts
interface IterateOptions {
  /** Inclusive lower bound. Starts at the first key >= start. */
  start?: KeyInput;
  /** Exclusive upper bound. Stops before the first key >= end. */
  end?: KeyInput;
  /** Only yield entries whose key begins with this prefix. */
  prefix?: KeyInput;
}
```

If `prefix` is given alongside `start`/`end`, the bounds are intersected.

#### `MTBLMetadata`

```ts
interface MTBLMetadata {
  version: "v1" | "v2";
  compression: "none" | "zlib" | "lz4" | "lz4hc" | "zstd" | "snappy";
  compressionAlgorithm: number;
  entryCount: number | bigint;
  dataBlockCount: number | bigint;
  bytesDataBlocks: number | bigint;  // compressed
  bytesIndexBlock: number | bigint;
  bytesKeys: number | bigint;        // uncompressed
  bytesValues: number | bigint;      // uncompressed
  fileSize: number;
}
```

Values are `bigint` when they exceed `Number.MAX_SAFE_INTEGER`.

## Compression support

| Algorithm | Decoder                        | Status      |
| --------- | ------------------------------ | ----------- |
| `none`    | pass-through                   | built-in    |
| `zlib`    | `node:zlib`                    | built-in    |
| `zstd`    | `node:zlib` (`zstdDecompress`) | Node 22.15+ |
| `lz4`     | `lz4-napi` peer dep            | optional    |
| `lz4hc`   | `lz4-napi` peer dep            | optional    |
| `snappy`  | `snappy` peer dep              | optional    |

If your file uses lz4 or snappy, install the relevant peer dep:

```bash
npm install lz4-napi    # for lz4 / lz4hc
npm install snappy      # for snappy
```

## Architecture

The reader is structured in layers:

- `file-handle.ts` — thin wrapper around `fs.promises.open` providing positional reads. The only module that touches the filesystem.
- `varint.ts` — varint and fixed-width LE integer decoders.
- `trailer.ts` — parses the 512-byte trailer at the end of the file.
- `compression.ts` — dispatches block decompression by algorithm.
- `block.ts` — parses an individual decompressed block (the LevelDB-style prefix-compressed layout with restart-point binary search). This is the workhorse module.
- `framed-block.ts` — reads a single block envelope from disk (`[varint length][u32 crc][payload]`), decompresses, and constructs a `Block`.
- `index-block.ts` — special handling for the index block, which maps keys to data block offsets.
- `reader.ts` — the public `MTBLReader` class. Pulls all the layers together.

### Memory model

For a 500 GB file with default 8 KB data blocks, there are roughly 65 million data blocks. The on-disk index is typically under 1% of file size (~2.5–5 GB for a 500 GB file).

The current implementation loads the full index block into memory at open time, then reads exactly one data block per `get()` call from disk. As long as iteration consumers process entries as they arrive, per-operation memory stays flat: one ~8 KB compressed block + one decompressed block (~30–50 KB) at a time.

If the index is too large for memory, the path forward is a sparse or on-demand index inside `index-block.ts` — only `findBlockForKey` would need to change.

## Format reference

Implementation derived from reading the C source directly: [farsightsec/mtbl](https://github.com/farsightsec/mtbl) (specifically `reader.c`, `block.c`, `metadata.c`, `varint.c`, `compression.c`).

The format is a LevelDB-derived SSTable:

- File ends with a fixed 512-byte trailer (magic `MTBL` for V2, fields are little-endian u64s).
- Trailer points to an index block, which is a regular block whose values are varint64 offsets to data blocks.
- Each block is framed on disk as `[varint64 length][u32 crc32c][payload]` (V2). V1 used a fixed u32 length instead of a varint.
- Block payloads use LevelDB-style prefix-compressed entries (`[varint shared][varint non_shared][varint value_length][suffix][value]`) with a restart array at the end for binary search.

## Contributing

Issues and PRs are welcome. If you have a real mtbl file written by the reference C library (`mtbl_create` or similar), adding it as a test fixture would be particularly valuable — the test suite currently only exercises files written by the TypeScript fixture writer in `test/fixture.ts`.
