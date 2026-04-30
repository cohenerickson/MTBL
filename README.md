# mtbl

Pure-TypeScript streaming reader for [mtbl](https://github.com/farsightsec/mtbl) (Sorted String Table) files. Memory-efficient enough for files in the hundreds of GB.

## Install

```bash
npm install
npm run build
npm test
```

## Usage

```ts
import { MTBLReader } from "mtbl";

const reader = new MTBLReader("./images.mtbl");
await reader.ready;

// Point lookup
const value = await reader.get("some-key"); // Buffer | null

// Bulk lookup (sorts internally for fast block reuse)
const map = await reader.getMany(["k1", "k2", "k3"]);

// Iteration — streaming, memory stays flat regardless of file size
for await (const { key, value } of reader.getPrefix("photos/2024/")) {
  // ...
}

await reader.close();
```

### Full API

```ts
class MTBLReader {
  constructor(path: string, options?: MTBLReaderOptions);

  ready: Promise<void>;

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
- `reader.ts` — the public `MTBLReader` class. Pulls all the layers together and provides the user-facing API.

### Memory model

For a 448 GB file with default 8 KB data blocks, the file has roughly 56 million data blocks. The on-disk index block in a typical mtbl file is under 1% of file size (~1–4 GB for a 448 GB file).

The current implementation loads the full index block into memory at open time, then reads exactly one data block per `get()` call from disk. As long as iteration consumers process entries as they arrive (and don't `Array.from` an `AsyncGenerator`), per-operation memory stays flat: one ~8 KB compressed block + one decompressed block (~30–50 KB) at a time.

If the index itself is too large for memory, the next step is implementing a sparse or on-demand index in `index-block.ts` — only the `findBlockForKey` method would need to change; nothing else in the reader cares.

## Format reference

Implementation derived from reading the C source directly: [farsightsec/mtbl](https://github.com/farsightsec/mtbl) (specifically `reader.c`, `block.c`, `metadata.c`, `varint.c`, `compression.c`).

The format is a LevelDB-derived SSTable:

- File ends with a fixed 512-byte trailer (magic `MTBL` for V2, fields are little-endian u64s).
- Trailer points to an index block, which is a regular block whose values are varint64 offsets to data blocks.
- Each block is framed on disk as `[varint64 length][u32 crc32c][payload]` (V2). V1 used a fixed u32 length instead of a varint.
- Block payloads use LevelDB-style prefix-compressed entries (`[varint shared][varint non_shared][varint value_length][suffix][value]`) with a restart array at the end for binary search.
