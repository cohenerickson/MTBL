/**
 * Thin wrapper around `fs.promises.open` that provides positional reads.
 *
 * Every disk read in the rest of the library goes through `readAt`, so this
 * is the only module that has to know about file descriptors. Keeps things
 * easy to mock in tests and easy to swap for an alternative I/O backend
 * (e.g. mmap, S3) later if needed.
 */

import { type FileHandle, open } from "node:fs/promises";

export class FileReader {
  readonly path: string;
  readonly #fh: FileHandle;
  readonly #size: number;
  #closed = false;

  private constructor(path: string, fh: FileHandle, size: number) {
    this.path = path;
    this.#fh = fh;
    this.#size = size;
  }

  /**
   * Open `path` for reading and stat it for the size.
   *
   * The returned reader holds a single FileHandle for its lifetime — call
   * `close()` when done.
   */
  static async open(path: string): Promise<FileReader> {
    const fh = await open(path, "r");
    try {
      const stat = await fh.stat({ bigint: false });
      const size = stat.size;
      if (!Number.isFinite(size)) {
        throw new Error(`could not determine size of ${path}`);
      }
      return new FileReader(path, fh, size);
    } catch (err) {
      // Best-effort cleanup if stat fails.
      try {
        await fh.close();
      } catch {
        /* ignore */
      }
      throw err;
    }
  }

  get size(): number {
    return this.#size;
  }

  get closed(): boolean {
    return this.#closed;
  }

  /**
   * Read exactly `length` bytes starting at `offset`.
   *
   * Always returns a Buffer of the requested length. Throws if the read
   * cannot be satisfied (short read at EOF, etc.).
   */
  async readAt(offset: number, length: number): Promise<Buffer> {
    if (this.#closed) {
      throw new Error(`FileReader for ${this.path} is closed`);
    }
    if (length === 0) return Buffer.alloc(0);
    if (length < 0 || !Number.isInteger(length)) {
      throw new Error(`invalid length: ${length}`);
    }
    if (offset < 0 || !Number.isFinite(offset)) {
      throw new Error(`invalid offset: ${offset}`);
    }

    const buf = Buffer.allocUnsafe(length);
    let read = 0;
    while (read < length) {
      const { bytesRead } = await this.#fh.read(
        buf,
        read,
        length - read,
        offset + read,
      );
      if (bytesRead === 0) {
        throw new Error(
          `short read at offset ${offset}: wanted ${length} bytes, got ${read}`,
        );
      }
      read += bytesRead;
    }
    return buf;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.#fh.close();
  }
}
