/**
 * End-to-end tests for MTBLReader.
 *
 * Each test writes a fixture file with our in-test writer, opens it with
 * MTBLReader, and exercises a public method.
 */

import { strict as assert } from "assert";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { afterEach, beforeEach, describe, test } from "node:test";

import { MTBLReader } from "../src/index.js";
import {
  COMPRESSION_NONE,
  COMPRESSION_ZLIB,
  writeMtblFile,
} from "./fixture.js";

let tmp: string;
beforeEach(() => {
  tmp = mkdtempSync(join(tmpdir(), "mtbl-test-"));
});
afterEach(() => {
  rmSync(tmp, { recursive: true, force: true });
});

/** Build a sorted array of [key, value] entries from a string map. */
function makeEntries(
  obj: Record<string, string>,
): { key: Buffer; value: Buffer }[] {
  return Object.keys(obj)
    .sort()
    .map((k) => ({
      key: Buffer.from(k, "utf8"),
      value: Buffer.from(obj[k]!, "utf8"),
    }));
}

const SMALL_DATA = {
  apple: "red",
  banana: "yellow",
  cherry: "dark-red",
  date: "brown",
  eggplant: "purple",
  fig: "purple-ish",
  grape: "green",
  honeydew: "pale-green",
};

describe("MTBLReader basics", () => {
  test("opens an uncompressed file and reads metadata", async () => {
    const path = join(tmp, "small.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA), {
      compression: COMPRESSION_NONE,
    });

    const reader = new MTBLReader(path);
    await reader.ready;

    const meta = await reader.metadata();
    assert.equal(meta.version, "v2");
    assert.equal(meta.compression, "none");
    assert.equal(Number(meta.entryCount), Object.keys(SMALL_DATA).length);
    assert.ok(Number(meta.dataBlockCount) >= 1);
    assert.ok(meta.fileSize > 512);

    await reader.close();
  });

  test("opens a zlib-compressed file", async () => {
    const path = join(tmp, "zlib.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA), {
      compression: COMPRESSION_ZLIB,
    });

    const reader = new MTBLReader(path);
    await reader.ready;
    const meta = await reader.metadata();
    assert.equal(meta.compression, "zlib");
    await reader.close();
  });

  test("rejects files that are too small", async () => {
    const path = join(tmp, "tiny.mtbl");
    const { writeFileSync } = await import("node:fs");
    writeFileSync(path, Buffer.from("nope"));

    const reader = new MTBLReader(path);
    await assert.rejects(() => reader.ready, /too small/i);
  });

  test("rejects files with bad magic", async () => {
    const path = join(tmp, "bad.mtbl");
    const { writeFileSync } = await import("node:fs");
    // 512 bytes of zeros — magic at the end is 0, not MTBL_MAGIC.
    writeFileSync(path, Buffer.alloc(512));

    const reader = new MTBLReader(path);
    await assert.rejects(() => reader.ready, /bad mtbl magic/);
  });
});

describe("get()", () => {
  for (const compression of [COMPRESSION_NONE, COMPRESSION_ZLIB] as const) {
    const label = compression === COMPRESSION_NONE ? "none" : "zlib";

    test(`finds existing keys (${label})`, async () => {
      const path = join(tmp, `get-${label}.mtbl`);
      writeMtblFile(path, makeEntries(SMALL_DATA), { compression });

      const reader = new MTBLReader(path);
      await reader.ready;

      for (const [k, v] of Object.entries(SMALL_DATA)) {
        const got = await reader.get(k);
        assert.ok(got !== null, `expected to find ${k}`);
        assert.equal(got!.toString("utf8"), v);
      }

      await reader.close();
    });

    test(`returns null for missing keys (${label})`, async () => {
      const path = join(tmp, `getmiss-${label}.mtbl`);
      writeMtblFile(path, makeEntries(SMALL_DATA), { compression });

      const reader = new MTBLReader(path);
      await reader.ready;

      // Before first key, between keys, after last key, completely unrelated.
      assert.equal(await reader.get("aaaaa"), null);
      assert.equal(await reader.get("blueberry"), null);
      assert.equal(await reader.get("zzz"), null);
      assert.equal(await reader.get("APPLE"), null); // case-sensitive

      await reader.close();
    });
  }

  test("accepts string, Buffer, and Uint8Array keys", async () => {
    const path = join(tmp, "keytypes.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const fromString = await reader.get("apple");
    const fromBuffer = await reader.get(Buffer.from("apple"));
    const fromU8 = await reader.get(new Uint8Array(Buffer.from("apple")));

    assert.ok(fromString && fromBuffer && fromU8);
    assert.equal(fromString.toString(), "red");
    assert.equal(fromBuffer.toString(), "red");
    assert.equal(fromU8.toString(), "red");

    await reader.close();
  });

  test("works with binary keys and binary values", async () => {
    const path = join(tmp, "binary.mtbl");
    const entries = [
      { key: Buffer.from([0x00, 0x01]), value: Buffer.from([0xff, 0xfe]) },
      { key: Buffer.from([0x00, 0x02]), value: Buffer.from([0x00]) },
      { key: Buffer.from([0x10, 0x00]), value: Buffer.from([0xde, 0xad, 0xbe, 0xef]) },
    ];
    writeMtblFile(path, entries);

    const reader = new MTBLReader(path);
    await reader.ready;

    const got = await reader.get(Buffer.from([0x10, 0x00]));
    assert.deepEqual([...got!], [0xde, 0xad, 0xbe, 0xef]);

    await reader.close();
  });
});

describe("has()", () => {
  test("returns true/false correctly", async () => {
    const path = join(tmp, "has.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    assert.equal(await reader.has("apple"), true);
    assert.equal(await reader.has("eggplant"), true);
    assert.equal(await reader.has("zucchini"), false);

    await reader.close();
  });
});

describe("getMany()", () => {
  test("returns map of found keys", async () => {
    const path = join(tmp, "getmany.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const result = await reader.getMany(["banana", "honeydew", "missing", "apple"]);
    // Map keys are Buffers from the input conversion; check by stringifying.
    const stringified = new Map<string, string>();
    for (const [k, v] of result) {
      stringified.set(k.toString("utf8"), v.toString("utf8"));
    }

    assert.equal(stringified.size, 3);
    assert.equal(stringified.get("apple"), "red");
    assert.equal(stringified.get("banana"), "yellow");
    assert.equal(stringified.get("honeydew"), "pale-green");
    assert.equal(stringified.has("missing"), false);

    await reader.close();
  });

  test("returns empty map for empty input", async () => {
    const path = join(tmp, "getmany-empty.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;
    const result = await reader.getMany([]);
    assert.equal(result.size, 0);
    await reader.close();
  });
});

describe("iterate()", () => {
  test("iterates all entries in order", async () => {
    const path = join(tmp, "iter.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const seen: [string, string][] = [];
    for await (const { key, value } of reader.iterate()) {
      seen.push([key.toString(), value.toString()]);
    }

    const expected = Object.entries(SMALL_DATA).sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0,
    );
    assert.deepEqual(seen, expected);

    await reader.close();
  });

  test("Symbol.asyncIterator yields the same as iterate()", async () => {
    const path = join(tmp, "iter-symbol.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const seen: string[] = [];
    for await (const { key } of reader) {
      seen.push(key.toString());
    }
    assert.deepEqual(seen, Object.keys(SMALL_DATA).sort());

    await reader.close();
  });

  test("respects start bound (inclusive)", async () => {
    const path = join(tmp, "iter-start.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const keys: string[] = [];
    for await (const { key } of reader.iterate({ start: "date" })) {
      keys.push(key.toString());
    }
    assert.deepEqual(keys, ["date", "eggplant", "fig", "grape", "honeydew"]);

    await reader.close();
  });

  test("respects end bound (exclusive)", async () => {
    const path = join(tmp, "iter-end.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const keys: string[] = [];
    for await (const { key } of reader.iterate({ end: "date" })) {
      keys.push(key.toString());
    }
    assert.deepEqual(keys, ["apple", "banana", "cherry"]);

    await reader.close();
  });

  test("respects both start and end", async () => {
    const path = join(tmp, "iter-range.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const keys: string[] = [];
    for await (const { key } of reader.iterate({
      start: "cherry",
      end: "fig",
    })) {
      keys.push(key.toString());
    }
    assert.deepEqual(keys, ["cherry", "date", "eggplant"]);

    await reader.close();
  });
});

describe("getPrefix()", () => {
  test("yields only entries with the prefix", async () => {
    const path = join(tmp, "prefix.mtbl");
    const data = {
      "photos/2023/a.jpg": "1",
      "photos/2023/b.jpg": "2",
      "photos/2024/c.jpg": "3",
      "photos/2024/d.jpg": "4",
      "videos/2024/e.mp4": "5",
      "zzz": "last",
    };
    writeMtblFile(path, makeEntries(data));

    const reader = new MTBLReader(path);
    await reader.ready;

    const keys: string[] = [];
    for await (const { key } of reader.getPrefix("photos/2024/")) {
      keys.push(key.toString());
    }
    assert.deepEqual(keys, ["photos/2024/c.jpg", "photos/2024/d.jpg"]);

    await reader.close();
  });

  test("yields nothing for non-matching prefix", async () => {
    const path = join(tmp, "prefix-empty.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const keys: string[] = [];
    for await (const { key } of reader.getPrefix("xyz")) {
      keys.push(key.toString());
    }
    assert.deepEqual(keys, []);

    await reader.close();
  });
});

describe("getRange()", () => {
  test("yields entries in [start, end)", async () => {
    const path = join(tmp, "range.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const keys: string[] = [];
    for await (const { key } of reader.getRange("banana", "fig")) {
      keys.push(key.toString());
    }
    assert.deepEqual(keys, ["banana", "cherry", "date", "eggplant"]);

    await reader.close();
  });
});

describe("keys() / values()", () => {
  test("keys() yields just keys", async () => {
    const path = join(tmp, "keys.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const seen: string[] = [];
    for await (const k of reader.keys()) {
      seen.push(k.toString());
    }
    assert.deepEqual(seen, Object.keys(SMALL_DATA).sort());

    await reader.close();
  });

  test("values() yields just values in key order", async () => {
    const path = join(tmp, "vals.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const seen: string[] = [];
    for await (const v of reader.values()) {
      seen.push(v.toString());
    }
    const expected = Object.keys(SMALL_DATA)
      .sort()
      .map((k) => SMALL_DATA[k as keyof typeof SMALL_DATA]);
    assert.deepEqual(seen, expected);

    await reader.close();
  });
});

describe("firstKey() / lastKey()", () => {
  test("returns expected bounds", async () => {
    const path = join(tmp, "bounds.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;

    const sortedKeys = Object.keys(SMALL_DATA).sort();
    const first = await reader.firstKey();
    const last = await reader.lastKey();

    assert.equal(first?.toString(), sortedKeys[0]);
    assert.equal(last?.toString(), sortedKeys[sortedKeys.length - 1]);

    await reader.close();
  });
});

describe("close()", () => {
  test("is idempotent", async () => {
    const path = join(tmp, "close.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;
    await reader.close();
    await reader.close(); // should not throw
  });

  test("subsequent calls throw", async () => {
    const path = join(tmp, "close2.mtbl");
    writeMtblFile(path, makeEntries(SMALL_DATA));

    const reader = new MTBLReader(path);
    await reader.ready;
    await reader.close();

    await assert.rejects(() => reader.get("apple"), /closed/i);
  });
});

describe("scaling: many entries across many blocks", () => {
  test("round-trips 5000 entries", async () => {
    const path = join(tmp, "many.mtbl");
    // 5000 entries, ~50 bytes each, with a small block size so we get
    // many data blocks.
    const entries: { key: Buffer; value: Buffer }[] = [];
    for (let i = 0; i < 5000; i++) {
      const key = Buffer.from(`key-${i.toString().padStart(8, "0")}`);
      const value = Buffer.from(`value-${i}-${"x".repeat(20)}`);
      entries.push({ key, value });
    }
    entries.sort((a, b) => Buffer.compare(a.key, b.key));

    writeMtblFile(path, entries, {
      blockSize: 512, // tiny blocks → many of them
      compression: COMPRESSION_ZLIB,
    });

    const reader = new MTBLReader(path);
    await reader.ready;

    const meta = await reader.metadata();
    assert.equal(Number(meta.entryCount), 5000);
    // Several data blocks, plus the index block.
    assert.ok(Number(meta.dataBlockCount) > 10);

    // Spot-check several point lookups.
    for (const i of [0, 1, 100, 1234, 4999]) {
      const k = `key-${i.toString().padStart(8, "0")}`;
      const got = await reader.get(k);
      assert.ok(got !== null, `expected ${k} to be present`);
      assert.equal(got!.toString(), `value-${i}-${"x".repeat(20)}`);
    }

    // Full iteration count.
    let count = 0;
    for await (const _ of reader.iterate()) count++;
    assert.equal(count, 5000);

    // Range scan.
    let rangeCount = 0;
    for await (const _ of reader.iterate({
      start: "key-00001000",
      end: "key-00002000",
    })) {
      rangeCount++;
    }
    assert.equal(rangeCount, 1000);

    // getMany with shuffled keys.
    const keys = [10, 2500, 4999, 0, 4998, 1].map(
      (i) => `key-${i.toString().padStart(8, "0")}`,
    );
    const result = await reader.getMany(keys);
    assert.equal(result.size, 6);

    await reader.close();
  });
});
