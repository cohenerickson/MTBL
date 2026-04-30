/**
 * Public API for the mtbl reader.
 *
 *   import { MTBLReader } from "mtbl";
 *
 *   const reader = new MTBLReader("./images.mtbl");
 *   await reader.ready;
 *
 *   const value = await reader.get("some-key");
 *   for await (const { key, value } of reader.getPrefix("photos/")) { ... }
 *
 *   await reader.close();
 */

export {
  MTBLReader,
  type IterateOptions,
  type KeyInput,
  type MTBLEntry,
  type MTBLMetadata,
  type MTBLReaderOptions
} from "./reader.js";

export {
  Compression,
  type CompressionId,
  type CompressionName,
  FormatVersion
} from "./constants.js";
