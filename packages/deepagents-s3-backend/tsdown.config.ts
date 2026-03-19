import { defineConfig } from "tsdown";

// Mark all node_modules as external since this is a library
const neverBundle = [/^[^./]/];

export default defineConfig([
  {
    entry: ["./src/index.ts"],
    format: ["esm"],
    dts: true,
    clean: true,
    sourcemap: true,
    minify: true,
    outDir: "dist",
    outExtensions: () => ({ js: ".js" }),
    deps: {
      neverBundle,
    },
  },
  {
    entry: ["./src/index.ts"],
    format: ["cjs"],
    dts: true,
    clean: true,
    sourcemap: true,
    minify: true,
    outDir: "dist",
    outExtensions: () => ({ js: ".cjs" }),
    deps: {
      neverBundle,
    },
  },
]);
