import { PGlite } from "https://cdn.jsdelivr.net/npm/@electric-sql/pglite@0.3.15/dist/index.js";
import { worker } from "https://cdn.jsdelivr.net/npm/@electric-sql/pglite@0.3.15/dist/worker/index.js";

worker({
  async init() {
    return new PGlite({ dataDir: "idb://megahal", relaxedDurability: true });
  },
});
