import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// base: "./" so the built site works from any path — a file:// open, a subdirectory
// on a static host, or GitHub Pages — without being rebuilt for each.
export default defineConfig({
  plugins: [react()],
  base: "./",
  server: { port: 5173 },
});
