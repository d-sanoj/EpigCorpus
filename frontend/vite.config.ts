/// <reference types="vitest/config" />
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In development Vite serves the UI and forwards /api to the backend
// (uv run uvicorn epigcorpus.api.server:app, port 8000). In production the
// backend serves the built files in dist/ itself, on one origin.
const backend = process.env.EPIGCORPUS_API ?? "http://127.0.0.1:8000";

export default defineConfig({
  // GitHub Pages serves the site from /<repository>/, so that build sets
  // VITE_BASE=/EpigCorpus/. The API-served build keeps the root, and the data
  // files the static build fetches resolve against the same base.
  base: process.env.VITE_BASE ?? "/",
  plugins: [react(), tailwindcss()],
  build: {
    // MapLibre (the basemap renderer) is one ~1 MB chunk, loaded on its own
    // after the app; it cannot be split further.
    chunkSizeWarningLimit: 1100,
  },
  server: {
    proxy: { "/api": { target: backend, changeOrigin: true } },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test-setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
