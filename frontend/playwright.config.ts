import { defineConfig } from "@playwright/test";

// The end-to-end tests drive the real app: a backend serving the built
// frontend and the full corpus. Start it first (see README), then
// `npm run e2e`; E2E_BASE_URL points elsewhere.
export default defineConfig({
  testDir: "e2e",
  timeout: 60_000,
  expect: { timeout: 20_000 },
  use: {
    baseURL: process.env.E2E_BASE_URL ?? "http://127.0.0.1:8000",
    viewport: { width: 1440, height: 900 },
  },
  reporter: [["list"]],
});
