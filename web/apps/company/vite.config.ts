import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// In dev the app and its two backends share an origin: /api goes to the
// Admin API and /live-api to the live-monitor service, prefixes stripped.
const strip = (prefix: string) => (path: string) => path.slice(prefix.length);

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174,
    proxy: {
      "/api": { target: process.env.ADMIN_API_URL ?? "http://localhost:8082", changeOrigin: true, rewrite: strip("/api") },
      "/live-api": { target: process.env.LIVE_URL ?? "http://localhost:8083", changeOrigin: true, rewrite: strip("/live-api") },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./test/setup.ts"],
  },
});
