import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The app talks only to the Candidate Workspace. In dev, /session is
// proxied to candidate-api so the app and API share an origin.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/session": { target: process.env.WORKSPACE_URL ?? "http://localhost:8081", changeOrigin: true },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./test/setup.ts"],
  },
});
