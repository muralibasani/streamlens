/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(import.meta.dirname, "src"),
    },
  },
  root: import.meta.dirname,
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  test: {
    globals: true,
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary"],
      reportsDirectory: "./coverage",
    },
  },
  server: {
    proxy: {
      "/api": {
        target: process.env.VITE_API_URL || "http://127.0.0.1:5000",
        changeOrigin: true,
        secure: false,
        ws: false,
        configure: (proxy) => {
          proxy.on("error", (err, _req, res) => {
            console.warn("[vite proxy]", err.message);
          });
          proxy.on("proxyReq", (proxyReq) => {
            proxyReq.setHeader("Connection", "keep-alive");
          });
        },
      },
    },
  },
});
