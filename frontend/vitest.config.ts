import path from "path"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vitest/config"

// Kept separate from vite.config.ts so the production build never has to load
// vitest: `vite build` reads only vite.config.ts, and the Docker image would
// otherwise fail if dev dependencies were ever pruned.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    include: ["src/**/*.test.{ts,tsx}"],
  },
})
