import path from "path"
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("node_modules")) {
            if (id.includes("react-router")) return "router"
            if (id.includes("@radix-ui")) return "radix"
            if (id.includes("@dnd-kit")) return "dnd"
            if (id.includes("recharts")) return "charts"
            if (id.includes("lucide-react")) return "icons"
            return "vendor"
          }
        },
      },
    },
  },
  server: {
    // Dev proxy to avoid CORS when backend runs on localhost:4000
    proxy: {
      '/api': {
        // let's have the target be configurable via env var
        target: process.env.VITE_API_PROXY_TARGET || 'http://localhost:4000',
        changeOrigin: true,
        secure: false,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
