import path from "path"
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

const mermaidChunk = [
  "mermaid",
  "cytoscape",
  "katex",
  "react-markdown",
  "remark-gfm",
  "rehype-raw",
]
const largeDeps = [
  "chevrotain",
  "langium",
  "layout-base",
  "lodash-es",
  "marked",
  "dagre-d3-es",
]

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  optimizeDeps: {
    include: ['mermaid'],
  },
  build: {
    commonjsOptions: {
      include: [/mermaid/, /node_modules/],
    },
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("node_modules")) {
            // Mermaid and some of its dependencies in their own chunk
            // to avoid bloating other chunks
            for (const dep of mermaidChunk) {
              if (id.includes(dep)) return "mermaid"
            }
            
            // Large dependencies in their own chunks to avoid bloating vendor
            for (const dep of largeDeps) {
              if (id.includes(dep)) return dep
            }

            if (id.includes("react-router")) return "router"
            if (id.includes("@radix-ui")) return "radix"
            if (id.includes("@dnd-kit")) return "dnd"
            if (id.includes("recharts")) return "charts"
            if (id.includes("lucide-react")) return "icons"

            // bundle pako (used to render gzipped data) separately
            if (id.includes("pako")) return "pako"

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
