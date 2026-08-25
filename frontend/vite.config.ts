import fs from "fs"
import path from "path"
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { defineConfig, loadEnv } from "vite"

// Canonical docs live at repo-root docs/. The Docker build flattens frontend/
// to the image root so docs/ sits beside it (<root>/docs); on a host checkout
// the repo root is one level up from this config. Resolve whichever exists so
// the `@docs` alias works in both layouts.
const docsDir = fs.existsSync(path.resolve(__dirname, "docs"))
  ? path.resolve(__dirname, "docs")
  : path.resolve(__dirname, "../docs")

const mermaidChunk = [
  "mermaid",
  "cytoscape",
  "katex",
  "react-markdown",
  "remark-gfm",
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
export default defineConfig(({mode}) => {
  // loadEnv reads .env files
  const env = loadEnv(mode, process.cwd(), "")

  return {
    plugins: [react(), tailwindcss()],
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
        "@docs": docsDir,
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
      // Listen on all interfaces so the dev server is reachable from outside
      // the container.
      host: true,
      // Only accept requests for known hostnames (defends against DNS-rebinding
      // now that the server binds all interfaces). localhost is always allowed;
      // set VITE_ALLOWED_HOSTS (comma-separated) to add dev domains/container
      // hostnames as needed.
      allowedHosts: [
        "localhost",
        "127.0.0.1",
        ...(env.VITE_ALLOWED_HOSTS
          ? env.VITE_ALLOWED_HOSTS.split(",").map((h) => h.trim()).filter(Boolean)
          : []),
      ],
      // Bind-mounted source on Docker Desktop (macOS/Windows) doesn't deliver
      // native filesystem events reliably, so poll to keep HMR working.
      watch: {
        usePolling: true,
      },
      // Allow serving the bundled docs dir when it lives outside the Vite root
      // (host checkout); inside the container it's already under the root.
      fs: {
        allow: [path.resolve(__dirname), docsDir],
      },
      // Dev proxy to avoid CORS when backend runs on localhost:4000
      proxy: {
        '/api': {
          // let's have the target be configurable via env var
          target: env.VITE_API_PROXY_TARGET || 'http://localhost:4000',
          changeOrigin: true,
          secure: false,
          rewrite: (path) => path.replace(/^\/api/, ''),
        },
      },
    },
  }
})
