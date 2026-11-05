import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { marcoDocs } from './docs-build/plugin.mjs'

declare const process: { env: Record<string, string | undefined> }

// `npm run dev` proxies the API to a running marco's admin listener (default :7301);
// `npm run dev:mock` serves the console on its mock transport, with no server.
const target = process.env.MARCO_URL ?? 'http://127.0.0.1:7301'

export default defineConfig({
  plugins: [react(), marcoDocs()],
  base: '/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    // no inline polyfill script: the page CSP is script-src 'self'
    modulePreload: { polyfill: false },
    // fonts as files: the CSP's font-src is 'self', not data:
    assetsInlineLimit: 0,
  },
  server: {
    port: 5730,
    proxy: {
      '/marco.v1.Graph': { target, changeOrigin: false },
      '/marco.v1.Admin': { target, changeOrigin: false },
      '/debug': { target, changeOrigin: false },
      '/metrics': { target, changeOrigin: false },
      '/readyz': { target, changeOrigin: false },
      '/resolve': { target, changeOrigin: false },
    },
  },
})
