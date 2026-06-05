import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { fileURLToPath, URL } from 'node:url'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    // Proxy API requests to the Flo dashboard server during development.
    // (Visual-first build runs on mock data; this is here for the later API pass.)
    proxy: {
      '/api/v1/workflow': { target: 'http://localhost:9002', changeOrigin: true },
      '/api': {
        target: 'http://localhost:9002',
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on('proxyRes', (proxyRes, _req, clientRes) => {
            if (proxyRes.headers['content-type']?.includes('text/event-stream')) {
              clientRes.socket?.setNoDelay(true)
            }
          })
        },
      },
    },
  },
  build: {
    // Embedded into the flo binary from web/dist/ — keep this output path.
    outDir: 'dist',
    sourcemap: false,
  },
})
