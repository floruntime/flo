import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],
  server: {
    // Proxy API requests to the Flo dashboard server during development
    proxy: {
      // Workflow REST + SSE → dashboard server (port 9002)
      // Dashboard server has access to ALL dispatchers via routeToShard(),
      // avoiding the SO_REUSEPORT cross-shard issue on port 9000.
      '/api/v1/workflow': {
        target: 'http://localhost:9002',
        changeOrigin: true,
      },
      // Dashboard API → dashboard server (aggregates, SSE, etc.)
      '/api': {
        target: 'http://localhost:9002',
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on('proxyRes', (proxyRes, _req, clientRes) => {
            if (proxyRes.headers['content-type']?.includes('text/event-stream')) {
              clientRes.socket?.setNoDelay(true);
            }
          });
        },
      },
    },
  },
  build: {
    // Output to dist directory for production builds
    outDir: 'dist',
    // Source maps add ~4.4 MB to the embedded binary — off by default.
    // Enable when needed: npm run build -- --sourcemap
    sourcemap: false,
    rollupOptions: {
      output: {
        // Split vendor chunks so large libs don't bloat the main bundle
        manualChunks: {
          'react-vendor': ['react', 'react-dom', 'react-router-dom'],
          'charts': ['recharts'],
        },
      },
    },
  },
})
