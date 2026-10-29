import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// In dev, proxy API + consumer calls to the local backend/consumer.
// In production the Nginx container proxies /api and /consumer instead.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': { target: 'http://localhost:8080', changeOrigin: true },
      '/consumer': { target: 'http://localhost:8081', changeOrigin: true },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
  },
});
