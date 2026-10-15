import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const apiProxy = {
  '/api': {
    target: process.env.VITE_API_PROXY_TARGET || 'http://localhost:8002',
    changeOrigin: true,
    rewrite: (path) => path.replace(/^\/api/, ''),
  },
}

export default defineConfig({
  plugins: [react()],
  server: { host: '127.0.0.1',
    port: 5174,
    strictPort: true,
    proxy: apiProxy,
  },
  preview: { host: '127.0.0.1', port: 5174, strictPort: true, proxy: apiProxy },
})
