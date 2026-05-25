// vite.config.js
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/tenders': 'http://localhost:8000',
      '/auth':    'http://localhost:8000',
    }
  }
})