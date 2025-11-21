import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 3000,
    open: true,
    allowedHosts: [
      'testapi.g123.top', // 添加你想允许的主机名
      'localhost',         // 如果需要，可以同时允许 localhost
      '127.0.0.1'          // 如果需要，也可以加入本地 IP 地址
    ]
  }
  
})

