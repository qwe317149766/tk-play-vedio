// 本地存储工具
export const storage = {
  get(key) {
    try {
      const item = localStorage.getItem(key)
      return item ? JSON.parse(item) : null
    } catch {
      return localStorage.getItem(key)
    }
  },

  set(key, value) {
    try {
      localStorage.setItem(key, typeof value === 'string' ? value : JSON.stringify(value))
    } catch (error) {
      console.error('存储失败:', error)
    }
  },

  remove(key) {
    localStorage.removeItem(key)
  }
}

