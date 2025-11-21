// 读取文本文件
export function readTextFile(file) {
	return new Promise((resolve, reject) => {
		const reader = new FileReader()
		reader.onload = e => resolve(e.target.result)
		reader.onerror = e => reject(e)
		reader.readAsText(file, 'UTF-8')
	})
}
