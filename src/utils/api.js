import axios from 'axios'

const API_BASE_URL = 'https://ins.g123.top/api'

const api = axios.create({
	baseURL: API_BASE_URL,
	headers: {
		'Content-Type': 'application/json',
	},
})

export const apiService = {
	// 服务列表
	async getServiceList() {
		const response = await api.get('/service/getServiceList')
		return response.data
	},

	// 卡详情
	async getCardInfo(orderKey) {
		const response = await api.get('/service/getKamiInfo', {
			params: { orderKey },
		})
		return response.data
	},

	// 父订单列表（根据卡密）
	async getParentOrders(orderKey) {
		const response = await api.get('/service/getJobOrder', {
			params: { orderKey },
		})
		return response.data
	},

	// 子订单列表（根据父订单号）
	async getChildOrders(orderId) {
		const response = await api.get('/service/getJobSubOrderByOrderId', {
			params: { order_id: orderId },
		})
		return response.data
	},
	// 下单 service/createService
	async createService(data) {
		const response = await api.post('/service/createService', data)
		return response.data
	},
}

export default apiService
