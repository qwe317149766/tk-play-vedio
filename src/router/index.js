import { createRouter, createWebHistory } from 'vue-router'
import LandingPage from '../components/LandingPage.vue'
import OrderPage from '../views/OrderPage.vue'

const routes = [
	{
		path: '/',
		name: 'Landing',
		component: LandingPage,
	},
	{
		path: '/order',
		name: 'Order',
		component: OrderPage,
	},
	{
		path: '/:pathMatch(.*)*',
		redirect: '/',
	},
]

export const router = createRouter({
	history: createWebHistory(),
	routes,
})
