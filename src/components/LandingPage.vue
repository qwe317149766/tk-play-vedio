<template>
	<div class="landing-shell" :style="themeStyle">
		<a-layout class="landing-layout">
			<!-- 固定头部导航 -->
			<a-layout-header class="lp-header">
				<div class="lp-header-inner">
					<div class="lp-header-left">
						<div class="lp-logo">
							<span class="lp-logo-mark">gd</span>
							<span class="lp-logo-text">云控</span>
						</div>
						<!-- 顶部左侧产品切换 -->
						<div class="lp-product-switch">
							<div
								v-for="product in products"
								:key="product.id"
								:class="[
									'lp-product-pill',
									{ active: currentProductId === product.id },
								]"
								@click="currentProductId = product.id">
								<span class="lp-product-name">
									{{ productLabel(product) }}
								</span>
							</div>
						</div>
					</div>
					<div class="lp-actions">
						<div class="lp-lang-switch">
							<button
								v-for="lang in ['zh', 'en']"
								:key="lang"
								:class="['lp-lang-btn', { active: currentLang === lang }]"
								@click="currentLang = lang">
								{{ lang === 'zh' ? '中' : 'EN' }}
							</button>
						</div>
						<a-button class="lp-ghost-btn" ghost @click="openTelegram">
							{{ texts.actions.telegram }}
						</a-button>
					</div>
				</div>
			</a-layout-header>

			<a-layout-content>
				<LandingTikTokContent
					v-if="currentProductId === 'tiktok'"
					:texts="texts"
					:is-yearly="isYearly"
					@toggle-yearly="isYearly = !isYearly"
					@enter="handleEnter" />
				<LandingInstagramContent v-else :texts="texts" @enter="handleEnter" />
			</a-layout-content>
		</a-layout>
	</div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRouter } from 'vue-router'
import LandingTikTokContent from './LandingTikTokContent.vue'
import LandingInstagramContent from './LandingInstagramContent.vue'

const router = useRouter()

const currentLang = ref('zh')
const isYearly = ref(false)
const currentProductId = ref('tiktok')

const products = [
	{
		id: 'tiktok',
		icon: '🎵',
		nameZh: 'TikTok',
		nameEn: 'TikTok',
		primary: '#25F4EE',
		secondary: '#FE2C55',
		accent: '#000000',
	},
	// {
	// 	id: 'instagram',
	// 	icon: '📸',
	// 	nameZh: 'Instagram',
	// 	nameEn: 'Instagram',
	// 	primary: '#F58529',
	// 	secondary: '#DD2A7B',
	// 	accent: '#515BD4',
	// },
]

const copy = {
	zh: {
		nav: {
			product: '产品',
			features: '能力',
			pricing: '价格',
			testimonials: '用户反馈',
		},
		actions: {
			start: '立即开始',
			startNow: '马上开始使用',
			enterSystem: '进入下单系统',
			viewDemo: '查看示例数据',
			subscribe: 'Telegram 咨询套餐',
			telegram: 'Telegram 咨询',
			contact: '联系团队',
		},
		hero: {
			tagLine: 'TikTok / Instagram 私信自动化平台',
			title: '让海外私信增长，像并发流水线一样稳定运转',
			subtitle:
				'批量账号管理、多类型消息并发调度与实时数据看板，让海外私信获客与触达像流水线一样稳定运行。',
			footnote:
				'支持 TikTok 与 Instagram，多账号并发发送、账号风控提示、失败重试全链路可视化。',
			badges: {
				pipeline: '从采集到发送，一条链路打通',
				dashboard: '实时数据看板与任务追踪',
			},
		},
		stats: {
			title: '数据看板示例',
			today: '今日发送',
			sent: '发送条数',
			successRate: '成功率',
			deliveryRate: '到达率',
			pending: '待处理',
			trendTitle: '最近 24 小时发送趋势',
			taskOverview: '任务概览',
			running: '进行中',
			done: '已完成',
			failed: '失败',
		},
		abilities: {
			title: '优势能力',
			subtitle:
				'围绕账号稳定性、发送成功率和运营效率，搭建了一套适合长期跑批的私信基础设施。',
			items: [
				{
					key: 'success',
					icon: '📨',
					title: '95%+ 发送成功率',
					desc: '并发调度 + 失败重试，整批名单稳定跑完不漏客。',
				},
				{
					key: 'delivery',
					icon: '📬',
					title: '98%+ 消息到达率',
					desc: '账号均摊与风控提示，降低异常波动和集中封禁风险。',
				},
				{
					key: 'manage',
					icon: '🗂️',
					title: '精细筛选与批量管理',
					desc: '按分组、状态、发送量多维筛选账号与客户，批量操作更省事。',
				},
				{
					key: 'dashboard',
					icon: '📊',
					title: '数据看板与任务追踪',
					desc: '发送进度、成功/失败统计与任务完成情况，一眼看清当前批次执行效果。',
				},
			],
		},
		collect: {
			title: '多样化消息类型',
			subtitle:
				'根据不同转化目标组合多种消息形态，在一次触达里同时承载介绍、导流与行动召唤。',
			items: [
				{
					key: 'text',
					title: '文本消息',
					desc: '支持多语言文本模版，适配不同人群与话术风格。',
				},
				{
					key: 'live',
					title: '分享直播间',
					desc: '一键附带直播间卡片，引导用户直接进入当前或预告直播。',
				},
				{
					key: 'post',
					title: '分享作品',
					desc: '将指定视频/作品直接附在私信中，用内容本身完成种草与转化。',
				},
				{
					key: 'profile',
					title: '分享用户名片',
					desc: '在私信中附带账号名片，方便用户一键关注或查看主页。',
				},
				{
					key: 'card',
					title: '卡片消息',
					desc: '支持带封面、标题与链接的卡片形式，用于承接店铺、落地页或活动页。',
				},
			],
			extra:
				'除主文本消息外，还可以在同一条私信中追加补充文本，让介绍和行动指引同时呈现。',
		},
		pricing: {
			title: '订阅价格',
			subtitle:
				'当前支持月卡、年卡与代理合作三种模式，可根据使用规模灵活选择。',
			accountLimit: '账号上限：',
			dailyQuota: '每日发送额度：',
			concurrent: '并发任务上限：',
			perSuccess: '成功条',
			payAsYouGoTitle: '按量付费',
			payAsYouGoDesc:
				'适合前期测试、波动需求或不想维护账号池的场景，仅按成功条数计费。',
			benefits: [
				'📊 高质量账号与用户数据来源',
				'💬 支持多语言文本消息模板',
				'📺 支持分享直播间',
				'🎬 支持分享作品',
				'👤 支持分享用户名片',
				'🧾 支持卡片消息',
			],
			plans: [
				{
					key: 'monthly',
					name: '月卡',
					price: 3000,
					cycle: '月',
					desc: '适合日常稳定运营，按月续费，随时按需增删账号。',
					accounts: '200 个',
					daily: '10,000 条',
					concurrent: '5 个',
					highlight: true,
				},
				{
					key: 'yearly',
					name: '年卡',
					price: 28800,
					cycle: '年',
					desc: '一年打包购买更划算，适合长期运营团队与机构。',
					accounts: '300 个',
					daily: '30,000 条',
					concurrent: '10 个',
					highlight: false,
				},
				{
					key: 'agent',
					name: '代理合作',
					price: 0,
					priceText: '联系咨询',
					cycle: '',
					desc: '支持代理与批量账号合作模式，请联系团队获取专属政策与报价。',
					accounts: '按需配置',
					daily: '按需配置',
					concurrent: '按需配置',
					highlight: false,
					extraBenefits: ['🔑 支持卡密分配，方便二级代理与账号交付'],
				},
			],
		},
		testimonials: {
			title: '用户推荐',
			subtitle:
				'来自不同团队的真实反馈，覆盖跨境电商、MCN、SaaS、品牌与本地生活等场景。',
			items: [
				{
					initial: '林',
					name: '林先生',
					title: '某跨境电商 · 运营总监',
					quote:
						'从采集到触达一条链路打通后，我们的私信效率提升非常明显，团队也更容易把精力放在转化上。',
				},
				{
					initial: '张',
					name: '张女士',
					title: '某 MCN 机构 · 增长负责人',
					quote:
						'账号分组和任务并发很实用，稳定性也比我们之前的方案好，数据看板对复盘很有帮助。',
				},
				{
					initial: '王',
					name: '王先生',
					title: '某 SaaS 团队 · 运维负责人',
					quote:
						'接入后几乎不需要额外维护，异常账号提示和失败统计让排查成本下降很多。',
				},
				{
					initial: '赵',
					name: '赵女士',
					title: '某本地生活 · 运营负责人',
					quote:
						'按量付费很适合我们前期测试素材和话术，成本清晰，效果一眼就能看出来。',
				},
			],
		},
		finalCta: {
			title: '让 TikTok / Instagram 私信增长，像生产线一样稳定运行',
			subtitle:
				'几分钟完成对接，即刻开始测试，从小规模试跑到大规模稳定运营，一套系统搞定。',
			highlights: [
				'几分钟完成接入与首批测试',
				'按量 / 套餐灵活选择成本可控',
				'并发调度自动跑完整批名单',
				'实时看板随时查看发送进度与效果',
			],
		},
		footer: {
			terms: '使用条款',
			privacy: '隐私政策',
		},
	},
	en: {
		nav: {
			product: 'Product',
			features: 'Features',
			pricing: 'Pricing',
			testimonials: 'Customers',
		},
		actions: {
			start: 'Get started',
			startNow: 'Start now',
			enterSystem: 'Go to order system',
			viewDemo: 'View sample dashboard',
			subscribe: 'Talk on Telegram',
			telegram: 'Talk on Telegram',
			contact: 'Contact sales',
		},
		hero: {
			tagLine: 'TikTok / Instagram DM automation platform',
			title: 'Run outbound DMs like a stable concurrency pipeline',
			subtitle:
				'Manage accounts in bulk, orchestrate diverse DM types and track everything in one real‑time dashboard.',
			footnote:
				'Supports both TikTok and Instagram, with multi-account concurrency, risk alerts and automatic retries across the whole flow.',
			badges: {
				pipeline: 'End‑to‑end from collection to sending',
				dashboard: 'Real‑time dashboard & task tracking',
			},
		},
		stats: {
			title: 'Sample dashboard',
			today: 'Today',
			sent: 'Messages sent',
			successRate: 'Success rate',
			deliveryRate: 'Delivery rate',
			pending: 'Pending',
			trendTitle: 'Send trend in last 24 hours',
			taskOverview: 'Task overview',
			running: 'Running',
			done: 'Completed',
			failed: 'Failed',
		},
		abilities: {
			title: 'Capabilities',
			subtitle:
				'Standardize your DM growth workflow from collection to sending and analytics, so your team can scale with confidence.',
			items: [
				{
					key: 'success',
					icon: '📨',
					title: '95%+ send success rate',
					desc: 'Smart concurrency and automatic retries help you reliably finish every batch.',
				},
				{
					key: 'delivery',
					icon: '📬',
					title: '98%+ delivery rate',
					desc: 'Account load balancing and risk alerts keep performance stable.',
				},
				{
					key: 'manage',
					icon: '🗂️',
					title: 'Granular account & user management',
					desc: 'Filter by group, status and volume, then apply bulk operations with ease.',
				},
				{
					key: 'dashboard',
					icon: '📊',
					title: 'Analytics & task tracking',
					desc: 'Progress, success/failure stats, and trends in one clean dashboard.',
				},
			],
		},
		collect: {
			title: 'Rich message types',
			subtitle:
				'Combine multiple message formats in a single DM to handle education, engagement and conversion together.',
			items: [
				{
					key: 'text',
					title: 'Text messages',
					desc: 'Support multilingual templates so you can adapt tone and copy to different audiences.',
				},
				{
					key: 'live',
					title: 'Share live rooms',
					desc: 'Attach a live room card in DMs to drive users directly into current or upcoming streams.',
				},
				{
					key: 'post',
					title: 'Share posts',
					desc: 'Attach specific videos/posts in the DM so the content itself does the storytelling.',
				},
				{
					key: 'profile',
					title: 'Share profiles',
					desc: 'Include profile cards so users can follow or view your account in one tap.',
				},
				{
					key: 'card',
					title: 'Card messages',
					desc: 'Support rich cards with cover, title and link for shops, landing pages or campaigns.',
				},
			],
			extra:
				'On top of the main text message you can append an extra piece of text, so both context and call‑to‑action are clear.',
		},
		pricing: {
			title: 'Pricing',
			subtitle:
				'We currently offer Monthly, Yearly and Agency plans — pick what matches your volume and collaboration model.',
			accountLimit: 'Account limit: ',
			dailyQuota: 'Daily send quota: ',
			concurrent: 'Concurrent tasks: ',
			perSuccess: 'successful DM',
			payAsYouGoTitle: 'Pay‑as‑you‑go',
			payAsYouGoDesc:
				'Great for early testing or bursty campaigns — pay only for successful deliveries.',
			benefits: [
				'📊 High‑quality account & user data',
				'💬 Multilingual text templates',
				'📺 Attach live room cards',
				'🎬 Share posts',
				'👤 Share profiles',
				'🧾 Send rich cards',
			],
			plans: [
				{
					key: 'monthly',
					name: 'Monthly',
					price: 3000,
					cycle: 'month',
					desc: 'Best for ongoing operations with flexible month‑to‑month control.',
					accounts: '200',
					daily: '10,000',
					concurrent: '5',
					highlight: true,
				},
				{
					key: 'yearly',
					name: 'Yearly',
					price: 28800,
					cycle: 'year',
					desc: 'Discounted annual pricing for teams running DM at scale long‑term.',
					accounts: '300',
					daily: '30,000',
					concurrent: '10',
					highlight: false,
				},
				{
					key: 'agent',
					name: 'Agency / Reseller',
					price: 0,
					priceText: 'Contact us',
					cycle: '',
					desc: 'For agencies and resellers managing large matrices — contact us for tailored limits and terms.',
					accounts: 'Flexible',
					daily: 'Flexible',
					concurrent: 'Flexible',
					highlight: false,
					extraBenefits: [
						'🔑 Support card-key distribution for downstream resellers',
					],
				},
			],
		},
		testimonials: {
			title: 'What teams say',
			subtitle:
				'Trusted by cross‑border e‑commerce, MCN, SaaS, brands and local services teams to scale outbound DMs.',
			items: [
				{
					initial: 'L',
					name: 'Mr. Lin',
					title: 'Ops Director · Cross‑border e‑commerce',
					quote:
						'Once the whole flow from collection to sending was unified, our DM efficiency jumped and the team could focus on conversion.',
				},
				{
					initial: 'Z',
					name: 'Ms. Zhang',
					title: 'Growth Lead · MCN agency',
					quote:
						'Account grouping and concurrency are very practical, and the dashboard makes reviewing campaigns much easier.',
				},
				{
					initial: 'W',
					name: 'Mr. Wang',
					title: 'Ops Lead · SaaS team',
					quote:
						'After onboarding, we barely need extra maintenance. Risk alerts and failure stats save a lot of debugging time.',
				},
				{
					initial: 'Z',
					name: 'Ms. Zhao',
					title: 'Ops Lead · Local services',
					quote:
						'Pay‑as‑you‑go is perfect for testing creatives and scripts. Costs are transparent and performance is obvious.',
				},
			],
		},
		finalCta: {
			title: 'Run your TikTok / Instagram DM like a reliable production line',
			subtitle:
				'Onboard in minutes, start small‑scale tests and scale to large‑volume operations — all in one system.',
			highlights: [
				'Get onboarded and send your first batch within minutes',
				'Mix pay‑as‑you‑go and plans to keep costs predictable',
				'Let smart concurrency finish every target list automatically',
				'Monitor progress and performance from a single live dashboard',
			],
		},
		footer: {
			terms: 'Terms',
			privacy: 'Privacy',
		},
	},
}

const texts = computed(() => copy[currentLang.value])

const currentProduct = computed(
	() => products.find(p => p.id === currentProductId.value) || products[0]
)

const themeStyle = computed(() => {
	const p = currentProduct.value
	return {
		'--lp-primary': p.primary,
		'--lp-secondary': p.secondary,
		'--lp-accent': p.accent,
		background: `radial-gradient(circle at top left, ${p.primary}44, transparent 55%),
                 radial-gradient(circle at bottom right, ${p.secondary}44, transparent 55%),
                 #020617`,
	}
})

function handleEnter() {
	router.push('/order')
}

function productLabel(product) {
	return currentLang.value === 'zh' ? product.nameZh : product.nameEn
}

function openTelegram() {
	window.open('https://t.me/igcreates', '_blank')
}

function displayPrice(base) {
	if (!isYearly.value) return `$${base}`
	// 简单示例：年付按 8 折
	const yearly = base * 12 * 0.8
	return `$${yearly.toFixed(0)}`
}
</script>

<style>
.landing-shell {
	height: 100vh;
	overflow-y: auto;
}

.landing-layout {
	min-height: 100vh;
	background: transparent;
}

.landing-page {
	max-width: 1200px;
	margin: 0 auto;
	padding: 96px 32px 24px;
	display: flex;
	flex-direction: column;
	gap: 40px;
	color: #e5e7eb;
}

.lp-header {
	position: fixed;
	top: 0;
	left: 0;
	right: 0;
	z-index: 50;
	display: flex;
	justify-content: center;
	backdrop-filter: blur(20px);
	-webkit-backdrop-filter: blur(20px);
	line-height: 1;
	background: radial-gradient(
			circle at 0% 0%,
			rgba(148, 163, 184, 0.25),
			transparent 55%
		),
		radial-gradient(circle at 100% 0%, rgba(79, 70, 229, 0.18), transparent 55%),
		linear-gradient(
			to bottom,
			rgba(2, 6, 23, 0.88),
			rgba(2, 6, 23, 0.8),
			rgba(2, 6, 23, 0.4),
			transparent
		);
	border-bottom: 1px solid rgba(148, 163, 184, 0.35);
	box-shadow: 0 10px 30px rgba(15, 23, 42, 0.45);
}

.lp-header-inner {
	width: 1200px;
	margin: 0 auto;
	display: flex;
	align-items: center;
	justify-content: space-between;
	gap: 24px;
	line-height: 1;
	padding: 12px 32px;
}

.lp-header-left {
	display: inline-flex;
	align-items: center;
	gap: 14px;
	padding: 6px 10px 6px 8px;
	border-radius: 999px;
	background: radial-gradient(
			circle at 0% 0%,
			rgba(148, 163, 184, 0.32),
			transparent 55%
		),
		radial-gradient(
			circle at 100% 100%,
			rgba(56, 189, 248, 0.36),
			transparent 55%
		),
		rgba(15, 23, 42, 0.82);
	box-shadow: 0 18px 40px rgba(15, 23, 42, 0.85);
	backdrop-filter: blur(24px);
	-webkit-backdrop-filter: blur(24px);
}

.lp-logo {
	display: flex;
	align-items: center;
	gap: 8px;
	font-weight: 700;
	font-size: 18px;
}

.lp-logo-mark {
	width: 28px;
	height: 28px;
	border-radius: 8px;
	background: linear-gradient(135deg, var(--lp-primary), var(--lp-secondary));
	display: inline-flex;
	align-items: center;
	justify-content: center;
	color: #fff;
	font-size: 16px;
}

.lp-logo-text {
	color: #e5e7eb;
}

.lp-nav {
	display: flex;
	align-items: center;
	gap: 20px;
	flex: 1;
	justify-content: center;
}

.lp-nav-link {
	font-size: 14px;
	color: #e5e7eb;
	text-decoration: none;
	position: relative;
	padding-bottom: 4px;
}

.lp-nav-link::after {
	content: '';
	position: absolute;
	left: 0;
	bottom: 0;
	width: 0;
	height: 2px;
	background: linear-gradient(135deg, var(--lp-primary), var(--lp-secondary));
	transition: width 0.2s ease;
}

.lp-nav-link:hover::after {
	width: 100%;
}

.lp-actions {
	display: flex;
	align-items: center;
	gap: 12px;
}

.lp-lang-switch {
	display: inline-flex;
	align-items: center;
	background: rgba(15, 23, 42, 0.85);
	border-radius: 999px;
	padding: 3px;
	border: 1px solid rgba(148, 163, 184, 0.45);
}

.lp-lang-btn {
	border: none;
	background: transparent;
	min-width: 34px;
	padding: 5px 10px;
	border-radius: 999px;
	font-size: 12px;
	cursor: pointer;
	color: #cbd5f5;
	font-weight: 500;
}

.lp-lang-btn.active {
	background: linear-gradient(135deg, var(--lp-primary), var(--lp-secondary));
	color: #fff;
	box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.6);
}

.lp-primary-btn {
	background: linear-gradient(135deg, var(--lp-primary), var(--lp-secondary));
	border: none;
}

.lp-ghost-btn {
	border-radius: 999px;
	border-color: rgba(148, 163, 184, 0.65);
	color: #e5e7eb;
	background: transparent;
	height: 36px;
	padding: 0 18px;
	display: inline-flex;
	align-items: center;
}

.lp-hero {
	display: grid;
	grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr);
	gap: 40px;
	align-items: center;
}

.lp-hero-left {
	display: flex;
	flex-direction: column;
	gap: 16px;
	position: relative;
	padding: 18px 18px 20px;
	border-radius: 26px;
	/* background: radial-gradient(
			circle at 0% 0%,
			rgba(148, 163, 184, 0.35),
			transparent 55%
		),
		radial-gradient(
			circle at 100% 100%,
			rgba(56, 189, 248, 0.42),
			transparent 55%
		),
		rgba(15, 23, 42, 0.88); */
	/* box-shadow: 0 28px 70px rgba(15, 23, 42, 0.9); */
	backdrop-filter: blur(28px);
	-webkit-backdrop-filter: blur(28px);
}

.lp-hero-tag {
	display: inline-flex;
	align-items: center;
	padding: 4px 10px;
	border-radius: 999px;
	background: rgba(15, 23, 42, 0.04);
	font-size: 12px;
	color: #4b5563;
}

.lp-hero-title {
	font-size: 34px;
	line-height: 1.2;
	font-weight: 800;
	color: #f9fafb;
}

.lp-hero-subtitle {
	font-size: 14px;
	color: #e5e7eb;
	max-width: 520px;
}

.lp-product-switch {
	display: inline-flex;
	padding: 2px 4px;
	background: rgba(255, 255, 255, 0.92);
	border-radius: 999px;
	border: 1px solid rgba(148, 163, 184, 0.3);
	gap: 2px;
}

.lp-product-pill {
	display: inline-flex;
	align-items: center;
	gap: 4px;
	padding: 4px 8px;
	border-radius: 999px;
	font-size: 11px;
	cursor: pointer;
	color: #4b5563;
}

.lp-product-pill.active {
	background: linear-gradient(135deg, var(--lp-primary), var(--lp-secondary));
	color: #fff;
}

.lp-product-icon {
	font-size: 14px;
}

.lp-badges {
	display: flex;
	flex-wrap: wrap;
	gap: 8px;
}

.lp-badge {
	padding: 4px 10px;
	border-radius: 999px;
	background: rgba(15, 23, 42, 0.85);
	border: 1px solid rgba(148, 163, 184, 0.6);
	box-shadow: 0 10px 20px rgba(15, 23, 42, 0.7);
}

.lp-badge-label {
	font-size: 12px;
	color: #e5e7eb;
}

.lp-cta-row {
	display: flex;
	align-items: center;
	gap: 16px;
	margin-top: 4px;
}

.lp-text-btn {
	border: none;
	background: transparent;
	font-size: 13px;
	color: #4b5563;
	cursor: pointer;
	text-decoration: underline;
	text-underline-offset: 3px;
}

.lp-hero-footnote {
	font-size: 12px;
	color: #9ca3af;
	max-width: 520px;
}

.lp-hero-right {
	display: flex;
	justify-content: flex-end;
	align-items: center;
}

.hero-visual {
	position: relative;
	width: 100%;
	max-width: 460px;
	height: 260px;
	border-radius: 32px;
	background: radial-gradient(
			circle at 0% 0%,
			rgba(148, 163, 184, 0.45),
			transparent 60%
		),
		radial-gradient(
			circle at 100% 100%,
			rgba(56, 189, 248, 0.55),
			transparent 60%
		),
		rgba(15, 23, 42, 0.96);
	box-shadow: 0 26px 60px rgba(15, 23, 42, 0.95);
	backdrop-filter: blur(26px);
	-webkit-backdrop-filter: blur(26px);
	overflow: hidden;
	padding: 18px 18px 16px;
	display: flex;
	align-items: center;
	justify-content: center;
}

.hero-orbit-scene {
	position: relative;
	width: 100%;
	height: 100%;
}

.hero-node {
	position: absolute;
	display: flex;
	align-items: center;
	justify-content: center;
	color: #e5e7eb;
}

.hero-node .glyph {
	display: flex;
	align-items: center;
	justify-content: center;
	border-radius: 999px;
	background: rgba(15, 23, 42, 0.9);
	box-shadow: 0 10px 25px rgba(15, 23, 42, 0.9);
	font-size: 22px;
	width: 44px;
	height: 44px;
}

.hero-node-main {
	top: 50%;
	left: 50%;
	transform: translate(-50%, -50%);
}

.hero-node-main .glyph {
	width: 72px;
	height: 72px;
	font-size: 32px;
	background: linear-gradient(135deg, var(--lp-primary), var(--lp-secondary));
	color: #0f172a;
	box-shadow: 0 0 0 4px rgba(15, 23, 42, 0.95),
		0 18px 40px rgba(15, 23, 42, 0.95);
}

.hero-node-main .ant-badge {
	display: inline-flex;
	align-items: center;
	justify-content: center;
}

.hero-node-main .ant-badge-count {
	min-width: 30px;
	height: 18px;
	line-height: 18px;
	background: rgba(15, 23, 42, 0.9);
	border-radius: 999px;
	padding: 0 6px;
	box-shadow: 0 8px 18px rgba(15, 23, 42, 0.9);
	font-size: 11px;
}

.hero-orbit-ring {
	position: absolute;
	inset: 18px;
	border-radius: 999px;
	animation: orbit-rotate 22s linear infinite;
}

.hero-node-text {
	top: 0;
	left: 50%;
	transform: translate(-50%, -50%);
}

.hero-node-live {
	top: 50%;
	right: 0;
	transform: translate(50%, -50%);
}

.hero-node-post {
	bottom: 0;
	left: 50%;
	transform: translate(-50%, 50%);
}

.hero-node-profile {
	top: 50%;
	left: 0;
	transform: translate(-50%, -50%);
}

.hero-node-card {
	bottom: 8%;
	right: 12%;
}

.hero-packet {
	position: absolute;
	width: 6px;
	height: 6px;
	border-radius: 999px;
	background: linear-gradient(135deg, var(--lp-primary), var(--lp-secondary));
	box-shadow: 0 0 0 2px rgba(15, 23, 42, 0.95), 0 0 12px rgba(56, 189, 248, 0.9);
	opacity: 0;
}

.packet-1 {
	top: 8%;
	left: 50%;
	animation: hero-send-1 3.4s ease-in-out infinite;
}

.packet-2 {
	top: 60%;
	right: -4%;
	animation: hero-send-2 3.8s ease-in-out infinite;
}

.packet-3 {
	bottom: -4%;
	left: 40%;
	animation: hero-send-3 4.1s ease-in-out infinite;
}

@keyframes orbit-rotate {
	from {
		transform: rotate(0deg);
	}

	to {
		transform: rotate(360deg);
	}
}

@keyframes hero-send-1 {
	0% {
		transform: translate(-50%, -50%);
		opacity: 0;
	}

	15% {
		opacity: 1;
	}

	60% {
		transform: translate(-50%, 40px);
		opacity: 1;
	}

	100% {
		transform: translate(-50%, 60px);
		opacity: 0;
	}
}

@keyframes hero-send-2 {
	0% {
		transform: translate(0, -50%);
		opacity: 0;
	}

	20% {
		opacity: 1;
	}

	70% {
		transform: translate(-80px, -20px);
		opacity: 1;
	}

	100% {
		transform: translate(-110px, 0);
		opacity: 0;
	}
}

@keyframes hero-send-3 {
	0% {
		transform: translate(-50%, 0);
		opacity: 0;
	}

	20% {
		opacity: 1;
	}

	70% {
		transform: translate(-10px, -60px);
		opacity: 1;
	}

	100% {
		transform: translate(0, -80px);
		opacity: 0;
	}
}

.lp-stats-card {
	width: 100%;
	max-width: 360px;
	background: rgba(255, 255, 255, 0.92);
	border-radius: 20px;
	padding: 18px 18px 16px;
	box-shadow: 0 20px 35px rgba(15, 23, 42, 0.18);
	border: 1px solid rgba(148, 163, 184, 0.2);
}

.lp-stats-header {
	display: flex;
	align-items: center;
	justify-content: space-between;
	margin-bottom: 12px;
}

.lp-stats-header h3 {
	font-size: 14px;
	font-weight: 600;
	color: #0f172a;
}

.lp-stats-tag {
	font-size: 11px;
	padding: 2px 8px;
	border-radius: 999px;
	background: rgba(15, 23, 42, 0.04);
	color: #6b7280;
}

.lp-stats-grid {
	display: grid;
	grid-template-columns: repeat(2, minmax(0, 1fr));
	gap: 10px;
}

.lp-stat-item {
	padding: 8px 10px;
	border-radius: 12px;
	background: #f9fafb;
}

.lp-stat-label {
	font-size: 11px;
	color: #6b7280;
}

.lp-stat-value {
	margin-top: 4px;
	font-size: 18px;
	font-weight: 700;
	color: #111827;
}

.lp-mini-title {
	margin-top: 12px;
	font-size: 11px;
	color: #6b7280;
}

.lp-trend-placeholder {
	margin-top: 6px;
	height: 48px;
	border-radius: 10px;
	background: radial-gradient(
			circle at 10% 20%,
			rgba(148, 163, 184, 0.15),
			transparent 55%
		),
		radial-gradient(
			circle at 90% 80%,
			rgba(148, 163, 184, 0.15),
			transparent 55%
		);
	position: relative;
	overflow: hidden;
}

.lp-trend-line {
	position: absolute;
	inset: 0;
	background-image: linear-gradient(
		120deg,
		transparent 0%,
		rgba(15, 23, 42, 0.08) 20%,
		rgba(15, 23, 42, 0.1) 50%,
		transparent 80%
	);
	opacity: 0.7;
}

.lp-task-tags {
	display: flex;
	flex-wrap: wrap;
	gap: 6px;
	margin-top: 8px;
}

.lp-task-tag {
	font-size: 11px;
	padding: 4px 8px;
	border-radius: 999px;
}

.lp-task-running {
	background: #eff6ff;
	color: #1d4ed8;
}

.lp-task-done {
	background: #ecfdf5;
	color: #047857;
}

.lp-task-failed {
	background: #fef2f2;
	color: #b91c1c;
}

.lp-section {
	padding: 8px 4px;
}

.lp-section-alt {
	background: rgba(255, 255, 255, 0.75);
	border-radius: 20px;
	padding: 24px 20px;
}

.lp-section-messages {
	background: radial-gradient(
			circle at 0% 0%,
			rgba(148, 163, 184, 0.25),
			transparent 55%
		),
		radial-gradient(
			circle at 100% 0%,
			rgba(56, 189, 248, 0.22),
			transparent 55%
		),
		rgba(15, 23, 42, 0.86);
	border-radius: 22px;
	padding: 24px 22px 20px;
	border: 1px solid rgba(148, 163, 184, 0.55);
	box-shadow: 0 28px 70px rgba(15, 23, 42, 0.9);
	backdrop-filter: blur(24px);
	-webkit-backdrop-filter: blur(24px);
}

.lp-section-header {
	text-align: left;
	margin-bottom: 20px;
}

.lp-section-header h2 {
	font-size: 20px;
	font-weight: 700;
	color: #f9fafb;
}

.lp-section-header p {
	margin-top: 6px;
	font-size: 13px;
	color: #9ca3af;
	max-width: 620px;
}

.lp-ability-grid {
	display: grid;
	grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
	gap: 16px;
}

.lp-ability-card {
	position: relative;
	overflow: hidden;
	padding: 16px 16px 14px;
	border-radius: 18px;
	background: radial-gradient(
			circle at 0% 0%,
			rgba(148, 163, 184, 0.18),
			transparent 55%
		),
		radial-gradient(
			circle at 100% 100%,
			rgba(37, 99, 235, 0.2),
			transparent 55%
		),
		rgba(15, 23, 42, 0.96);
	border: 1px solid rgba(148, 163, 184, 0.55);
	box-shadow: 0 18px 35px rgba(15, 23, 42, 0.55),
		0 0 0 1px rgba(15, 23, 42, 0.65);
	backdrop-filter: blur(18px);
	-webkit-backdrop-filter: blur(18px);
	transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease;
}

.lp-ability-card::after {
	content: '';
	position: absolute;
	inset: 0;
	background: linear-gradient(
		135deg,
		rgba(255, 255, 255, 0.1),
		transparent 40%,
		transparent 60%,
		rgba(56, 189, 248, 0.22)
	);
	opacity: 0;
	transition: opacity 0.2s ease;
	pointer-events: none;
}

.lp-ability-card:hover {
	transform: translateY(-4px);
	box-shadow: 0 24px 45px rgba(15, 23, 42, 0.7),
		0 0 0 1px rgba(56, 189, 248, 0.6);
	border-color: rgba(129, 140, 248, 0.8);
}

.lp-ability-card:hover::after {
	opacity: 1;
}

.lp-ability-icon {
	width: 34px;
	height: 34px;
	border-radius: 999px;
	background: radial-gradient(circle at 0% 0%, #22d3ee, #6366f1);
	display: inline-flex;
	align-items: center;
	justify-content: center;
	margin-bottom: 6px;
	color: #0f172a;
	font-size: 18px;
	box-shadow: 0 0 0 2px rgba(15, 23, 42, 0.9), 0 10px 25px rgba(15, 23, 42, 0.8);
}

.lp-ability-title {
	font-size: 14px;
	font-weight: 600;
	color: #e5e7eb;
	margin-bottom: 4px;
}

.lp-ability-desc {
	font-size: 12px;
	color: #9ca3af;
}

.lp-collect-grid {
	display: grid;
	grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
	gap: 16px;
}

.lp-collect-card {
	position: relative;
	padding: 14px 14px 12px;
	border-radius: 16px;
	background: radial-gradient(
			circle at 0% 0%,
			rgba(148, 163, 184, 0.18),
			transparent 55%
		),
		radial-gradient(
			circle at 100% 100%,
			rgba(79, 70, 229, 0.18),
			transparent 55%
		),
		rgba(15, 23, 42, 0.96);
	border: 1px solid rgba(148, 163, 184, 0.55);
	box-shadow: 0 16px 35px rgba(15, 23, 42, 0.55),
		0 0 0 1px rgba(15, 23, 42, 0.7);
	overflow: hidden;
	transition: transform 0.18s ease, box-shadow 0.18s ease,
		border-color 0.18s ease;
}

.lp-collect-card::before {
	content: '';
	position: absolute;
	inset: 0;
	background: radial-gradient(
		circle at 0% 0%,
		rgba(56, 189, 248, 0.15),
		transparent 55%
	);
	opacity: 0;
	transition: opacity 0.18s ease;
	pointer-events: none;
}

.lp-collect-card:hover {
	transform: translateY(-3px);
	border-color: rgba(56, 189, 248, 0.75);
	box-shadow: 0 22px 40px rgba(15, 23, 42, 0.7),
		0 0 0 1px rgba(56, 189, 248, 0.6);
}

.lp-collect-card:hover::before {
	opacity: 1;
}

.lp-collect-card h3 {
	font-size: 14px;
	font-weight: 600;
	color: #e5e7eb;
	margin-bottom: 4px;
}

.lp-collect-card p {
	font-size: 12px;
	color: #cbd5f5;
}

.lp-collect-card-header {
	display: flex;
	align-items: center;
	gap: 10px;
	margin-bottom: 6px;
}

.lp-collect-index {
	width: 26px;
	height: 26px;
	border-radius: 999px;
	background: radial-gradient(
		circle at 0% 0%,
		var(--lp-primary),
		var(--lp-secondary)
	);
	display: inline-flex;
	align-items: center;
	justify-content: center;
	color: #0f172a;
	font-size: 13px;
	font-weight: 600;
	box-shadow: 0 0 0 2px rgba(248, 250, 252, 0.95);
}

.lp-collect-title-wrap h3 {
	margin-bottom: 0;
}

.lp-collect-tag {
	font-size: 11px;
	color: #9ca3af;
	margin-top: 2px;
}

.lp-collect-desc {
	margin-top: 4px;
}

.lp-collect-note {
	margin-top: 12px;
	font-size: 12px;
	color: #9ca3af;
}

.lp-pricing-toggle {
	display: inline-flex;
	align-items: center;
	gap: 8px;
	margin-bottom: 16px;
}

.lp-toggle-label {
	font-size: 12px;
	color: #9ca3af;
}

.lp-toggle-label.active {
	color: #111827;
	font-weight: 500;
}

.lp-toggle-switch {
	width: 44px;
	height: 22px;
	border-radius: 999px;
	border: none;
	background: rgba(148, 163, 184, 0.4);
	padding: 2px;
	cursor: pointer;
	position: relative;
}

.lp-toggle-thumb {
	position: absolute;
	top: 3px;
	left: 3px;
	width: 16px;
	height: 16px;
	border-radius: 999px;
	background: #fff;
	box-shadow: 0 1px 2px rgba(15, 23, 42, 0.25);
	transition: transform 0.18s ease;
}

.lp-toggle-thumb.yearly {
	transform: translateX(20px);
}

.lp-pricing-grid {
	display: grid;
	grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
	gap: 16px;
}

.lp-pricing-card {
	border-radius: 18px;
	padding: 16px 14px 14px;
	background: radial-gradient(
			circle at 0% 0%,
			rgba(148, 163, 184, 0.3),
			transparent 60%
		),
		radial-gradient(
			circle at 100% 100%,
			rgba(56, 189, 248, 0.3),
			transparent 60%
		),
		rgba(15, 23, 42, 0.9);
	border: 1px solid rgba(148, 163, 184, 0.5);
	box-shadow: 0 18px 40px rgba(15, 23, 42, 0.9);
	backdrop-filter: blur(24px);
	-webkit-backdrop-filter: blur(24px);
	transform: translateY(0) scale(1);
	transition: transform 0.28s ease, box-shadow 0.28s ease,
		border-color 0.28s ease, background 0.28s ease;
	display: flex;
	flex-direction: column;
}

.lp-pricing-card.highlight {
	border-color: var(--lp-primary);
	box-shadow: 0 24px 50px rgba(15, 23, 42, 1), 0 0 0 1px rgba(15, 23, 42, 0.9);
	transform: translateY(-4px) scale(1.02);
	background: radial-gradient(
			circle at 0% 0%,
			color-mix(in srgb, var(--lp-primary) 40%, transparent),
			transparent 60%
		),
		radial-gradient(
			circle at 100% 100%,
			color-mix(in srgb, var(--lp-secondary) 40%, transparent),
			transparent 60%
		),
		rgba(15, 23, 42, 0.98);
}

.lp-pricing-card:hover {
	transform: translateY(-4px) scale(1.02);
	box-shadow: 0 22px 48px rgba(15, 23, 42, 1);
	border-color: var(--lp-primary);
}

.lp-plan-name {
	font-size: 13px;
	font-weight: 600;
	color: #e5e7eb;
}

.lp-plan-price {
	margin-top: 6px;
	margin-bottom: 6px;
}

.lp-plan-price .amount {
	font-size: 22px;
	font-weight: 700;
	color: #f9fafb;
}

.lp-plan-price .cycle {
	font-size: 11px;
	color: #9ca3af;
	margin-left: 2px;
}

.lp-plan-desc {
	font-size: 12px;
	color: #9ca3af;
	min-height: 32px;
}

.lp-plan-meta {
	list-style: none;
	margin: 8px 0 10px;
	padding: 0;
	font-size: 12px;
	color: #9ca3af;
}

.lp-plan-meta li + li {
	margin-top: 4px;
}

.lp-plan-meta strong {
	font-weight: 500;
}

.lp-plan-btn {
	margin-top: auto;
	background: linear-gradient(135deg, var(--lp-primary), var(--lp-secondary));
	border: none;
	color: #fff;
	box-shadow: 0 14px 30px rgba(15, 23, 42, 0.9);
}

.lp-onetime-card {
	margin-top: 16px;
	padding: 14px 14px 12px;
	border-radius: 16px;
	background: rgba(15, 23, 42, 0.95);
	color: #e5e7eb;
	display: flex;
	align-items: center;
	justify-content: space-between;
	gap: 14px;
	flex-wrap: wrap;
}

.lp-onetime-title {
	font-size: 13px;
	font-weight: 600;
}

.lp-onetime-desc {
	font-size: 12px;
	color: #9ca3af;
	flex: 1;
}

.lp-onetime-price {
	font-size: 16px;
	font-weight: 700;
	white-space: nowrap;
}

.lp-onetime-price span {
	font-size: 11px;
	margin-left: 4px;
}

.lp-testimonial-grid {
	display: grid;
	grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
	gap: 16px;
}

.lp-testimonial-card {
	border-radius: 16px;
	padding: 16px 14px 14px;
	background: rgba(255, 255, 255, 0.9);
	border: 1px solid rgba(226, 232, 240, 0.9);
}

.lp-testimonial-quote {
	font-size: 12px;
	color: #4b5563;
	margin-bottom: 10px;
}

.lp-testimonial-author {
	display: flex;
	align-items: center;
	gap: 10px;
}

.lp-avatar {
	width: 26px;
	height: 26px;
	border-radius: 999px;
	background: rgba(15, 23, 42, 0.08);
	display: inline-flex;
	align-items: center;
	justify-content: center;
	font-size: 13px;
}

.lp-author-name {
	font-size: 13px;
	font-weight: 600;
	color: #111827;
}

.lp-author-title {
	font-size: 11px;
	color: #6b7280;
}

.lp-cta-section {
	padding-bottom: 0;
}

.lp-cta-box {
	border-radius: 20px;
	padding: 20px 18px 18px;
	background: linear-gradient(
		135deg,
		rgba(15, 23, 42, 0.98),
		rgba(15, 23, 42, 0.95),
		rgba(31, 41, 55, 0.96)
	);
	color: #e5e7eb;
	display: flex;
	flex-direction: column;
	gap: 10px;
}

.lp-cta-header h2 {
	font-size: 20px;
	font-weight: 700;
}

.lp-cta-header p {
	font-size: 13px;
	color: #9ca3af;
	max-width: 580px;
}

.lp-cta-actions {
	margin-top: 4px;
	display: flex;
	flex-wrap: wrap;
	gap: 10px;
}

.lp-cta-main-btn {
	min-width: 180px;
}

.lp-cta-highlight-list {
	display: flex;
	flex-wrap: wrap;
	gap: 8px 14px;
	margin-top: 4px;
}

.lp-cta-highlight {
	display: inline-flex;
	align-items: center;
	gap: 6px;
	font-size: 12px;
	color: #cbd5f5;
}

.lp-cta-highlight .dot {
	width: 6px;
	height: 6px;
	border-radius: 999px;
	background: linear-gradient(135deg, var(--lp-primary), var(--lp-secondary));
	box-shadow: 0 0 0 2px rgba(15, 23, 42, 0.9);
}

.lp-footer {
	display: flex;
	align-items: center;
	justify-content: space-between;
	font-size: 12px;
	color: #9ca3af;
	margin-top: 4px;
}

.lp-footer-right {
	display: flex;
	gap: 12px;
}

.lp-footer-link {
	color: #9ca3af;
	text-decoration: none;
}

.lp-footer-link:hover {
	text-decoration: underline;
}

@media (max-width: 900px) {
	.landing-page {
		padding: 80px 16px 16px;
	}

	.lp-header-inner {
		flex-direction: column;
		align-items: stretch;
		gap: 12px;
	}

	.lp-header-left {
		align-items: flex-start;
	}

	.lp-nav {
		order: 3;
		justify-content: flex-start;
		flex-wrap: wrap;
	}

	.lp-actions {
		width: 100%;
		justify-content: flex-start;
	}

	.lp-hero {
		grid-template-columns: minmax(0, 1fr);
	}

	.lp-hero-right {
		justify-content: flex-start;
		margin-top: 12px;
	}

	.lp-stats-card {
		max-width: 100%;
	}

	.lp-footer {
		flex-direction: column;
		align-items: flex-start;
		gap: 4px;
	}
}
</style>
