<template>
	<div class="landing-page">
		<!-- 主视觉 -->
		<section class="lp-hero lp-hero-banner">
			<div class="lp-hero-left">
				<h1 class="lp-hero-title">
					{{ texts.hero.title }}
				</h1>
				<p class="lp-hero-subtitle">
					{{ texts.hero.subtitle }}
				</p>

				<!-- 关键卖点 -->
				<div class="lp-badges">
					<div class="lp-badge">
						<span class="lp-badge-label">
							{{ texts.hero.badges.pipeline }}
						</span>
					</div>
					<div class="lp-badge">
						<span class="lp-badge-label">
							{{ texts.hero.badges.dashboard }}
						</span>
					</div>
				</div>

				<!-- <div class="lp-cta-row">
					<a-button
						type="primary"
						size="large"
						class="lp-primary-btn"
						@click="$emit('enter')">
						{{ texts.actions.enterSystem }}
					</a-button>
				</div> -->
			</div>

			<!-- 右侧炫酷动效：中心收件箱 + 环绕的多类型消息图标 -->
			<div class="lp-hero-right">
				<div class="hero-visual">
					<div class="hero-orbit-scene">
						<button
							class="hero-node hero-node-main"
							style="background-color: transparent; border: none"
							@click="$emit('enter')">
							<a-badge :count="inboxCount" :overflow-count="99999">
								<span class="glyph">📨</span>
							</a-badge>
						</button>
						<div class="hero-orbit-ring">
							<div class="hero-node hero-node-text">
								<span class="glyph">💬</span>
							</div>
							<div class="hero-node hero-node-live">
								<span class="glyph">📺</span>
							</div>
							<div class="hero-node hero-node-post">
								<span class="glyph">🎬</span>
							</div>
							<div class="hero-node hero-node-profile">
								<span class="glyph">👤</span>
							</div>
							<div class="hero-node hero-node-card">
								<span class="glyph">🧾</span>
							</div>
						</div>

						<!-- 发送动效小光点 -->
						<div class="hero-packet packet-1"></div>
						<div class="hero-packet packet-2"></div>
						<div class="hero-packet packet-3"></div>
					</div>
				</div>
			</div>
		</section>

		<!-- 能力与优势 -->
		<section class="lp-section">
			<div class="lp-section-header">
				<h2>{{ texts.abilities.title }}</h2>
				<p>{{ texts.abilities.subtitle }}</p>
			</div>

			<div class="lp-ability-grid">
				<div
					class="lp-ability-card"
					v-for="item in texts.abilities.items"
					:key="item.key">
					<div class="lp-ability-icon">
						{{ item.icon }}
					</div>
					<h3 class="lp-ability-title">{{ item.title }}</h3>
					<p class="lp-ability-desc">{{ item.desc }}</p>
				</div>
			</div>
		</section>

		<!-- 消息类型多样性 -->
		<section class="lp-section lp-section-alt lp-section-messages">
			<div class="lp-section-header">
				<h2>{{ texts.collect.title }}</h2>
				<p>{{ texts.collect.subtitle }}</p>
			</div>

			<div class="lp-collect-grid">
				<div
					class="lp-collect-card"
					v-for="(item, index) in texts.collect.items"
					:key="item.key">
					<div class="lp-collect-card-header">
						<div class="lp-collect-index">
							<span>{{ index + 1 }}</span>
						</div>
						<div class="lp-collect-title-wrap">
							<h3>{{ item.title }}</h3>
							<p class="lp-collect-tag">Message type {{ index + 1 }}</p>
						</div>
					</div>
					<p class="lp-collect-desc">{{ item.desc }}</p>
				</div>
			</div>

			<p v-if="texts.collect.extra" class="lp-collect-note">
				{{ texts.collect.extra }}
			</p>
		</section>

		<!-- 价格套餐 -->
		<section class="lp-section" id="pricing">
			<div class="lp-section-header">
				<h2>{{ texts.pricing.title }}</h2>
				<p>{{ texts.pricing.subtitle }}</p>
			</div>

			<div class="lp-pricing-grid">
				<div
					v-for="plan in texts.pricing.plans"
					:key="plan.key"
					:class="['lp-pricing-card', { highlight: plan.highlight }]">
					<div class="lp-plan-name">{{ plan.name }}</div>
					<div class="lp-plan-price">
						<span class="amount">
							{{ plan.priceText || displayPrice(plan.price) }}
						</span>
						<span v-if="plan.cycle" class="cycle"> / {{ plan.cycle }} </span>
					</div>
					<p class="lp-plan-desc">{{ plan.desc }}</p>
					<ul class="lp-plan-meta">
						<li v-for="benefit in texts.pricing.benefits" :key="benefit">
							{{ benefit }}
						</li>
						<li v-for="extra in plan.extraBenefits || []" :key="extra">
							{{ extra }}
						</li>
					</ul>
					<a-button
						type="primary"
						block
						class="lp-plan-btn"
						@click="openTelegram">
						{{ texts.actions.subscribe }}
					</a-button>
				</div>
			</div>

			<!-- <div class="lp-onetime-card">
				<div class="lp-onetime-title">
					{{ texts.pricing.payAsYouGoTitle }}
				</div>
				<div class="lp-onetime-desc">
					{{ texts.pricing.payAsYouGoDesc }}
				</div>
				<div class="lp-onetime-price">
					$0.005 <span>/ {{ texts.pricing.perSuccess }}</span>
				</div>
			</div> -->
		</section>

		<!-- 底部 CTA 与页脚 -->
		<section class="lp-section lp-cta-section">
			<div class="lp-cta-box">
				<div class="lp-cta-header">
					<h2>{{ texts.finalCta.title }}</h2>
					<p>{{ texts.finalCta.subtitle }}</p>
				</div>

				<ul v-if="texts.finalCta.highlights" class="lp-cta-highlight-list">
					<li
						v-for="item in texts.finalCta.highlights"
						:key="item"
						class="lp-cta-highlight">
						<span class="dot"></span>
						<span class="text">{{ item }}</span>
					</li>
				</ul>

				<div class="lp-cta-actions">
					<a-button
						type="primary"
						size="large"
						class="lp-primary-btn lp-cta-main-btn"
						@click="openTelegram">
						{{ texts.actions.startNow }}
					</a-button>
					<!-- <a-button ghost size="large" class="lp-ghost-btn">
						{{ texts.actions.contact }}
					</a-button> -->
				</div>
			</div>
		</section>

		<footer class="lp-footer">
			<div class="lp-footer-left">
				<span>gd云控</span>
				<span>© 2025</span>
			</div>
			<div class="lp-footer-right">
				<a href="#" class="lp-footer-link">{{ texts.footer.terms }}</a>
				<a href="#" class="lp-footer-link">{{ texts.footer.privacy }}</a>
			</div>
		</footer>
	</div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'

const props = defineProps({
	texts: { type: Object, required: true },
	isYearly: { type: Boolean, default: false },
})

const inboxCount = ref(10240)
let inboxTimer

onMounted(() => {
	inboxTimer = setInterval(() => {
		inboxCount.value += 1
	}, 2000)
})

onUnmounted(() => {
	if (inboxTimer) clearInterval(inboxTimer)
})

function displayPrice(base) {
	return `$${base}`
}

function openTelegram() {
	window.open('https://t.me/igcreates', '_blank')
}
</script>
