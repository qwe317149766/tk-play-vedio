<template>
  <div class="landing-page">
    <!-- 这里先复用一份结构，后续可以按 Instagram 单独调整文案和布局 -->
    <section class="lp-hero">
      <div class="lp-hero-left">
        <h1 class="lp-hero-title">
          {{ texts.hero.title }}
        </h1>
        <p class="lp-hero-subtitle">
          {{ texts.hero.subtitle }}
        </p>
        <div class="lp-cta-row">
          <a-button
            type="primary"
            size="large"
            class="lp-primary-btn"
            @click="$emit('enter')"
          >
            {{ texts.actions.enterSystem }}
          </a-button>
        </div>
      </div>
      <!-- 右侧可以后续按 Instagram 需求补充独立内容 -->
    </section>
  </div>
</template>

<script setup>
defineProps({
  texts: { type: Object, required: true }
})
</script>


