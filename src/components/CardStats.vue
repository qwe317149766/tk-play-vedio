<template>
  <div class="section" v-if="stats">
    <div class="section-title">💳 卡密统计</div>
    <div class="card-stats">
      <div class="card-stat-item">
        <div class="card-stat-label">已完成订单</div>
        <div class="card-stat-value" style="color: #667eea;">
          {{ stats.complate_job_num || 0 }}
        </div>
      </div>
      <div class="card-stat-item">
        <div class="card-stat-label">已使用额度</div>
        <div class="card-stat-value" style="color: #f59e0b;">
          {{ stats.num - stats.splus_num }}
        </div>
      </div>
      <div class="card-stat-item">
        <div class="card-stat-label">剩余额度</div>
        <div class="card-stat-value" style="color: #10b981;">
          {{ stats.splus_num }}
        </div>
      </div>
    </div>
    <div style="margin-top: 10px; text-align: center;">
      <a-button type="primary" @click="$emit('refresh')">
        🔄 刷新统计
      </a-button>
    </div>
  </div>
</template>

<script setup>
defineProps({
  stats: Object
})

defineEmits(['refresh'])

function formatQuota (value) {
  const num = Number(value) || 0
  return num.toFixed(2)
}
</script>
