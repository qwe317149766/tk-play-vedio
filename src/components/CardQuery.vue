<template>
  <div>
    <!-- 购买卡密联系方式 -->
    <div class="contact-section">
      <div class="contact-card">
        <div class="contact-header">
          <span class="contact-icon">💳</span>
          <span class="contact-title">购买卡密</span>
        </div>
        <div class="contact-content">
          <div class="contact-item" v-if="contactInfo.telegram">
            <span class="contact-label">Telegram：</span>
            <span class="contact-value">{{ contactInfo.telegram.name }}</span>
            <a-button type="link" style="color: white;" size="small"
              @click="copyToClipboard(contactInfo.telegram.url, 'telegram')">
              📋 复制
            </a-button>
            <a-button type="link" style="color: white;" size="small"
              @click="openTelegram(contactInfo.telegram.url)">前往</a-button>
          </div>
        </div>
      </div>
    </div>

    <div class="section">
      <div class="input-group">
        <label>查询卡密</label>
        <a-input v-model:value="queryCardKey" placeholder="输入要查询的卡密" @keypress.enter="handleQuery" />
        <div class="hint">查询后可查看该卡密剩余积分及关联订单</div>
      </div>
      <div style="display: flex; gap: 10px; margin-bottom: 15px;">
        <a-button type="primary" @click="handleQuery" style="flex: 1;">
          查询卡密信息
        </a-button>
        <a-button @click="handleReset" style="flex: 1;">清空</a-button>
      </div>
      <div v-if="queryResult" class="card-query-result">
        <div class="card-query-stats" v-if="queryStats">
          <div class="card-query-stat">
            <div class="label">剩余积分</div>
            <div class="value">{{ queryStats.splus_num }}</div>
          </div>
          <div class="card-query-stat">
            <div class="label">已使用积分</div>
            <div class="value">{{ queryStats.num - queryStats.splus_num }}</div>
          </div>
          <div class="card-query-stat">
            <div class="label">已完成订单</div>
            <div class="value">{{ queryStats.complate_job_num }}</div>
          </div>
        </div>
        <div class="card-query-orders" v-if="queryOrders">
          <div class="card-query-orders-title">关联订单</div>
          <div class="card-query-orders-list" style="max-height: 400px; overflow-y: auto;">
            <div v-if="queryOrders.length === 0" class="card-query-placeholder">
              该卡密尚未产生订单
            </div>
            <div v-else>
              <div v-for="order in queryOrders.slice(0, 10)" :key="order.id" class="card-order-row">
                <div>
                  <div class="card-order-title">
                    订单 #{{ order.id }} · {{ getServiceName(order.service_type) }}
                  </div>
                  <div class="card-order-meta">
                    数量：{{ order.order_num || 0 }} ｜ 完成：{{ order.complete_num || 0 }} ｜
                    下单时间：{{ order.created_at || 'N/A' }}
                  </div>
                </div>
                <span :class="['order-status', getStatusClass(order.status)]">
                  {{ getStatusText(order.status) }}
                </span>
              </div>
              <p v-if="queryOrders.length > 10" class="card-query-placeholder">
                仅显示最近 10 条，共 {{ queryOrders.length }} 条订单
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { message } from 'ant-design-vue'
import { SERVICE_TYPES, ORDER_STATUS } from '../constants'
import { contactConfig } from '../config/contact'

defineProps({
  visible: Boolean
})

// 联系方式信息
const contactInfo = ref(contactConfig)

// 打开telegram
function openTelegram (url) {
  window.open(url, '_blank')
}

// 复制到剪贴板
function copyToClipboard (text, type) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(() => {
      message.success(`${type}已复制到剪贴板`)
    }).catch(() => {
      fallbackCopy(text, type)
    })
  } else {
    fallbackCopy(text, type)
  }
}

// 备用复制方法
function fallbackCopy (text, type) {
  const textArea = document.createElement('textarea')
  textArea.value = text
  textArea.style.position = 'fixed'
  textArea.style.left = '-999999px'
  document.body.appendChild(textArea)
  textArea.focus()
  textArea.select()
  try {
    document.execCommand('copy')
    message.success(`${type}已复制到剪贴板`)
  } catch (err) {
    message.error('复制失败，请手动复制')
  }
  document.body.removeChild(textArea)
}

const emit = defineEmits(['query', 'reset'])

const queryCardKey = ref('')
const queryResult = ref(false)
const queryStats = ref(null)
const queryOrders = ref(null)

function formatQuota (value) {
  const num = Number(value) || 0
  return num.toFixed(2)
}

function getServiceName (serviceType) {
  return SERVICE_TYPES[serviceType]?.name || serviceType
}

function getStatusClass (status) {
  return ORDER_STATUS[status]?.class || 'pending'
}

function getStatusText (status) {
  return ORDER_STATUS[status]?.text || '未知'
}

function handleQuery () {
  if (!queryCardKey.value.trim()) {
    return
  }
  emit('query', queryCardKey.value.trim())
}

function handleReset () {
  queryCardKey.value = ''
  queryResult.value = false
  queryStats.value = null
  queryOrders.value = null
  emit('reset')
}

function setQueryResult (stats, orders) {
  queryResult.value = true
  queryStats.value = stats
  queryOrders.value = orders
}

defineExpose({
  setQueryResult,
  queryCardKey
})
</script>

<style scoped>
.section {
  margin-bottom: 20px;
}

.section-title {
  font-size: 18px;
  font-weight: 600;
  margin-bottom: 15px;
  color: #333;
  display: flex;
  align-items: center;
}

.section-title::before {
  content: '';
  width: 4px;
  height: 20px;
  background: #667eea;
  margin-right: 10px;
  border-radius: 2px;
}

.input-group {
  margin-bottom: 20px;
}

.input-group label {
  display: block;
  margin-bottom: 8px;
  font-weight: 500;
  color: #555;
}

.hint {
  font-size: 12px;
  color: #999;
  margin-top: 5px;
}

.card-query-result {
  background: #f8f9fa;
  border-radius: 12px;
  padding: 15px;
  margin-top: 10px;
}

.card-query-orders-title {
  font-weight: 600;
  color: #333;
  margin-bottom: 10px;
}

.card-query-orders-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.card-query-placeholder {
  font-size: 12px;
  color: #999;
  text-align: center;
}

.contact-section {
  margin-bottom: 20px;
}

.contact-card {
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  border-radius: 12px;
  padding: 20px;
  color: white;
}

.contact-header {
  display: flex;
  align-items: center;
  margin-bottom: 15px;
  font-size: 18px;
  font-weight: 600;
}

.contact-icon {
  font-size: 24px;
  margin-right: 10px;
}

.contact-title {
  color: white;
}

.contact-content {
  background: rgba(255, 255, 255, 0.15);
  border-radius: 8px;
  padding: 15px;
  backdrop-filter: blur(10px);
}

.contact-item {
  display: flex;
  align-items: center;
  margin-bottom: 12px;
  font-size: 14px;
}

.contact-item:last-child {
  margin-bottom: 0;
}

.contact-label {
  font-weight: 500;
  min-width: 60px;
  color: rgba(255, 255, 255, 0.9);
}

.contact-value {
  flex: 1;
  color: white;
  font-weight: 600;
  margin-right: 10px;
  word-break: break-all;
}
</style>
