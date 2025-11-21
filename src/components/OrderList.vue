<template>
  <div class="order-list">
    <a-spin :spinning="loading">
      <div v-if="orders.length === 0 && !loading" class="order-empty">
        <p>暂无订单</p>
      </div>
      <div v-for="order in orders" :key="order.id" class="order-item">
        <div class="order-item-header">
          <div>
            <div class="order-item-title">
              任务 #{{ order.id }} · {{ getServiceName(order) }}
            </div>
            <!-- <div class="order-meta">
              下单时间：{{ order.created_at || 'N/A' }}
            </div> -->
          </div>
          <!-- 付款信息 -->
          <div>
            <span>付款：</span>
            <span>{{ order.pay_amount }} 积分</span>
          </div>
          <div class="order-actions">
            <span :class="['order-status', getStatusClass(order.status)]">
              {{ getStatusText(order.status) }}
            </span>
            <a-button type="link" size="small" @click="handleLoadChildren(order)">
              <template v-if="childLoading[getOrderKey(order)]">
                加载中...
              </template>
              <template v-else-if="hasChildData(order)">
                刷新子订单
              </template>
              <template v-else>
                查看子订单
              </template>
            </a-button>
          </div>
        </div>

        <div class="child-orders" v-if="shouldShowChild(order)">
          <a-spin :spinning="childLoading[getOrderKey(order)]">
            <template v-if="childLoading[getOrderKey(order)]">
              <div class="child-placeholder">正在加载子订单...</div>
            </template>
            <template v-else>
              <div v-if="childOrders[getOrderKey(order)]?.length">
                <div v-for="child in childOrders[getOrderKey(order)]" :key="child.id || child.order_id"
                  class="child-order-item">
                  <div class="child-order-header">
                    <div class="child-title">
                      子订单 #{{ child.order_info }}
                    </div>
                    <span :class="['order-status', getStatusClass(child.status)]">
                      {{ getStatusText(child.status) }}
                    </span>
                  </div>
                  <div class="child-meta">
                    <span style="margin-right: 10px;">数量：{{ child.order_num || 0 }}</span>
                    <span>完成：{{ child.complete_num || 0 }}</span>
                  </div>
                  <div class="progress-bar small">
                    <div class="progress-bar-fill" :style="{ width: `${getProgress(child)}%` }"></div>
                  </div>
                </div>
              </div>
              <div v-else class="child-placeholder">
                暂无子订单
              </div>
            </template>
          </a-spin>
        </div>
      </div>
    </a-spin>
  </div>
</template>

<script setup>
import { SERVICE_TYPES, ORDER_STATUS } from '../constants'

const props = defineProps({
  orders: {
    type: Array,
    default: () => []
  },
  loading: {
    type: Boolean,
    default: false
  },
  childOrders: {
    type: Object,
    default: () => ({})
  },
  childLoading: {
    type: Object,
    default: () => ({})
  }
})

const emit = defineEmits(['load-children'])

function getOrderKey (order) {
  return order?.order_id || order?.id
}

function handleLoadChildren (order) {
  emit('load-children', order)
}

function hasChildData (order) {
  const key = getOrderKey(order)
  return Array.isArray(props.childOrders[key])
}

function shouldShowChild (order) {
  const key = getOrderKey(order)
  return props.childLoading[key] || hasChildData(order)
}

function getServiceName (order) {
  const key = order?.service_type || order?.order_type || order?.product_type || ''
  return SERVICE_TYPES[key]?.name || order?.product_name || key || '未知服务'
}

function getStatusClass (status) {
  return ORDER_STATUS[status]?.class || 'pending'
}

function getStatusText (status) {
  return ORDER_STATUS[status]?.text || '未知'
}

function getProgress (order) {
  const total = Number(order.order_num || order.total_num || order.job_num || 0)
  if (!total) return 0
  const done = Number(order.complete_num || order.finish_num || order.success_num || 0)
  return Math.min(100, (done / total) * 100)
}
</script>

<style scoped>
.order-empty {
  text-align: center;
  padding: 40px;
  color: #999;
}

.order-item {
  background: rgba(255, 255, 255, 0.9);
  border-radius: 12px;
  padding: 20px;
  margin-bottom: 16px;
  box-shadow: 0 10px 20px rgba(15, 23, 42, 0.08);
}

.order-item-header {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
}

.order-item-title {
  font-weight: 600;
  font-size: 16px;
  color: #111827;
}

.order-meta {
  font-size: 12px;
  color: #6b7280;
  margin-top: 4px;
}

.order-actions {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 4px;
}

.order-status {
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
}

.progress-bar {
  height: 6px;
  background: #e5e7eb;
  border-radius: 999px;
  overflow: hidden;
  margin: 12px 0 6px;
}

.progress-bar.small {
  height: 4px;
  margin-top: 8px;
}

.progress-bar-fill {
  height: 100%;
  background: linear-gradient(90deg, #6366f1, #8b5cf6);
  border-radius: inherit;
  transition: width 0.3s ease;
}

.progress-text {
  font-size: 12px;
  text-align: right;
  color: #6b7280;
}

.child-orders {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px dashed #e5e7eb;
}

.child-order-item {
  background: #f9fafb;
  border-radius: 10px;
  padding: 12px;
  margin-bottom: 10px;
}

.child-order-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 6px;
}

.child-title {
  font-weight: 600;
  color: #1f2937;
}

.child-meta {
  font-size: 12px;
  color: #6b7280;
  display: flex;
  align-items: center;
}

.child-placeholder {
  text-align: center;
  padding: 12px;
  color: #9ca3af;
  font-size: 13px;
}
</style>
