<template>
  <div class="app-container">
    <!-- 背景装饰元素 -->
    <div class="bg-shape"></div>
    <div class="bg-pattern-1"></div>
    <div class="bg-pattern-2"></div>
    <div class="bg-pattern-3"></div>
    <!-- 主页面 -->
    <div>
      <div class="header">
        <h1>🎵 TikTok服务下单系统</h1>
        <p>专业、快速、安全的TikTok增值服务</p>
      </div>

      <div class="content">
        <!-- 服务选择 -->
        <div class="section">
          <div class="section-title">选择服务类型</div>
          <div class="service-grid">
            <ServiceCard v-for="service in renderedServices" :key="service.key" :service-key="service.key"
              :service="service" :price="service.price" :is-selected="selectedService === service.key"
              @select="selectService" />
          </div>
        </div>

        <!-- 播放服务配置 -->
        <PlayConfig v-if="selectedService === 'playVedio'" v-model="playConfig" />

        <!-- 点赞服务配置 -->
        <LikeConfig v-if="selectedService === 'likeVedio'" v-model="likeConfig" />

        <!-- 评论服务配置 -->
        <CommentConfig v-if="selectedService === 'commentVedio'" v-model="commentConfig" />

        <!-- 私信服务配置 -->
        <FollowConfig v-if="selectedService === 'followVedio'" v-model="followConfig" />

        <!-- 备注 -->
        <div class="section">
          <div class="input-group">
            <label>备注（选填）</label>
            <a-textarea v-model:value="remark" :rows="3" placeholder="输入订单备注信息" />
          </div>
        </div>

        <!-- 卡密统计 -->
        <CardStats v-if="cardStats" :stats="cardStats" @refresh="refreshCardStats" />

        <div style="margin: -30px 0 10px; text-align: center;">
          <a-button type="primary" @click="toggleCardQuery" style="background: #10b981; border-color: #10b981;">
            🔎 卡密查询
          </a-button>
        </div>

        <!-- 订单摘要 -->
        <OrderSummary :selected-service="selectedService" :service-prices="servicePrices" :play-config="playConfig"
          :like-config="likeConfig" :comment-config="commentConfig" :follow-config="followConfig" />

        <!-- 下单按钮 -->
        <a-button type="primary" size="large" block :disabled="!canSubmit" @click="showConfirmModal"
          style="margin-bottom: 10px;">
          立即下单
        </a-button>

        <a-button size="large" block @click="showProgressPage">
          查看订单进度
        </a-button>
      </div>
    </div>


    <!-- 确认下单模态框 -->
    <a-modal v-model:open="showConfirm" ok-text="确认下单" cancel-text="取消" title="确认下单信息" @ok="submitOrder"
      @cancel="showConfirm = false">
      <div class="order-info">
        <div class="order-info-row">
          <span>服务类型：</span>
          <strong>{{ confirmData.service }}</strong>
        </div>
        <div class="order-info-row">
          <span>数量：</span>
          <strong>{{ confirmData.quantity }}</strong>
        </div>
        <div class="order-info-row">
          <span>单价：</span>
          <strong>{{ confirmData.unitPrice }}</strong>
        </div>
        <div class="order-info-row" style="border-top: 2px solid #e0e0e0; padding-top: 10px; margin-top: 10px;">
          <span>总计：</span>
          <strong style="color: #667eea; font-size: 18px;">{{ confirmData.total }}</strong>
        </div>
      </div>
      <a-form-item label="卡密" style="margin-top: 20px;">
        <a-input v-model:value="cardKey" placeholder="请输入卡密" />
        <div class="hint">首次下单需要输入卡密，系统会自动记住</div>
      </a-form-item>
    </a-modal>

    <!-- 卡密查询模态框 -->
    <a-modal v-model:open="showCardQueryModal" title="🔐 卡密查询" width="800px" :footer="null">
      <div class="card-query-modal-content">
        <CardQuery ref="cardQueryRef" :visible="showCardQueryModal" @query="handleCardQuery"
          @reset="handleCardQueryReset" />
      </div>
    </a-modal>

    <!-- 订单进度模态框 -->
    <a-modal v-model:open="showProgressModal" title="📊 订单进度查询" width="800px" :footer="null"
      @open="handleProgressModalOpen">
      <div class="progress-modal-content">
        <!-- 订单号查询 -->
        <div class="section">
          <div class="input-group">
            <label>输入卡密</label>
            <div style="display: flex; gap: 10px;">
              <a-input v-model:value="searchOrderKey" placeholder="请输入卡密" @keypress.enter="searchOrderById"
                style="flex: 1;" />
              <a-button type="primary" @click="searchOrderById">
                🔍 查询
              </a-button>
            </div>
          </div>
        </div>
        <!-- 订单列表 -->
        <div class="section">
          <div class="section-title">📦 订单列表</div>
          <div style="max-height: 500px; overflow-y: auto;">
            <OrderList :orders="orders" :loading="ordersLoading" :child-orders="childOrdersMap"
              :child-loading="childLoadingMap" @load-children="handleLoadChildOrders" />
          </div>
        </div>
      </div>
    </a-modal>

    <!-- 下单成功模态框 -->
    <a-modal v-model:open="showSuccess" :footer="null" :closable="false">
      <div class="success-page">
        <div class="success-icon">✅</div>
        <h2>下单成功！</h2>
        <p>您的订单已提交，系统正在处理中</p>

        <div class="order-info">
          <div class="order-info-row">
            <span>订单编号：</span>
            <strong>{{ successData.orderId }}</strong>
          </div>
          <div class="order-info-row">
            <span>服务类型：</span>
            <strong>{{ successData.service }}</strong>
          </div>
          <div class="order-info-row">
            <span>下单数量：</span>
            <strong>{{ successData.quantity }}</strong>
          </div>
          <div class="order-info-row">
            <span>支付金额：</span>
            <strong>{{ successData.amount }}</strong>
          </div>
        </div>

        <div style="display: flex; gap: 10px; margin-top: 20px;">
          <a-button type="primary" block @click="handleViewOrder">
            查看订单进度
          </a-button>
          <a-button block @click="handleContinueOrder">
            继续下单
          </a-button>
        </div>
      </div>
    </a-modal>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { message } from 'ant-design-vue'
import ServiceCard from './components/ServiceCard.vue'
import PlayConfig from './components/PlayConfig.vue'
import LikeConfig from './components/LikeConfig.vue'
import CommentConfig from './components/CommentConfig.vue'
import FollowConfig from './components/FollowConfig.vue'
import OrderSummary from './components/OrderSummary.vue'
import CardStats from './components/CardStats.vue'
import CardQuery from './components/CardQuery.vue'
import OrderList from './components/OrderList.vue'
import { apiService } from './utils/api'
import { storage } from './utils/storage'
import { SERVICE_TYPES, SERVICE_KEY_MAP } from './constants'

const serviceList = ref([])
const servicePrices = ref({})
const selectedService = ref('')

const playConfig = ref({
  videoIds: [],
  orderQuantityPerVideo: 1
})

const likeConfig = ref({
  videoIds: [],
  likeCountPerVideo: 1000
})

const commentConfig = ref({
  videoIds: [],
  commentCountPerVideo: 1000,
  commentTemplates: [],
  addEmoji: true
})

const followConfig = ref({
  targetUsers: [],
  messageContent: '',
  addRandomEmoji: true,
  customEmojis: []
})

const initialOrderKey = storage.get('cardKey') || ''
const remark = ref('')
const cardKey = ref(initialOrderKey)
const cardStats = ref(null)
const cardQueryRef = ref(null)
const orders = ref([])
const ordersLoading = ref(false)
const searchOrderKey = ref(initialOrderKey)
const childOrdersMap = ref({})
const childLoadingMap = ref({})

// 模态框
const showConfirm = ref(false)
const showSuccess = ref(false)
const showProgressModal = ref(false)
const showCardQueryModal = ref(false)
const confirmData = ref({})
const successData = ref({})

// 计算属性
const canSubmit = computed(() => {
  if (!selectedService.value) return false

  const service = selectedService.value

  if (isServiceType(service, 'play')) {
    return playConfig.value.videoIds.length > 0 &&
      playConfig.value.orderQuantityPerVideo > 0
  } else if (isServiceType(service, 'like')) {
    return likeConfig.value.videoIds.length > 0 &&
      likeConfig.value.likeCountPerVideo > 0
  } else if (isServiceType(service, 'comment')) {
    return commentConfig.value.videoIds.length > 0 &&
      commentConfig.value.commentCountPerVideo > 0 &&
      commentConfig.value.commentTemplates.length > 0
  } else if (isServiceType(service, 'follow')) {
    return followConfig.value.targetUsers.length > 0 &&
      followConfig.value.messageContent.length > 0
  }

  return false
})

// 工具方法
function resolveServiceKey (value) {
  if (!value) return ''
  const lower = String(value).toLowerCase()
  for (const [canonical, variants] of Object.entries(SERVICE_KEY_MAP)) {
    if (variants.some(v => v.toLowerCase() === lower)) {
      return canonical
    }
  }
  return value
}

function isServiceType (serviceKey, type) {
  const canonical = resolveServiceKey(type)
  const variants = SERVICE_KEY_MAP[canonical] || []
  return variants.some(v => v === serviceKey)
}

async function fetchOrdersByCard (orderKey) {
  const parentRes = await apiService.getParentOrders(orderKey)
  if (parentRes?.code !== 200) {
    const err = new Error(parentRes?.msg || '获取订单列表失败')
    err.response = parentRes
    throw err
  }

  const parentList = Array.isArray(parentRes.data?.list) ? parentRes.data.list : []
  return parentList
}

// 方法
function selectService (serviceKey) {
  selectedService.value = serviceKey
}

function toggleCardQuery () {
  showCardQueryModal.value = true
}

const DEFAULT_SERVICE_KEYS = Object.values(SERVICE_KEY_MAP).map(list => list[0])

const renderedServices = computed(() => {
  return serviceList.value.map((item, index) => normalizeServiceItem(item, index))
})

watch(renderedServices, (list) => {
  const priceMap = {}
  list.forEach((item) => {
    priceMap[item.key] = item.price
  })
  servicePrices.value = priceMap

  if (!list.find(item => item.key === selectedService.value)) {
    selectedService.value = list[0]?.key || ''
  }
}, { immediate: true })

async function loadServiceList () {
  try {
    const result = await apiService.getServiceList()
    if (result.code === 200) {
      const list = Array.isArray(result.data)
        ? result.data
        : (Array.isArray(result.data?.list) ? result.data.list : [])
      serviceList.value = list
    }
  } catch (error) {
    message.error('网络错误，使用默认服务列表')
  }
}

function normalizeServiceItem (item, index) {
  const fallbackKey = DEFAULT_SERVICE_KEYS[index] || `service_${item.id || index}`
  const rawKey = (item.product_type && item.product_type.trim()) || fallbackKey
  const key = resolveServiceKey(rawKey)
  const base = SERVICE_TYPES[key] || SERVICE_TYPES[resolveServiceKey(fallbackKey)] || {}

  return {
    ...item,
    key,
    icon: item.img || '🛠️',
    name: item.product_name,
    unit: `${item.unit_num} 次`,
    price: Number(item.price) || 0
  }
}

function showConfirmModal () {
  if (!selectedService.value) return

  const service = selectedService.value
  const unitPrice = servicePrices.value[service] || 0
  let detailText = ''
  let totalTasks = 0
  let total = 0

  if (isServiceType(service, 'play')) {
    const videoCount = playConfig.value.videoIds.length
    const orderQuantityPerVideo = playConfig.value.orderQuantityPerVideo
    const playCountPerVideo = orderQuantityPerVideo * 1000
    totalTasks = videoCount * playCountPerVideo
    detailText = `${videoCount} 个视频 × ${orderQuantityPerVideo} 单（每单1000次播放） = ${totalTasks} 次播放`
    total = (totalTasks / 1000) * unitPrice
  } else if (isServiceType(service, 'like')) {
    const videoCount = likeConfig.value.videoIds.length
    const likeCountPerVideo = likeConfig.value.likeCountPerVideo
    totalTasks = videoCount * likeCountPerVideo
    detailText = `${videoCount} 个视频 × ${likeCountPerVideo} 个点赞 = ${totalTasks} 个点赞`
    total = (totalTasks / 1000) * unitPrice
  } else if (isServiceType(service, 'comment')) {
    const videoCount = commentConfig.value.videoIds.length
    const commentCountPerVideo = commentConfig.value.commentCountPerVideo
    totalTasks = videoCount * commentCountPerVideo
    detailText = `${videoCount} 个视频 × ${commentCountPerVideo} 条评论 = ${totalTasks} 条评论`
    total = (totalTasks / 1000) * unitPrice
  } else if (isServiceType(service, 'follow')) {
    const userCount = followConfig.value.targetUsers.length
    totalTasks = userCount
    detailText = `${userCount} 个用户`
    total = (totalTasks / 1000) * unitPrice
  }

  confirmData.value = {
    service: SERVICE_TYPES[service].name,
    quantity: detailText,
    unitPrice: `${unitPrice} 积分/${SERVICE_TYPES[service].unit}`,
    total: `${total.toFixed(2)} 积分`
  }

  showConfirm.value = true
}

async function submitOrder () {
  if (!cardKey.value.trim()) {
    message.error('请输入卡密')
    return
  }

  // 保存卡密
  storage.set('cardKey', cardKey.value)

  try {
    const service = selectedService.value
    let orderData = {
      service_type: service,
      remark: remark.value,
      card_key: cardKey.value
    }

    // 根据服务类型添加特定配置
    if (isServiceType(service, 'play')) {
      orderData.video_ids = playConfig.value.videoIds
      const orderQuantityPerVideo = playConfig.value.orderQuantityPerVideo
      orderData.play_count_per_video = orderQuantityPerVideo * 1000
      orderData.total_tasks = playConfig.value.videoIds.length * orderData.play_count_per_video
    } else if (isServiceType(service, 'like')) {
      orderData.video_ids = likeConfig.value.videoIds
      orderData.like_count_per_video = likeConfig.value.likeCountPerVideo
      orderData.total_tasks = likeConfig.value.videoIds.length * likeConfig.value.likeCountPerVideo
    } else if (isServiceType(service, 'comment')) {
      orderData.video_ids = commentConfig.value.videoIds
      orderData.comment_count_per_video = commentConfig.value.commentCountPerVideo
      orderData.comment_templates = commentConfig.value.commentTemplates
      orderData.add_emoji = commentConfig.value.addEmoji
      orderData.total_tasks = commentConfig.value.videoIds.length * commentConfig.value.commentCountPerVideo
    } else if (isServiceType(service, 'follow')) {
      orderData.target_users = followConfig.value.targetUsers
      orderData.message_content = followConfig.value.messageContent
      orderData.add_random_emoji = followConfig.value.addRandomEmoji
      orderData.custom_emojis = followConfig.value.customEmojis
      orderData.total_tasks = followConfig.value.targetUsers.length
    }

    const result = await apiService.createService(orderData)

    if (result.code === 0) {
      showConfirm.value = false
      showSuccessModal(result.data)
      // 刷新卡密统计
      await refreshCardStats()
    } else {
      message.error(`下单失败: ${result.msg}`)
    }
  } catch (error) {
    console.error('下单失败:', error)
    message.error('网络错误，请稍后重试')
  }
}

function showSuccessModal (orderData) {
  const service = selectedService.value
  const unitPrice = servicePrices.value[service] || 0
  let quantityText = ''
  let totalTasks = orderData.total_tasks || 0
  let total = 0

  if (isServiceType(service, 'play')) {
    const videoCount = playConfig.value.videoIds.length
    const orderQuantityPerVideo = playConfig.value.orderQuantityPerVideo
    quantityText = `${videoCount} 个视频 × ${orderQuantityPerVideo} 单（每单1000次播放） = ${totalTasks} 次播放`
    total = (totalTasks / 1000) * unitPrice
  } else if (isServiceType(service, 'like')) {
    quantityText = `${likeConfig.value.videoIds.length} 个视频，共 ${totalTasks} 个点赞`
    total = (totalTasks / 1000) * unitPrice
  } else if (isServiceType(service, 'comment')) {
    quantityText = `${commentConfig.value.videoIds.length} 个视频，共 ${totalTasks} 条评论`
    total = (totalTasks / 1000) * unitPrice
  } else if (isServiceType(service, 'follow')) {
    quantityText = `${followConfig.value.targetUsers.length} 个用户`
    total = (totalTasks / 1000) * unitPrice
  }

  successData.value = {
    orderId: orderData.order_id || orderData.id || 'N/A',
    service: SERVICE_TYPES[service].name,
    quantity: quantityText,
    amount: `${total.toFixed(2)} 积分`
  }

  showSuccess.value = true
}

function handleViewOrder () {
  showSuccess.value = false
  showProgressModal.value = true
  loadOrders()
}

function handleContinueOrder () {
  showSuccess.value = false
  resetForm()
}

function resetForm () {
  remark.value = ''
  playConfig.value = { videoIds: [], orderQuantityPerVideo: 1 }
  likeConfig.value = { videoIds: [], likeCountPerVideo: 1000 }
  commentConfig.value = {
    videoIds: [],
    commentCountPerVideo: 1000,
    commentTemplates: [],
    addEmoji: true
  }
  followConfig.value = {
    targetUsers: [],
    messageContent: '',
    addRandomEmoji: true,
    customEmojis: []
  }

  const firstService = renderedServices.value[0]
  selectedService.value = firstService?.key || ''
}

function showProgressPage () {
  showProgressModal.value = true
}

function handleProgressModalOpen () {
  // 模态框打开时自动加载订单
  if (cardKey.value) {
    loadOrders()
  }
}

async function loadOrders (orderKey = cardKey.value) {
  ordersLoading.value = true
  try {
    if (!orderKey) {
      message.warning('请先输入卡密')
      orders.value = []
      return
    }
    const list = await fetchOrdersByCard(orderKey)
    orders.value = list
    childOrdersMap.value = {}
    childLoadingMap.value = {}
  } catch (error) {
    console.error('加载订单失败:', error)
    const msg = error?.message || '网络错误，请稍后重试'
    message.error(msg)
    orders.value = []
  } finally {
    ordersLoading.value = false
  }
}

async function searchOrderById () {
  const key = searchOrderKey.value.trim()
  if (!key) {
    message.warning('请输入卡密')
    return
  }

  await loadOrders(key)
}

async function loadAllOrders () {
  if (!cardKey.value) {
    message.warning('请先输入卡密')
    return
  }
  await loadOrders()
}

async function handleLoadChildOrders (parentOrder) {
  const parentId = parentOrder?.order_id || parentOrder?.id
  if (!parentId) {
    message.warning('无法识别父订单编号')
    return
  }

  childLoadingMap.value = {
    ...childLoadingMap.value,
    [parentId]: true
  }

  try {
    const result = await apiService.getChildOrders(parentId)
    if (result.code === 200) {
      const list = Array.isArray(result.data?.list) ? result.data.list : []
      childOrdersMap.value = {
        ...childOrdersMap.value,
        [parentId]: list
      }
    } else {
      throw new Error(result?.msg || '加载子订单失败')
    }
  } catch (error) {
    console.error('加载子订单失败:', error)
    message.error(error?.message || '网络错误，请稍后重试')
  } finally {
    childLoadingMap.value = {
      ...childLoadingMap.value,
      [parentId]: false
    }
  }
}

async function refreshCardStats () {
  if (!cardKey.value) {
    message.warning('请先输入卡密')
    return
  }

  handleCardQuery(cardKey.value)
}

async function handleCardQuery (queryCardKey) {
  try {
    const cardInfoResult = await apiService.getCardInfo(queryCardKey)
    if (cardInfoResult.code !== 200) {
      throw new Error(cardInfoResult?.msg || '查询失败')
    }

    const stats = cardInfoResult.data?.order_info || {}
    const relatedOrders = await fetchOrdersByCard(queryCardKey)

    cardQueryRef.value?.setQueryResult(stats, relatedOrders)
    cardKey.value = queryCardKey
    storage.set('cardKey', queryCardKey)
    cardStats.value = stats
  } catch (error) {
    console.error('查询卡密信息失败:', error)
    const msg = error?.message || '网络错误，请稍后重试'
    message.error(msg)
  }
}

function handleCardQueryReset () {
  // 重置逻辑
}

// 监听卡密变化
watch(cardKey, async (newVal) => {
  if (newVal) {
    storage.set('cardKey', newVal)
    await refreshCardStats()
  }
})

// 初始化
onMounted(async () => {
  // 加载价格
  await loadServiceList()

  // 加载保存的卡密
  const savedCardKey = storage.get('cardKey')
  if (savedCardKey) {
    cardKey.value = savedCardKey
    await refreshCardStats()
  }
})
</script>

<style scoped>
.order-info {
  background: #f8f9fa;
  border-radius: 12px;
  padding: 20px;
  margin: 20px 0;
}

.order-info-row {
  display: flex;
  justify-content: space-between;
  margin-bottom: 10px;
  font-size: 14px;
}

.success-page {
  text-align: center;
  padding: 20px;
}

.success-icon {
  font-size: 80px;
  color: #28a745;
  margin-bottom: 20px;
}

.success-page h2 {
  color: #333;
  margin-bottom: 10px;
}

.success-page p {
  color: #666;
  margin-bottom: 30px;
}

.input-group {
  margin-bottom: 20px;
}

.input-group label {
  display: inline-block;
  margin-bottom: 8px;
  font-weight: 500;
  color: #555;
}

.progress-modal-content .section {
  margin-bottom: 20px;
}

.hint {
  font-size: 12px;
  color: #999;
  margin-top: 5px;
}
</style>
