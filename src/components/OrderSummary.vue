<template>
  <div class="summary-box">
    <div class="summary-row">
      <span>服务类型：</span>
      <span>{{ summaryService }}</span>
    </div>
    <div class="summary-row">
      <span>单价：</span>
      <span>{{ summaryUnitPrice }}</span>
    </div>
    <div class="summary-row">
      <span>数量：</span>
      <span>{{ summaryQuantity }}</span>
    </div>
    <div class="summary-row total" style="margin-bottom: 0;">
      <span>总计：</span>
      <span>{{ summaryTotal }}</span>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { SERVICE_TYPES, SERVICE_KEY_MAP } from '../constants'

const props = defineProps({
  selectedService: String,
  servicePrices: Object,
  playConfig: Object,
  likeConfig: Object,
  commentConfig: Object,
  followConfig: Object
})

const summaryService = computed(() => {
  if (!props.selectedService || !SERVICE_TYPES[props.selectedService]) {
    return '未选择'
  }
  return SERVICE_TYPES[props.selectedService].name
})

const summaryUnitPrice = computed(() => {
  if (!props.selectedService) return '-'
  const unitPrice = props.servicePrices[props.selectedService] || 0
  const unit = SERVICE_TYPES[props.selectedService]?.unit || ''
  return `${unitPrice} 积分/${unit}`
})

function isServiceType (service, type) {
  const variants = SERVICE_KEY_MAP[type] || []
  return variants.includes(service)
}

const summaryQuantity = computed(() => {
  if (!props.selectedService) return '0 单位'

  const service = props.selectedService

  if (isServiceType(service, 'playVedio')) {
    const videoCount = props.playConfig?.videoIds?.length || 0
    const orderQuantityPerVideo = props.playConfig?.orderQuantityPerVideo || 0
    const totalTasks = videoCount * orderQuantityPerVideo * 1000
    return `${videoCount} 个视频 × ${orderQuantityPerVideo} 单（每单1000次播放） = ${totalTasks} 次播放`
  } else if (isServiceType(service, 'likeVedio')) {
    const videoCount = props.likeConfig?.videoIds?.length || 0
    const likeCountPerVideo = props.likeConfig?.likeCountPerVideo || 0
    return `${videoCount} 个视频 × ${likeCountPerVideo} 个点赞`
  } else if (isServiceType(service, 'commentVedio')) {
    const videoCount = props.commentConfig?.videoIds?.length || 0
    const commentCountPerVideo = props.commentConfig?.commentCountPerVideo || 0
    return `${videoCount} 个视频 × ${commentCountPerVideo} 条评论`
  } else if (isServiceType(service, 'followVedio')) {
    const userCount = props.followConfig?.targetUsers?.length || 0
    return `${userCount} 个用户`
  }

  return '0 单位'
})

const summaryTotal = computed(() => {
  if (!props.selectedService) return '0 积分'

  const service = props.selectedService
  const unitPrice = props.servicePrices[service] || 0
  let totalTasks = 0

  if (isServiceType(service, 'playVedio')) {
    const videoCount = props.playConfig?.videoIds?.length || 0
    const orderQuantityPerVideo = props.playConfig?.orderQuantityPerVideo || 0
    totalTasks = videoCount * orderQuantityPerVideo * 1000
    return `${((totalTasks / 1000) * unitPrice).toFixed(2)} 积分`
  } else if (isServiceType(service, 'likeVedio')) {
    const videoCount = props.likeConfig?.videoIds?.length || 0
    const likeCountPerVideo = props.likeConfig?.likeCountPerVideo || 0
    totalTasks = videoCount * likeCountPerVideo
    return `${((totalTasks / 1000) * unitPrice).toFixed(2)} 积分`
  } else if (isServiceType(service, 'commentVedio')) {
    const videoCount = props.commentConfig?.videoIds?.length || 0
    const commentCountPerVideo = props.commentConfig?.commentCountPerVideo || 0
    totalTasks = videoCount * commentCountPerVideo
    return `${((totalTasks / 1000) * unitPrice).toFixed(2)} 积分`
  } else if (isServiceType(service, 'followVedio')) {
    const userCount = props.followConfig?.targetUsers?.length || 0
    totalTasks = userCount
    return `${((totalTasks / 1000) * unitPrice).toFixed(2)} 积分`
  }

  return '0 积分'
})
</script>
