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
  renderedServices: Array,
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

  // 从 renderedServices 中获取当前服务的完整信息
  const serviceObj = props.renderedServices?.find(s => s.key === props.selectedService)
  if (serviceObj) {
    const price = Number(serviceObj.price) || 0
    const unitNum = Number(serviceObj.unit_num) || 1
    // 根据服务类型显示不同的单位
    if (isServiceType(props.selectedService, 'playVedio')) {
      return `${price} 积分/${unitNum}次播放`
    } else if (isServiceType(props.selectedService, 'likeVedio')) {
      return `${price} 积分/${unitNum}个点赞`
    } else if (isServiceType(props.selectedService, 'commentVedio')) {
      return `${price} 积分/${unitNum}条评论`
    } else if (isServiceType(props.selectedService, 'followVedio')) {
      return `${price} 积分/${unitNum}条私信`
    }
  }

  // 降级方案：使用默认值
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
    // 从 renderedServices 中获取当前服务的 unit_num
    const serviceObj = props.renderedServices?.find(s => s.key === service)
    const unitNum = Number(serviceObj?.unit_num) || 1000
    const totalTasks = videoCount * orderQuantityPerVideo * unitNum
    return `${videoCount} 个视频 × ${orderQuantityPerVideo} 单（每单${unitNum}次播放） = ${totalTasks} 次播放`
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
  // 从 renderedServices 中获取当前服务的完整信息
  const serviceObj = props.renderedServices?.find(s => s.key === service)
  if (!serviceObj) return '0 积分'

  const price = Number(serviceObj.price) || 0
  const unitNum = Number(serviceObj.unit_num) || 1
  let totalTasks = 0

  if (isServiceType(service, 'playVedio')) {
    const videoCount = props.playConfig?.videoIds?.length || 0
    const orderQuantityPerVideo = props.playConfig?.orderQuantityPerVideo || 0
    totalTasks = videoCount * orderQuantityPerVideo * unitNum
    // 根据 unit_num 和 price 计算：总价 = (总次数 / 单位次数) * 单价
    return `${((totalTasks / unitNum) * price).toFixed(2)} 积分`
  } else if (isServiceType(service, 'likeVedio')) {
    const videoCount = props.likeConfig?.videoIds?.length || 0
    const likeCountPerVideo = props.likeConfig?.likeCountPerVideo || 0
    totalTasks = videoCount * likeCountPerVideo
    return `${((totalTasks / unitNum) * price).toFixed(2)} 积分`
  } else if (isServiceType(service, 'commentVedio')) {
    const videoCount = props.commentConfig?.videoIds?.length || 0
    const commentCountPerVideo = props.commentConfig?.commentCountPerVideo || 0
    totalTasks = videoCount * commentCountPerVideo
    return `${((totalTasks / unitNum) * price).toFixed(2)} 积分`
  } else if (isServiceType(service, 'followVedio')) {
    const userCount = props.followConfig?.targetUsers?.length || 0
    totalTasks = userCount
    return `${((totalTasks / unitNum) * price).toFixed(2)} 积分`
  }

  return '0 积分'
})
</script>
