<template>
  <div class="section">
    <div class="section-title">播放任务配置</div>

    <div class="input-group">
      <label>视频ID列表（直接输入或上传TXT文件）</label>
      <a-textarea v-model:value="videoIdsText" :rows="5"
        placeholder="每行一个视频ID，例如：&#10;7569608169052212501&#10;7569608169052212502&#10;7569608169052212503"
        @change="handleVideoIdsChange" />
      <div style="display: flex; gap: 10px; margin-top: 10px;">
        <a-button type="primary" @click="triggerFileInput" style="flex: 1;">
          📁 上传TXT文件
        </a-button>
        <a-button @click="clearVideoIds" style="flex: 1;">🗑️ 清空</a-button>
      </div>
      <input ref="fileInput" type="file" accept=".txt" style="display: none" @change="handleFileChange" />
      <div class="hint">{{ videoCountHint }}</div>
    </div>

    <div class="input-group">
      <label>每个视频订单数量（每单=1000次播放）</label>
      <a-input-number v-model:value="orderQuantityPerVideo" :min="1" :step="1" style="width: 100%"
        placeholder="例如：5 表示5000次播放" @change="handleQuantityChange" />
      <div class="hint">订单以1000次播放为单位，请输入整数订单数</div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { readTextFile } from '../utils/file'

const props = defineProps({
  modelValue: Object
})

const emit = defineEmits(['update:modelValue'])

const fileInput = ref(null)
const videoIdsText = ref('')
const orderQuantityPerVideo = ref(1)

const videoIds = computed(() => {
  return videoIdsText.value.split('\n')
    .map(id => id.trim())
    .filter(id => id.length > 0)
})

const videoCountHint = computed(() => {
  const count = videoIds.value.length
  return count > 0 ? `已输入 ${count} 个视频ID` : '请直接输入或上传文件，每行一个视频ID'
})

const triggerFileInput = () => {
  fileInput.value?.click()
}

const handleFileChange = async (e) => {
  const file = e.target.files[0]
  if (file) {
    try {
      const content = await readTextFile(file)
      const ids = content.split('\n').map(id => id.trim()).filter(id => id.length > 0)
      videoIdsText.value = ids.join('\n')
      updateConfig()
      e.target.value = ''
    } catch (error) {
      console.error('读取文件失败:', error)
    }
  }
}

const clearVideoIds = () => {
  videoIdsText.value = ''
  updateConfig()
}

const handleVideoIdsChange = () => {
  updateConfig()
}

const handleQuantityChange = () => {
  updateConfig()
}

const updateConfig = () => {
  emit('update:modelValue', {
    videoIds: videoIds.value,
    orderQuantityPerVideo: orderQuantityPerVideo.value
  })
}

// 初始化
if (props.modelValue) {
  videoIdsText.value = props.modelValue.videoIds?.join('\n') || ''
  orderQuantityPerVideo.value = props.modelValue.orderQuantityPerVideo || 1
}

watch(() => props.modelValue, (newVal) => {
  if (newVal) {
    videoIdsText.value = newVal.videoIds?.join('\n') || ''
    orderQuantityPerVideo.value = newVal.orderQuantityPerVideo || 1
  }
}, { deep: true })
</script>

<style scoped>
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
</style>
