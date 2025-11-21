<template>
  <div class="section">
    <div class="section-title">评论任务配置</div>

    <div class="input-group">
      <label class="input-group-label">待评论视频ID列表（直接输入或上传TXT文件）</label>
      <a-textarea v-model:value="videoIdsText" :rows="5" placeholder="每行一个视频ID" @change="handleVideoIdsChange" />
      <div style="display: flex; gap: 10px; margin-top: 10px;">
        <a-button type="primary" @click="triggerFileInput" style="flex: 1;">
          📁 上传TXT文件
        </a-button>
        <a-button @click="clearVideoIds" style="flex: 1;">🗑️ 清空</a-button>
      </div>
      <input ref="fileInput" type="file" accept=".txt" style="display: none" @change="handleFileChange" />
      <div class="hint">请直接输入或上传文件，每行一个视频ID</div>
    </div>

    <div class="input-group">
      <label class="input-group-label">每个视频评论数量</label>
      <a-input-number v-model:value="commentCountPerVideo" :min="100" :step="100" style="width: 100%"
        placeholder="例如：1000" @change="handleCountChange" />
      <div class="hint">建议按1000为单位，例如：1000、2000、3000</div>
    </div>

    <div class="input-group">
      <label class="input-group-label">评论模板列表（直接输入或上传TXT文件）</label>
      <a-textarea v-model:value="templatesText" :rows="6" placeholder="每行一条评论模板" @change="handleTemplatesChange" />
      <div style="display: flex; gap: 10px; margin-top: 10px;">
        <a-button type="primary" @click="triggerTemplatesFileInput" style="flex: 1;">
          📁 上传TXT文件
        </a-button>
        <a-button @click="clearTemplates" style="flex: 1;">🗑️ 清空</a-button>
      </div>
      <input ref="templatesFileInput" type="file" accept=".txt" style="display: none"
        @change="handleTemplatesFileChange" />
      <div class="hint">系统会随机选择模板进行评论</div>
    </div>

    <div class="input-group">
      <a-checkbox v-model:checked="addEmoji">
        随机添加表情后缀
      </a-checkbox>
      <div class="hint">例如：很棒的视频 👍、喜欢 ❤️、太好了 🎉</div>
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
const templatesFileInput = ref(null)
const videoIdsText = ref('')
const templatesText = ref('')
const commentCountPerVideo = ref(1000)
const addEmoji = ref(true)

const videoIds = computed(() => {
  return videoIdsText.value.split('\n')
    .map(id => id.trim())
    .filter(id => id.length > 0)
})

const commentTemplates = computed(() => {
  return templatesText.value.split('\n')
    .map(t => t.trim())
    .filter(t => t.length > 0)
})

const triggerFileInput = () => {
  fileInput.value?.click()
}

const triggerTemplatesFileInput = () => {
  templatesFileInput.value?.click()
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

const handleTemplatesFileChange = async (e) => {
  const file = e.target.files[0]
  if (file) {
    try {
      const content = await readTextFile(file)
      const templates = content.split('\n').map(t => t.trim()).filter(t => t.length > 0)
      templatesText.value = templates.join('\n')
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

const clearTemplates = () => {
  templatesText.value = ''
  updateConfig()
}

const handleVideoIdsChange = () => {
  updateConfig()
}

const handleTemplatesChange = () => {
  updateConfig()
}

const handleCountChange = () => {
  updateConfig()
}

watch(addEmoji, () => {
  updateConfig()
})

const updateConfig = () => {
  emit('update:modelValue', {
    videoIds: videoIds.value,
    commentCountPerVideo: commentCountPerVideo.value,
    commentTemplates: commentTemplates.value,
    addEmoji: addEmoji.value
  })
}

// 初始化
if (props.modelValue) {
  videoIdsText.value = props.modelValue.videoIds?.join('\n') || ''
  templatesText.value = props.modelValue.commentTemplates?.join('\n') || ''
  commentCountPerVideo.value = props.modelValue.commentCountPerVideo || 1000
  addEmoji.value = props.modelValue.addEmoji !== undefined ? props.modelValue.addEmoji : true
}

watch(() => props.modelValue, (newVal) => {
  if (newVal) {
    videoIdsText.value = newVal.videoIds?.join('\n') || ''
    templatesText.value = newVal.commentTemplates?.join('\n') || ''
    commentCountPerVideo.value = newVal.commentCountPerVideo || 1000
    addEmoji.value = newVal.addEmoji !== undefined ? newVal.addEmoji : true
  }
}, { deep: true })
</script>

<style scoped>
.input-group {
  margin-bottom: 20px;
}

.input-group-label {
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
