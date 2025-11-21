<template>
  <div class="section">
    <div class="section-title">私信任务配置</div>

    <div class="input-group">
      <label class="input-group-label">待私信用户列表（直接输入或上传TXT文件）</label>
      <a-textarea v-model:value="usersText" :rows="5" placeholder="每行一个用户名或用户ID" @change="handleUsersChange" />
      <div style="display: flex; gap: 10px; margin-top: 10px;">
        <a-button type="primary" @click="triggerFileInput" style="flex: 1;">
          📁 上传TXT文件
        </a-button>
        <a-button @click="clearUsers" style="flex: 1;">🗑️ 清空</a-button>
      </div>
      <input ref="fileInput" type="file" accept=".txt" style="display: none" @change="handleFileChange" />
      <div class="hint">{{ usersCountHint }}</div>
    </div>

    <div class="input-group">
      <label class="input-group-label">私信文本内容</label>
      <a-textarea v-model:value="messageContent" :rows="4" placeholder="输入要发送的私信内容..." @change="handleMessageChange" />
      <div class="hint">支持多行文本，可以使用变量：{username} = 用户名</div>
    </div>

    <div class="input-group">
      <a-checkbox v-model:checked="addRandomEmoji">
        随机添加表情后缀
      </a-checkbox>
      <div class="hint">系统会从常用表情中随机选择1-3个添加到消息末尾</div>
    </div>

    <div class="input-group">
      <label class="input-group-label">自定义表情（可选）</label>
      <a-input v-model:value="customEmojisText" placeholder="例如：😊,❤️,👍,🎉,✨" @change="handleEmojisChange" />
      <div class="hint">用逗号分隔多个表情，留空则使用系统默认</div>
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
const usersText = ref('')
const messageContent = ref('')
const addRandomEmoji = ref(true)
const customEmojisText = ref('')

const targetUsers = computed(() => {
  return usersText.value.split('\n')
    .map(u => u.trim())
    .filter(u => u.length > 0)
})

const customEmojis = computed(() => {
  return customEmojisText.value.split(',')
    .map(e => e.trim())
    .filter(e => e.length > 0)
})

const usersCountHint = computed(() => {
  const count = targetUsers.value.length
  return count > 0 ? `已输入 ${count} 个用户` : '请直接输入或上传文件，每行一个用户'
})

const triggerFileInput = () => {
  fileInput.value?.click()
}

const handleFileChange = async (e) => {
  const file = e.target.files[0]
  if (file) {
    try {
      const content = await readTextFile(file)
      const users = content.split('\n').map(u => u.trim()).filter(u => u.length > 0)
      usersText.value = users.join('\n')
      updateConfig()
      e.target.value = ''
    } catch (error) {
      console.error('读取文件失败:', error)
    }
  }
}

const clearUsers = () => {
  usersText.value = ''
  updateConfig()
}

const handleUsersChange = () => {
  updateConfig()
}

const handleMessageChange = () => {
  updateConfig()
}

const handleEmojisChange = () => {
  updateConfig()
}

watch(addRandomEmoji, () => {
  updateConfig()
})

const updateConfig = () => {
  emit('update:modelValue', {
    targetUsers: targetUsers.value,
    messageContent: messageContent.value,
    addRandomEmoji: addRandomEmoji.value,
    customEmojis: customEmojis.value
  })
}

// 初始化
if (props.modelValue) {
  usersText.value = props.modelValue.targetUsers?.join('\n') || ''
  messageContent.value = props.modelValue.messageContent || ''
  addRandomEmoji.value = props.modelValue.addRandomEmoji !== undefined ? props.modelValue.addRandomEmoji : true
  customEmojisText.value = props.modelValue.customEmojis?.join(',') || ''
}

watch(() => props.modelValue, (newVal) => {
  if (newVal) {
    usersText.value = newVal.targetUsers?.join('\n') || ''
    messageContent.value = newVal.messageContent || ''
    addRandomEmoji.value = newVal.addRandomEmoji !== undefined ? newVal.addRandomEmoji : true
    customEmojisText.value = newVal.customEmojis?.join(',') || ''
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
