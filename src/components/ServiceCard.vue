<template>
  <div :class="['service-card', { selected: isSelected, presale: service?.is_sell === 0 }]" @click="handleClick">
    <div v-if="service?.is_sell === 0" class="presale-tag">预售</div>
    <div class="icon">{{ service?.icon }}</div>
    <div class="name">{{ service?.name }}</div>
    <div class="price" v-if="price !== null">
      {{ price }} 积分 / {{ service?.unit }}
    </div>
  </div>
</template>

<script setup>
import { message } from 'ant-design-vue'

const props = defineProps({
  serviceKey: String,
  service: Object,
  price: [Number, null],
  isSelected: Boolean
})

const emit = defineEmits(['select'])

function handleClick () {
  if (props.service?.is_sell === 0) {
    message.info('该服务为预售，请耐心等待')
    return
  }
  emit('select', props.serviceKey)
}
</script>

<style scoped>
.presale-tag {
  display: inline-flex;
  align-items: center;
  position: absolute;
  top: 0;
  right: 0;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  color: #b45309;
  background: rgba(245, 158, 11, 0.15);
  margin-bottom: 10px;
}

.service-card.presale {
  border-color: rgba(245, 158, 11, 0.4);
}
</style>
