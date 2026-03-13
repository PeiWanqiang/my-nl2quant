<template>
  <div class="chat-container">
    <div class="message-list">
      <div v-for="(msg, index) in messages" :key="index" :class="['message', msg.role]">
        <span v-if="msg.role === 'user'">👤: </span>
        <span v-else>🤖: </span>
        <span>{{ msg.content }}</span>
      </div>
    </div>
    <div class="input-area">
      <el-input 
        v-model="input" 
        type="textarea" 
        :rows="3" 
        placeholder="例如：寻找近期放量突破的股票" 
        @keyup.enter="sendMessage"
      />
      <el-button type="primary" class="send-btn" @click="sendMessage" :loading="loading">发送</el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'

const emit = defineEmits(['update-conditions'])

const messages = ref<{role: string, content: string}[]>([
  {role: 'ai', content: '您好！我是 NL2Quant 助理，请用自然语言描述您的选股策略。'}
])
const input = ref('')
const loading = ref(false)
const currentConditions = ref<any[]>([])

const sendMessage = async () => {
  if (!input.value.trim()) return
  
  const userText = input.value
  messages.value.push({ role: 'user', content: userText })
  input.value = ''
  loading.value = true

  try {
    const response = await fetch('/api/v1/gateway/chat/negotiate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 
        user_input: userText,
        current_conditions: currentConditions.value
      })
    })
    
    const data = await response.json()
    messages.value.push({ role: 'ai', content: data.ai_message })
    
    currentConditions.value = data.extracted_conditions
    emit('update-conditions', currentConditions.value, data.executable_code)
    
  } catch (err) {
    messages.value.push({ role: 'ai', content: '服务异常，请重试。' })
    console.error(err)
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.chat-container {
  display: flex;
  flex-direction: column;
  height: 100%;
}
.message-list {
  flex: 1;
  overflow-y: auto;
  padding-bottom: 20px;
}
.message {
  margin-bottom: 15px;
  line-height: 1.5;
}
.message.user {
  text-align: right;
  color: #409eff;
}
.message.ai {
  text-align: left;
  background-color: #f4f4f5;
  padding: 10px;
  border-radius: 4px;
}
.input-area {
  margin-top: auto;
}
.send-btn {
  margin-top: 10px;
  width: 100%;
}
</style>
