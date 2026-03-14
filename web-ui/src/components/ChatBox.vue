<template>
  <div class="chat-container">
    <div class="chat-header">
      <h3 class="chat-title">量化策略助理</h3>
    </div>
    <div class="message-list" ref="messageListRef">
      <div v-for="(msg, index) in messages" :key="index" :class="['message-wrapper', msg.role]">
        <div class="avatar">{{ msg.role === 'user' ? '🧑‍💻' : '🤖' }}</div>
        <div class="message-bubble">
          {{ msg.content }}
        </div>
      </div>
    </div>
    <div class="input-area">
      <el-input 
        v-model="input" 
        type="textarea" 
        :rows="3" 
        placeholder="例如：寻找近期放量突破的股票..." 
        resize="none"
        @keyup.enter.prevent="sendMessage"
        :disabled="loading"
      />
      <div class="input-actions">
        <el-button 
          type="primary" 
          class="send-btn" 
          @click="sendMessage" 
          :loading="loading"
          round
        >
          发送指令
        </el-button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, nextTick } from 'vue'
import { ElMessage } from 'element-plus'

const emit = defineEmits(['update-conditions', 'clear-results'])

const messages = ref<{role: string, content: string}[]>([
  {role: 'ai', content: '您好！我是 NL2Quant 助理，请用自然语言描述您的选股策略。'}
])
const input = ref('')
const loading = ref(false)
const currentConditions = ref<any[]>([])
const messageListRef = ref<HTMLElement | null>(null)

const scrollToBottom = async () => {
  await nextTick()
  if (messageListRef.value) {
    messageListRef.value.scrollTop = messageListRef.value.scrollHeight
  }
}

const sendMessage = async () => {
  if (!input.value.trim() || loading.value) return
  
  const userText = input.value
  messages.value.push({ role: 'user', content: userText })
  input.value = ''
  
  await scrollToBottom()
  
  // Create a placeholder for the loading state message
  messages.value.push({ role: 'ai', content: '🧠 正在解析量化意图...' })
  const aiMessageIndex = messages.value.length - 1
  
  await scrollToBottom()
  
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
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }
    
    const data = await response.json()
    
    // Update the message with a better interactive prompt
    messages.value[aiMessageIndex].content = `✅ 解析成功！我已提取出 ${data.extracted_conditions?.length || 0} 个量化条件，请在右侧面板核对并微调参数。确认无误后点击执行测试。`
    
    currentConditions.value = data.extracted_conditions || []
    emit('update-conditions', currentConditions.value, data.executable_code)
    
  } catch (err) {
    messages.value[aiMessageIndex].content = '服务异常，请重试。'
    console.error(err)
    ElMessage.error('网络或服务异常')
  } finally {
    loading.value = false
    await scrollToBottom()
  }
}
</script>

<style scoped>
.chat-container {
  display: flex;
  flex-direction: column;
  height: 100%;
  background-color: var(--el-bg-color);
}
.chat-header {
  padding: 16px 20px;
  border-bottom: 1px solid var(--el-border-color-light);
  background-color: var(--el-bg-color);
}
.chat-title {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
  color: var(--el-text-color-primary);
}
.message-list {
  flex: 1;
  overflow-y: auto;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 16px;
  background-color: var(--el-fill-color-blank);
}
.message-wrapper {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  max-width: 90%;
}
.message-wrapper.user {
  align-self: flex-end;
  flex-direction: row-reverse;
}
.avatar {
  font-size: 24px;
  background: var(--el-fill-color-light);
  border-radius: 50%;
  padding: 4px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.05);
}
.message-bubble {
  padding: 12px 16px;
  border-radius: 12px;
  font-size: 14px;
  line-height: 1.6;
  word-break: break-word;
  box-shadow: 0 1px 2px rgba(0,0,0,0.05);
}
.message-wrapper.user .message-bubble {
  background-color: var(--el-color-primary);
  color: white;
  border-top-right-radius: 4px;
}
.message-wrapper.ai .message-bubble {
  background-color: var(--el-fill-color-light);
  color: var(--el-text-color-primary);
  border-top-left-radius: 4px;
}
.input-area {
  padding: 16px;
  background-color: var(--el-bg-color);
  border-top: 1px solid var(--el-border-color-light);
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.input-actions {
  display: flex;
  justify-content: flex-end;
}
.send-btn {
  padding: 8px 24px;
  font-weight: 500;
}
</style>
