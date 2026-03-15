<template>
  <div class="app-container" :class="{ 'dark-mode': isDark }">
    <el-config-provider :builtin-theme="isDark ? 'dark' : 'light'">
      <el-container class="main-layout">
        <el-header class="app-header">
          <div class="logo-area">
            <el-icon class="logo-icon"><DataLine /></el-icon>
            <h2>NL2Quant 智能量化终端</h2>
          </div>
          <div class="actions-area">
            <el-switch
              v-model="isDark"
              inline-prompt
              :active-icon="Moon"
              :inactive-icon="Sunny"
              @change="toggleDark"
            />
          </div>
        </el-header>
        
        <el-container class="content-container">
          <el-aside width="35%" class="chat-sidebar">
            <chat-box 
              @update-conditions="handleConditionsUpdate" 
              @clear-results="clearResults"
            />
          </el-aside>
          
          <el-main class="workspace">
            <div class="workspace-scroll">
              <condition-tree 
                v-if="conditions.length" 
                :conditions="conditions" 
                :executing="isExecuting"
                @execute="handleExecute" 
                @delete="handleDeleteCondition"
                @update="handleConditionChange"
              />
              <el-empty v-else class="empty-state" description="输入您的选股灵感，让 AI 帮您生成量化策略" />
              
              <quant-chart 
                v-if="showChart" 
                :table-data="executionResults" 
                :loading="isExecuting" 
              />
            </div>
          </el-main>
        </el-container>
      </el-container>
    </el-config-provider>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useDark, useToggle } from '@vueuse/core'
import { DataLine, Moon, Sunny } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import ChatBox from './components/ChatBox.vue'
import ConditionTree from './components/ConditionTree.vue'
import QuantChart from './components/QuantChart.vue'

const isDark = useDark()
const toggleDark = useToggle(isDark)

const conditions = ref<any[]>([])
const showChart = ref(false)
const generatedCode = ref('')
const executionResults = ref<any[]>([])
const isExecuting = ref(false)

const handleConditionsUpdate = (newConditions: any[], code: string | null) => {
  conditions.value = newConditions
  if (code) {
    generatedCode.value = code
  }
}

const handleConditionChange = (updatedCondition: any) => {
  const index = conditions.value.findIndex(c => c.id === updatedCondition.id)
  if (index !== -1) {
    conditions.value[index] = updatedCondition
  }
}

const handleDeleteCondition = (id: string) => {
  conditions.value = conditions.value.filter(c => c.id !== id)
  if (conditions.value.length === 0) {
    showChart.value = false
    executionResults.value = []
  }
}

const clearResults = () => {
  conditions.value = []
  showChart.value = false
  executionResults.value = []
}

const handleExecute = async () => {
  if (isExecuting.value) return
  if (conditions.value.length === 0) {
     ElMessage.warning("请先添加策略条件")
     return
  }
  
  isExecuting.value = true
  showChart.value = true // Show loading state in chart area
  executionResults.value = [] // Clear previous results
  
  try {
     // Because users can edit parameters, we need to request the AI to generate NEW code based on the edited conditions
      // 直接调用 confirm_and_execute 一步完成
      const res = await fetch('/api/v1/gateway/quant/confirm_and_execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          user_input: "确认执行当前策略",
          current_conditions: conditions.value 
        })
      });
      
      const data = await res.json()
     if (res.ok) {
       executionResults.value = data.data || []
       ElMessage.success(`执行成功，共找到 ${executionResults.value.length} 只股票`)
     } else {
       ElMessage.error(`执行失败: ${data.detail || '未知错误'}`)
     }
  } catch (err: any) {
    console.error(err)
    ElMessage.error(`请求异常: ${err.message}`)
  } finally {
    isExecuting.value = false
  }
}
</script>

<style>
/* Global variables */
:root {
  --app-bg: #f5f7fa;
  --panel-bg: #ffffff;
  --border-color: #e4e7ed;
  --header-bg: #ffffff;
  --header-text: #303133;
}

html.dark {
  --app-bg: #141414;
  --panel-bg: #1d1e1f;
  --border-color: #303030;
  --header-bg: #141414;
  --header-text: #e5eaf3;
}

body {
  margin: 0;
  padding: 0;
  background-color: var(--app-bg);
  color: var(--header-text);
}
</style>

<style scoped>
.app-container {
  height: 100vh;
  width: 100vw;
  overflow: hidden;
}

.main-layout {
  height: 100%;
}

.app-header {
  background-color: var(--header-bg);
  border-bottom: 1px solid var(--border-color);
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 20px;
  height: 60px;
  box-shadow: 0 1px 4px rgba(0,21,41,0.08);
  z-index: 10;
}

.logo-area {
  display: flex;
  align-items: center;
  gap: 10px;
}

.logo-icon {
  font-size: 24px;
  color: #409EFF;
}

.logo-area h2 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  letter-spacing: 1px;
}

.content-container {
  height: calc(100vh - 60px);
}

.chat-sidebar {
  background-color: var(--panel-bg);
  border-right: 1px solid var(--border-color);
  display: flex;
  flex-direction: column;
}

.workspace {
  background-color: var(--app-bg);
  padding: 20px;
  position: relative;
  overflow: hidden;
}

.workspace-scroll {
  height: 100%;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.empty-state {
  margin-top: 10vh;
  background-color: var(--panel-bg);
  border-radius: 8px;
  padding: 40px;
  box-shadow: 0 2px 12px 0 rgba(0,0,0,0.05);
}
</style>

