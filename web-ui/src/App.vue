<template>
  <div class="app-container">
    <el-container>
      <el-header>
        <h2>NL2Quant 智能量化选股系统</h2>
      </el-header>
      <el-container>
        <el-aside width="400px">
          <chat-box @update-conditions="handleConditionsUpdate" />
        </el-aside>
        <el-main>
          <condition-tree v-if="conditions.length" :conditions="conditions" @execute="handleExecute" />
          <el-empty v-else description="输入您的选股灵感，让AI帮您生成策略" />
          <el-divider v-if="conditions.length" />
          <quant-chart v-if="showChart" />
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import ChatBox from './components/ChatBox.vue'
import ConditionTree from './components/ConditionTree.vue'
import QuantChart from './components/QuantChart.vue'

const conditions = ref<any[]>([])
const showChart = ref(false)
const generatedCode = ref('')

const handleConditionsUpdate = (newConditions: any[], code: string | null) => {
  conditions.value = newConditions
  if (code) {
    generatedCode.value = code
  }
}

const handleExecute = async () => {
  if (!generatedCode.value) {
     alert("策略尚未生成执行代码，请继续与AI确认。")
     return
  }
  try {
     const res = await fetch('/api/v1/gateway/quant/execute', {
       method: 'POST',
       headers: { 'Content-Type': 'application/json' },
       body: JSON.stringify({ code: generatedCode.value })
     });
     const data = await res.json()
     if (res.ok) {
       showChart.value = true
     } else {
       alert("执行失败: " + JSON.stringify(data))
     }
  } catch (err) {
    console.error(err)
  }
}
</script>

<style scoped>
.app-container {
  height: 100vh;
}
.el-header {
  background-color: #409EFF;
  color: white;
  display: flex;
  align-items: center;
}
.el-aside {
  border-right: 1px solid #dcdfe6;
  padding: 10px;
}
.el-main {
  padding: 20px;
}
</style>
