<template>
  <el-card class="condition-card" shadow="hover">
    <template #header>
      <div class="card-header">
        <span class="panel-title">🎯 策略微调面板 (白盒)</span>
        <el-button type="success" :loading="executing" @click="$emit('execute')">执行沙盒测试</el-button>
      </div>
    </template>
    
    <div class="conditions-container">
      <el-card 
        v-for="cond in conditions" 
        :key="cond.id" 
        class="condition-item"
        shadow="never"
      >
        <div class="condition-header">
          <h4>{{ cond.name }}</h4>
          <el-button 
            type="danger" 
            text 
            @click="$emit('delete', cond.id)"
            title="删除此条件"
          >
            🗑️ 删除
          </el-button>
        </div>
        <p class="desc">{{ cond.description }}</p>
        
        <div class="param-list">
          <div v-for="(param, key) in cond.parameters" :key="key" class="param-inline">
            <span class="param-label">{{ key }}</span>
            <el-input-number 
              v-if="param.type === 'int'" 
              v-model="param.value" 
              :min="param.min || 0" 
              :max="param.max || 1000"
              size="small"
              class="inline-input"
              @change="$emit('update', cond)"
            />
            <el-input-number 
              v-else-if="param.type === 'float'" 
              v-model="param.value" 
              :min="param.min || 0" 
              :max="param.max || 1000"
              :step="0.1"
              size="small"
              class="inline-input"
              @change="$emit('update', cond)"
            />
            <el-input 
              v-else 
              v-model="param.value" 
              size="small"
              class="inline-input"
              @change="$emit('update', cond)"
            />
          </div>
        </div>
      </el-card>
    </div>
  </el-card>
</template>

<script setup lang="ts">
defineProps<{
  conditions: any[]
  executing?: boolean
}>()
defineEmits(['execute', 'delete', 'update'])
</script>

<style scoped>
.condition-card {
  margin-bottom: 20px;
  border-radius: 8px;
}
.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.panel-title {
  font-weight: 600;
  font-size: 16px;
}
.conditions-container {
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.condition-item {
  background-color: var(--el-fill-color-light);
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
}
.condition-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}
.condition-header h4 {
  margin: 0;
  color: var(--el-text-color-primary);
  font-size: 15px;
}
.desc {
  color: var(--el-text-color-secondary);
  font-size: 13px;
  margin-bottom: 12px;
  margin-top: 0;
}
.param-list {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
}
.param-inline {
  display: flex;
  align-items: center;
  background-color: var(--el-bg-color);
  padding: 4px 12px;
  border-radius: 4px;
  border: 1px solid var(--el-border-color);
}
.param-label {
  margin-right: 8px;
  font-size: 13px;
  color: var(--el-text-color-regular);
}
.inline-input {
  width: 120px;
}
</style>
