<template>
  <el-card class="condition-card">
    <template #header>
      <div class="card-header">
        <span>策略微调面板 (白盒)</span>
        <el-button type="success" @click="$emit('execute')">执行沙盒测试</el-button>
      </div>
    </template>
    
    <div v-for="cond in conditions" :key="cond.id" class="condition-item">
      <h4>{{ cond.name }}</h4>
      <p class="desc">{{ cond.description }}</p>
      
      <el-form label-width="120px" class="param-form">
        <el-form-item v-for="(param, key) in cond.parameters" :key="key" :label="key.toString()">
          <el-slider 
            v-if="param.type === 'int' || param.type === 'float'" 
            v-model="param.value" 
            :min="param.min || 0" 
            :max="param.max || 100"
            :step="param.type === 'float' ? 0.1 : 1"
            show-input
          />
        </el-form-item>
      </el-form>
      <el-divider />
    </div>
  </el-card>
</template>

<script setup lang="ts">
defineProps<{
  conditions: any[]
}>()
defineEmits(['execute'])
</script>

<style scoped>
.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.condition-item {
  margin-bottom: 20px;
}
.desc {
  color: #909399;
  font-size: 14px;
  margin-bottom: 10px;
}
.param-form {
  padding-left: 20px;
}
</style>
