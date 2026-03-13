<template>
  <div>
    <h3>回测/选股结果盘</h3>
    <el-table :data="tableData" style="width: 100%" border>
      <el-table-column prop="code" label="股票代码" width="180" />
      <el-table-column prop="name" label="名称" width="180" />
      <el-table-column prop="price" label="最新价" />
      <el-table-column prop="change" label="涨跌幅" />
    </el-table>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'

// Mock Data
const tableData = ref([
  { code: '000001', name: '平安银行', price: '10.50', change: '+2.1%' },
  { code: '600519', name: '贵州茅台', price: '1750.00', change: '+1.5%' },
])
</script>
