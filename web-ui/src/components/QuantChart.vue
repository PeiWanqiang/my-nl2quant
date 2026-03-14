<template>
  <div>
    <h3>回测/选股结果盘 (共 {{ tableData.length }} 只)</h3>
    <el-table :data="tableData" style="width: 100%" border max-height="400">
      <el-table-column prop="code" label="股票代码" width="120" />
      <el-table-column prop="name" label="名称" width="150" />
      <el-table-column prop="price" label="最新价">
        <template #default="scope">
          <span :style="{ color: parseFloat(scope.row.change) >= 0 ? '#f56c6c' : '#67c23a' }">
            {{ scope.row.price }}
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="change" label="涨跌幅">
        <template #default="scope">
          <span :style="{ color: parseFloat(scope.row.change) >= 0 ? '#f56c6c' : '#67c23a' }">
            {{ parseFloat(scope.row.change) > 0 ? '+' : '' }}{{ scope.row.change }}
          </span>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  tableData: any[]
}>()
</script>
