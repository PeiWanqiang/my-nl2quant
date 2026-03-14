<template>
  <div>
    <h3>回测/选股结果盘 (共 {{ tableData.length }} 只)</h3>
    <el-table :data="tableData" style="width: 100%" border max-height="400">
      <el-table-column prop="code" label="股票代码" width="180" />
      <el-table-column prop="name" label="名称" width="180" />
      <el-table-column prop="price" label="最新价" />
      <el-table-column prop="change" label="涨跌幅" />
    </el-table>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  tableData: any[]
}>()
</script>
