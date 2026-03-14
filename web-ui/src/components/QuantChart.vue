<template>
  <el-card class="result-card" shadow="hover" v-loading="loading">
    <template #header>
      <div class="card-header">
        <span class="panel-title">📊 回测与选股结果 (共 {{ tableData.length }} 只)</span>
      </div>
    </template>
    
    <el-table 
      :data="tableData" 
      style="width: 100%" 
      border 
      max-height="500"
      :row-class-name="tableRowClassName"
    >
      <el-table-column prop="code" label="股票代码" width="120" fixed="left" />
      <el-table-column prop="name" label="名称" width="120" fixed="left" />
      <el-table-column prop="price" label="最新价" width="100">
        <template #default="scope">
          <span :class="getPriceColorClass(scope.row)">
            {{ scope.row.price || '--' }}
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="change" label="涨跌幅" width="100">
        <template #default="scope">
          <span :class="getPriceColorClass(scope.row)">
            {{ formatChange(scope.row.change) }}
          </span>
        </template>
      </el-table-column>
      
      <!-- 动态列渲染 -->
      <el-table-column 
        v-for="col in dynamicColumns" 
        :key="col" 
        :prop="col" 
        :label="formatColumnLabel(col)" 
        min-width="120"
      >
        <template #default="scope">
          {{ formatDynamicValue(scope.row[col]) }}
        </template>
      </el-table-column>

      <!-- 快捷操作列 -->
      <el-table-column label="快捷操作" width="180" fixed="right">
        <template #default="scope">
          <el-button size="small" type="primary" link @click="openExternalLink(scope.row.code, 'xueqiu')">
            雪球行情
          </el-button>
          <el-button size="small" type="success" link @click="openExternalLink(scope.row.code, 'ths')">
            同花顺
          </el-button>
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  tableData: any[]
  loading?: boolean
}>()

// 排除基础列，计算出动态列
const baseColumns = ['code', 'name', 'price', 'change', 'pct_change', 'id', 'trade_date']
const dynamicColumns = computed(() => {
  if (props.tableData.length === 0) return []
  const firstRow = props.tableData[0]
  return Object.keys(firstRow).filter(key => !baseColumns.includes(key))
})

const formatColumnLabel = (key: string) => {
  const dict: Record<string, string> = {
    'pe': '市盈率(PE)',
    'pb': '市净率(PB)',
    'vol': '成交量',
    'amount': '成交额',
    'turnover_rate': '换手率',
    'amplitude': '振幅',
    'high': '最高价',
    'low': '最低价',
    'open': '开盘价',
    'close': '收盘价'
  }
  return dict[key] || key
}

const formatDynamicValue = (val: any) => {
  if (typeof val === 'number') {
    return Number.isInteger(val) ? val : val.toFixed(2)
  }
  return val
}

const getChangeValue = (row: any) => {
  return parseFloat(row.change || row.pct_change || 0)
}

const formatChange = (val: any) => {
  if (val === undefined || val === null) return '--'
  const num = parseFloat(val)
  if (isNaN(num)) return val
  return num > 0 ? `+${num}%` : `${num}%`
}

const getPriceColorClass = (row: any) => {
  const change = getChangeValue(row)
  if (change > 0) return 'color-up'
  if (change < 0) return 'color-down'
  return ''
}

const tableRowClassName = ({ row }: { row: any }) => {
  const change = getChangeValue(row)
  if (change > 0) return 'up-row'
  if (change < 0) return 'down-row'
  return ''
}

const openExternalLink = (code: string, platform: 'xueqiu' | 'ths') => {
  if (!code) return
  let prefix = 'SH'
  if (code.startsWith('6')) prefix = 'SH'
  if (code.startsWith('0') || code.startsWith('3')) prefix = 'SZ'
  
  const formattedCode = `${prefix}${code}`
  
  if (platform === 'xueqiu') {
    window.open(`https://xueqiu.com/S/${formattedCode}`, '_blank')
  } else if (platform === 'ths') {
    window.open(`http://stockpage.10jqka.com.cn/${code}/`, '_blank')
  }
}
</script>

<style scoped>
.result-card {
  margin-top: 20px;
  border-radius: 8px;
}
.card-header {
  font-weight: 600;
  font-size: 16px;
}
.color-up {
  color: #f56c6c;
  font-weight: bold;
}
.color-down {
  color: #67c23a;
  font-weight: bold;
}

:deep(.el-table .up-row) {
  background-color: rgba(245, 108, 108, 0.04) !important;
}
:deep(.el-table .down-row) {
  background-color: rgba(103, 194, 58, 0.04) !important;
}
</style>
