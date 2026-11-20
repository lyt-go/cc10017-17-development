<template>
  <section class="page" data-module="stream_quality">
    <header class="page-head">
      <div>
        <h2>流式质量管理</h2>
        <p class="page-desc">维护流式数据实时完整性、延迟与分布的质量监控规则，为流式质量提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记质量规则</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出流式质量清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <section class="offset-board" data-testid="alert-board">
      <header class="offset-head">
        <div>
          <h3>分区级质量告警</h3>
          <p class="page-desc">核对水位追踪的双水位偏移报告：按同一检查版本列出受影响分区与偏移来源。</p>
        </div>
        <div class="page-actions">
          <label class="offset-topic">
            <span>监控主题</span>
            <input v-model="alertTopic" data-testid="alert-topic" placeholder="如 TOPI-0001" />
          </label>
          <label class="offset-topic">
            <span>检查版本</span>
            <input v-model="alertVersion" data-testid="alert-version" placeholder="留空取当前版本" />
          </label>
          <button class="btn primary" data-testid="check-report" type="button" @click="checkReport">
            核对偏移报告
          </button>
        </div>
      </header>

      <div v-if="reconciliation" class="offset-detail" data-testid="reconciliation">
        <dl>
          <div><dt>报告编号</dt><dd data-testid="recon-report">{{ reconciliation.报告.报告编号 }}</dd></div>
          <div><dt>检查版本</dt><dd>{{ reconciliation.报告.检查版本 }}</dd></div>
          <div><dt>偏移来源</dt><dd data-testid="recon-source">{{ reconciliation.偏移来源 }}</dd></div>
          <div><dt>受影响分区</dt><dd data-testid="recon-partitions">{{ reconciliation.受影响分区.join('、') || '无' }}</dd></div>
          <div><dt>来源批次</dt><dd data-testid="recon-batch">{{ reconciliation.来源批次 ?? '—' }}</dd></div>
          <div><dt>核对结果</dt><dd>{{ reconciliation.检查结果 ? (reconciliation.检查结果.passed ? '通过' : '告警') : '未核对' }}</dd></div>
        </dl>
      </div>

      <table class="data-table">
        <thead>
          <tr>
            <th>告警编号</th>
            <th>规则编号</th>
            <th>监控主题</th>
            <th>分区编号</th>
            <th>偏移来源</th>
            <th>报告编号</th>
            <th>检查版本</th>
            <th>来源批次</th>
            <th>状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="alert in alerts" :key="String(alert.id)" :data-testid="`alert-${alert.分区编号}`">
            <td>{{ alert.告警编号 }}</td>
            <td>{{ alert.规则编号 }}</td>
            <td>{{ alert.监控主题 }}</td>
            <td>{{ alert.分区编号 }}</td>
            <td>{{ alert.偏移来源 }}</td>
            <td>{{ alert.报告编号 }}</td>
            <td>{{ alert.检查版本 }}</td>
            <td>{{ alert.来源批次 }}</td>
            <td>{{ alert.状态 }}</td>
          </tr>
          <tr v-if="!alerts.length">
            <td colspan="9" class="empty-state">暂无分区级告警，质量规则生效后按偏移报告核对生成</td>
          </tr>
        </tbody>
      </table>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无流式质量数据，可先登记质量规则</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记质量规则</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条流式质量记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/stream_quality'
const columns = ["规则编号", "监控主题", "校验类型", "告警阈值", "采样比例", "检测窗口", "阻断动作", "规则版本"]
const actions = ["提交校验", "标记生效", "停用规则"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["质量规则总数", "待校验规则", "已生效规则"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

const alertTopic = ref('TOPI-0001')
const alertVersion = ref('')
const alerts = ref<Record<string, any>[]>([])
const reconciliation = ref<Record<string, any> | null>(null)

async function reloadAlerts() {
  try {
    const query = new URLSearchParams({ topic: alertTopic.value.trim() }).toString()
    const response = await request(`${ENDPOINT}/alerts?${query}`)
    if (!response.ok) throw new Error('分区告警读取失败')
    alerts.value = (await response.json()).items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区告警读取失败'
  }
}

async function checkReport() {
  errorMessage.value = ''
  const topic = alertTopic.value.trim()
  if (!topic) {
    errorMessage.value = '请先填写监控主题'
    return
  }
  const version = alertVersion.value.trim()
  if (!version) {
    errorMessage.value = '请填写检查版本，或先在水位追踪页生成报告'
    return
  }
  try {
    const response = await request(`${ENDPOINT}/reports/${encodeURIComponent(topic)}/${encodeURIComponent(version)}`)
    if (!response.ok) throw new Error('该检查版本没有对应的水位偏移报告')
    reconciliation.value = await response.json()
    await reloadAlerts()
  } catch (error) {
    reconciliation.value = null
    errorMessage.value = error instanceof Error ? error.message : '偏移报告核对失败'
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'stream_quality.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('流式质量动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '流式质量操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('质量规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '流式质量列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void reloadAlerts()
})
</script>
