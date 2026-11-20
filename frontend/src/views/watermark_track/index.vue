<template>
  <section class="page" data-module="watermark_track">
    <header class="page-head">
      <div>
        <h2>水位追踪管理</h2>
        <p class="page-desc">维护流处理事件时间与处理时间的双水位追踪，为水位追踪提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记水位标记</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出水位追踪清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <section class="offset-board" data-testid="offset-board">
      <header class="offset-head">
        <div>
          <h3>双水位偏移报告</h3>
          <p class="page-desc">一个检查版本只出一份报告，保留偏移来源与受影响分区。</p>
        </div>
        <div class="page-actions">
          <label class="offset-topic">
            <span>数据主题</span>
            <input v-model="reportTopic" data-testid="offset-topic" placeholder="如 TOPI-0001" />
          </label>
          <button class="btn primary" data-testid="generate-report" type="button" @click="generateReport">
            生成偏移报告
          </button>
          <button class="btn" data-testid="refresh-reports" type="button" @click="reloadReports">
            刷新报告
          </button>
        </div>
      </header>

      <div class="offset-cards">
        <article v-for="card in reportCards" :key="card.label" class="offset-card" data-testid="offset-card">
          <span class="stat-label">{{ card.label }}</span>
          <strong class="stat-value">{{ card.value }}</strong>
        </article>
      </div>

      <table class="data-table">
        <thead>
          <tr>
            <th>当前检查版本</th>
            <th>报告编号</th>
            <th>偏移来源</th>
            <th>受影响分区</th>
            <th>来源批次</th>
            <th>版本</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="view in reportViews"
            :key="view.数据主题"
            :data-testid="`offset-view-${view.数据主题}`"
            :data-current-version="String(view.当前检查版本 ?? '')"
          >
            <td>{{ view.数据主题 }} / {{ view.当前检查版本 ?? '—' }}</td>
            <td>{{ view.当前报告编号 ?? '—' }}</td>
            <td :data-testid="`offset-source-${view.数据主题}`">{{ view.当前偏移来源 ?? '—' }}</td>
            <td :data-testid="`offset-partitions-${view.数据主题}`">
              {{ view.当前受影响分区.length ? view.当前受影响分区.join('、') : '无' }}
            </td>
            <td>{{ view.当前来源批次 ?? '—' }}</td>
            <td>
              共 {{ view.版本数 }} 版，完整 {{ view.完整版本数 }}，未完成 {{ view.未完成版本数 }}
            </td>
            <td class="row-actions">
              <button
                class="link"
                type="button"
                :disabled="!view.当前报告"
                :data-testid="`offset-detail-${view.数据主题}`"
                @click="openReport(view.当前报告)"
              >
                查看报告
              </button>
            </td>
          </tr>
          <tr v-if="!reportViews.length">
            <td colspan="7" class="empty-state">暂无水位偏移报告，指定数据主题后可生成第一份</td>
          </tr>
        </tbody>
      </table>

      <div v-if="selectedReport" class="offset-detail" data-testid="offset-detail">
        <header class="offset-detail-head">
          <h4>报告 {{ selectedReport.报告编号 }} · 检查版本 {{ selectedReport.检查版本 }}</h4>
          <span :class="selectedReport.状态 === '完整' ? 'tag-ok' : 'tag-warn'">{{ selectedReport.状态 }}</span>
          <button class="link" type="button" data-testid="offset-detail-close" @click="selectedReport = null">收起</button>
        </header>
        <dl>
          <div><dt>偏移来源</dt><dd data-testid="report-source">{{ selectedReport.偏移来源 }}</dd></div>
          <div><dt>受影响分区</dt><dd>{{ selectedReport.受影响分区.join('、') || '无' }}</dd></div>
          <div><dt>来源批次</dt><dd>{{ selectedReport.来源批次 ?? '—' }}</dd></div>
          <div><dt>保留报告</dt><dd>{{ selectedReport.保留报告 ?? '—' }}</dd></div>
          <div><dt>缺失分区</dt><dd>{{ selectedReport.缺失分区.join('、') || '无' }}</dd></div>
          <div><dt>中断期偏移来源</dt><dd>{{ selectedReport.中断期偏移来源 ?? '—' }}</dd></div>
        </dl>
        <p class="offset-note" data-testid="report-note">{{ selectedReport.来源判定 }}</p>
        <table class="data-table">
          <thead>
            <tr>
              <th>分区</th>
              <th>事件时间水位</th>
              <th>事件时间偏移(秒)</th>
              <th>处理时间水位</th>
              <th>处理时间偏移(秒)</th>
              <th>延迟记录数</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in selectedReport.分区明细" :key="item.分区编号" :data-testid="`report-partition-${item.分区编号}`">
              <td>{{ item.分区编号 }}</td>
              <td>{{ item.事件时间水位 ?? '—' }}</td>
              <td>{{ item.事件时间偏移秒 ?? '—' }}</td>
              <td>{{ item.处理时间水位 ?? '—' }}</td>
              <td>{{ item.处理时间偏移秒 ?? '—' }}</td>
              <td>{{ item.延迟记录数 ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
        <div class="page-actions">
          <button
            v-if="selectedReport.状态 !== '完整'"
            class="btn primary"
            data-testid="retry-report"
            type="button"
            @click="retryReport(selectedReport)"
          >
            重试补齐缺失分区
          </button>
          <button
            class="btn"
            data-testid="recompute-report"
            type="button"
            @click="recomputeReport(selectedReport)"
          >
            重算全部分区
          </button>
        </div>
      </div>
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
          <td :colspan="columns.length + 1" class="empty-state">暂无水位追踪数据，可先登记水位标记</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记水位标记</h3>
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
      <span>共 {{ total }} 条水位追踪记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type Report = Record<string, any>
type ReportView = {
  数据主题: string
  主题名称: string
  当前检查版本: string | null
  当前报告编号: string | null
  当前偏移来源: string | null
  当前受影响分区: string[]
  当前来源批次: string | null
  版本数: number
  完整版本数: number
  未完成版本数: number
  当前报告: Report | null
}

const ENDPOINT = '/api/watermark_track'
const REPORT_ENDPOINT = `${ENDPOINT}/reports`
const columns = ["标记编号", "数据主题", "分区编号", "事件时间水位", "处理时间水位", "延迟记录数", "消费偏移", "检查时间"]
const actions = ["标记正常", "标记滞后", "停用追踪"]
const statuses = ["草稿", "正常", "滞后", "已停用"]
const metricNames = ["水位标记总数", "正常水位", "滞后水位"]

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

const reportTopic = ref('TOPI-0001')
const reportCards = ref<{ label: string; value: number }[]>([])
const reportViews = ref<ReportView[]>([])
const selectedReport = ref<Report | null>(null)

async function reloadReports() {
  errorMessage.value = ''
  try {
    const response = await request(`${REPORT_ENDPOINT}/overview`)
    if (!response.ok) throw new Error('水位偏移报告读取失败')
    const payload = await response.json()
    reportCards.value = payload.cards ?? []
    reportViews.value = payload.views ?? []
    // 列表与详情读同一份报告：已选中的报告按编号回读，保持一致。
    if (selectedReport.value) {
      selectedReport.value = await fetchReport(selectedReport.value.id)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '水位偏移报告读取失败'
  }
}

async function fetchReport(id: number): Promise<Report | null> {
  const response = await request(`${REPORT_ENDPOINT}/${id}`)
  if (!response.ok) return null
  return (await response.json()) as Report
}

function openReport(report: Report | null) {
  selectedReport.value = report
}

async function generateReport() {
  errorMessage.value = ''
  const topic = reportTopic.value.trim()
  if (!topic) {
    errorMessage.value = '请先填写数据主题'
    return
  }
  try {
    const response = await request(`${REPORT_ENDPOINT}/generate`, {
      method: 'POST',
      body: JSON.stringify({ topic }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message || '偏移报告生成失败')
    reportTopic.value = topic
    await reloadReports()
    if (payload.entry?.id) selectedReport.value = await fetchReport(payload.entry.id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '偏移报告生成失败'
  }
}

async function retryReport(report: Report) {
  errorMessage.value = ''
  try {
    const response = await request(`${REPORT_ENDPOINT}/${report.id}/retry`, { method: 'POST' })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message || '补齐缺失分区失败')
    await reloadReports()
    selectedReport.value = await fetchReport(report.id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补齐缺失分区失败'
  }
}

async function recomputeReport(report: Report) {
  errorMessage.value = ''
  try {
    const response = await request(`${REPORT_ENDPOINT}/generate`, {
      method: 'POST',
      body: JSON.stringify({ topic: report.数据主题, check_version: report.检查版本, scope: '全部' }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message || '重算全部分区失败')
    await reloadReports()
    selectedReport.value = payload.entry?.id ? await fetchReport(payload.entry.id) : null
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重算全部分区失败'
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
    link.download = 'watermark_track.csv'
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
      throw new Error('水位追踪动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '水位追踪操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('水位标记列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '水位追踪列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void reloadReports()
})
</script>
