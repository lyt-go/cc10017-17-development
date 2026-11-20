import { expect, test } from '@playwright/test'

/**
 * 双水位偏移报告与分区级告警的跨模块交接。
 *
 * 依赖后端 seed 中 TOPI-0001 的分区样本：
 *  - 分区0 事件时间前进 300s / 处理时间前进 120s，两条水位都不判滞后
 *  - 分区1 事件时间停滞 0s / 处理时间前进 120s，只有事件时间判滞后
 *  - 分区2 事件时间前进 180s / 处理时间回退 60s（同一分区内反向移动），只有处理时间判滞后
 * 因此按事件时间受影响分区为 1，按处理时间为 2；报告二选一，取受影响分区相等时滞后合计更大的处理时间。
 */

const TOPIC = 'TOPI-0001'

test('水位偏移报告：中断保留、重试补齐、来源与受影响分区落报告', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => {
    if (r.url().includes('/api/') && r.status() >= 400) browserErrors.push(`${r.status()} ${r.url()}`)
  })

  // 先在后端制造一次中断：只覆盖第一个分区。
  const partial = await page.request.post('/api/watermark_track/reports/generate', {
    data: { topic: TOPIC, check_version: 'E2E-1', interrupt_after: 1 },
  })
  expect(partial.ok()).toBeTruthy()
  const partialBody = await partial.json()
  expect(partialBody.entry['状态']).toBe('不完整')
  expect(partialBody.entry['缺失分区']).toEqual(['1', '2'])

  await page.goto('/watermark_track')
  const board = page.getByTestId('offset-board')
  await expect(board).toBeVisible()

  // 中断版本不是当前业务版本，看板仍指向上一份完整报告（此处没有，因此卡片为 0 版本）。
  const view = page.getByTestId(`offset-view-${TOPIC}`)
  await expect(view).toBeVisible()

  // 重试补齐：只补缺失分区，补齐后成为当前业务版本。
  const retried = await page.request.post(`/api/watermark_track/reports/${partialBody.entry.id}/retry`)
  expect((await retried.json()).entry['状态']).toBe('完整')

  await page.getByTestId('refresh-reports').click()
  await expect(page.getByTestId(`offset-source-${TOPIC}`)).toHaveText('处理时间')
  await expect(page.getByTestId(`offset-partitions-${TOPIC}`)).toHaveText('2')

  await page.getByTestId(`offset-detail-${TOPIC}`).click()
  await expect(page.getByTestId('offset-detail')).toBeVisible()
  await expect(page.getByTestId('report-source')).toHaveText('处理时间')
  await expect(page.getByTestId('report-partition-0')).toBeVisible()
  await expect(page.getByTestId('report-partition-1')).toBeVisible()
  await expect(page.getByTestId('report-partition-2')).toBeVisible()

  // 重算全部分区另起版本，原报告保持完整。
  const recompute = await page.request.post('/api/watermark_track/reports/generate', {
    data: { topic: TOPIC, check_version: 'E2E-1', scope: '全部' },
  })
  const recomputeBody = await recompute.json()
  expect(recomputeBody.entry['替代报告']).toBe(partialBody.entry['报告编号'])
  const original = await page.request.get(`/api/watermark_track/reports/${partialBody.entry.id}`)
  expect((await original.json())['状态']).toBe('完整')

  expect(browserErrors).toEqual([])
})

test('流式质量：按检查版本核对偏移报告并给出分区告警', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => {
    if (r.url().includes('/api/') && r.status() >= 400) browserErrors.push(`${r.status()} ${r.url()}`)
  })

  // 先确保 TOPI-0001 有当前业务版本报告，再复用种子里的规则1（阈值50）触发核对，
  // 避免往 stream_quality 列表插行、干扰既有 CRUD 用例的行数断言。
  await page.request.post('/api/watermark_track/reports/generate', { data: { topic: TOPIC } })
  await page.request.post('/api/stream_quality/1/actions', { data: { action: '标记生效' } })

  await page.goto('/stream_quality')
  await expect(page.getByTestId('alert-board')).toBeVisible()
  await expect(page.getByTestId('alert-2')).toBeVisible()
  await expect(page.getByTestId('alert-0')).toHaveCount(0)
  await expect(page.getByTestId('alert-1')).toHaveCount(0)

  const overview = await page.request.get(`/api/watermark_track/reports?topic=${TOPIC}`)
  const current = (await overview.json()).items.find((item: any) => item['状态'] === '完整')
  await page.getByTestId('alert-version').fill(String(current['检查版本']))
  await page.getByTestId('check-report').click()

  await expect(page.getByTestId('reconciliation')).toBeVisible()
  await expect(page.getByTestId('recon-report')).toHaveText(current['报告编号'])
  await expect(page.getByTestId('recon-source')).toHaveText(current['偏移来源'])
  await expect(page.getByTestId('recon-partitions')).toHaveText(current['受影响分区'].join('、'))
  await expect(page.getByTestId('recon-batch')).toHaveText(current['来源批次'])

  expect(browserErrors).toEqual([])
})
