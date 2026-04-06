<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'

const AUTO_REFRESH_MS = 2000
const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '')
const MULTIPART_SESSION_PREFIX = 'nas.multipart.upload'
const multipartThresholdRaw = Number(import.meta.env.VITE_MULTIPART_THRESHOLD_BYTES || 32 * 1024 * 1024)
const MULTIPART_THRESHOLD_BYTES = Number.isFinite(multipartThresholdRaw)
  ? Math.max(1, multipartThresholdRaw)
  : 32 * 1024 * 1024
const PIPELINE_STAGE_ORDER = [
  { id: 'extract', label: 'Extract' },
  { id: 'transform', label: 'Transform' },
  { id: 'validate', label: 'Validate' },
  { id: 'final', label: 'Finalize' },
]

const uploadFile = ref(null)
const uploadMeta = ref('')
const searchQuery = ref('')
const searchMeta = ref('')
const searchItems = ref([])
const ingestItems = ref([])
const auditRuns = ref([])
const dashboardRefreshInFlight = ref(false)
const lastUpdatedLabel = ref('Waiting for first sync')

let refreshTimer = null

async function apiFetch(path, init = {}) {
  const res = await fetch(`${API_BASE_URL}${path}`, init)
  if (!res.ok) {
    const body = await res.text()
    throw new Error(body || `${res.status} ${res.statusText}`)
  }
  return res
}

async function fetchJson(path) {
  const res = await apiFetch(path)
  return await res.json()
}

function uploadSessionStorageKey(file) {
  return `${MULTIPART_SESSION_PREFIX}:${file.name}:${file.size}:${file.lastModified}`
}

function loadSavedUploadSession(file) {
  try {
    const raw = window.localStorage.getItem(uploadSessionStorageKey(file))
    return raw ? JSON.parse(raw) : null
  } catch {
    return null
  }
}

function saveUploadSession(file, session) {
  window.localStorage.setItem(uploadSessionStorageKey(file), JSON.stringify(session))
}

function clearUploadSession(file) {
  window.localStorage.removeItem(uploadSessionStorageKey(file))
}

function touchRefreshLabel() {
  lastUpdatedLabel.value = new Date().toLocaleTimeString()
}

function statusClass(status) {
  const normalized = String(status ?? '').toLowerCase()
  if (normalized === 'ok' || normalized === 'completed') return 'pill ok'
  if (normalized === 'failed' || normalized === 'error' || normalized === 'interrupted') return 'pill fail'
  if (normalized === 'running' || normalized === 'queued' || normalized === 'pausing') return 'pill active'
  return 'pill'
}

function loadStatusClass(status) {
  const normalized = String(status ?? '').toLowerCase()
  if (normalized === 'completed') return 'pill ok'
  if (normalized === 'failed') return 'pill fail'
  if (normalized === 'pending') return 'pill active'
  return 'pill'
}

function formatStageLabel(stage) {
  const raw = String(stage ?? '').trim()
  if (!raw) return '-'
  const normalized = raw.toLowerCase()
  const labels = {
    extract: 'Extract',
    clean: 'Transform',
    transform: 'Transform',
    validated: 'Validated',
    validate: 'Validate',
    final: 'Finalize',
    starting: 'Starting',
    starting_pipeline: 'Starting Pipeline',
    initializing_spark: 'Initializing Spark',
    spark_ready: 'Spark Ready',
    loading_db: 'Loading DB',
    resuming: 'Resuming',
    writing_output: 'Writing Output',
    completed: 'Completed',
    failed: 'Failed',
    failing: 'Failing',
    interrupted: 'Interrupted',
    pausing: 'Pausing',
    paused: 'Paused',
  }
  if (labels[normalized]) return labels[normalized]
  if (normalized.startsWith('spark_stage_')) return raw.replaceAll('_', ' ')
  return raw.replaceAll('_', ' ')
}

function mapStageToSection(stage) {
  const normalized = String(stage ?? '').trim().toLowerCase()
  if (!normalized) return null
  if (['extract', 'starting', 'starting_pipeline', 'initializing_spark', 'spark_ready', 'resuming'].includes(normalized)) {
    return 'extract'
  }
  if (['clean', 'transform'].includes(normalized)) return 'transform'
  if (['validated', 'validate', 'validating'].includes(normalized)) return 'validate'
  if (['final', 'writing_output', 'loading_db', 'completed'].includes(normalized)) return 'final'
  return null
}

function mapPctToSection(progressPct) {
  if (!Number.isFinite(progressPct)) return 'extract'
  if (progressPct >= 88) return 'final'
  if (progressPct >= 70) return 'validate'
  if (progressPct >= 45) return 'transform'
  return 'extract'
}

function formatDate(value) {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
}

function progressNumber(value, status) {
  const numeric = Number(value)
  if (Number.isFinite(numeric)) return Math.max(0, Math.min(100, numeric))
  return ['completed', 'failed', 'interrupted'].includes(String(status ?? '').toLowerCase()) ? 100 : 0
}

function pipelineSections(run) {
  const status = String(run.status ?? '').toLowerCase()
  const progressPct = Number(run.progress_pct)
  const currentSection = mapStageToSection(run.progress_stage) || mapPctToSection(progressPct)
  const currentIdx = Math.max(0, PIPELINE_STAGE_ORDER.findIndex((item) => item.id === currentSection))

  return PIPELINE_STAGE_ORDER.map((item, idx) => {
    let state = 'pending'
    if (status === 'completed') {
      state = 'done'
    } else if (status === 'paused' || status === 'pausing') {
      if (idx < currentIdx) state = 'done'
      else if (idx === currentIdx) state = 'paused'
    } else if (status === 'failed' || status === 'interrupted') {
      if (idx < currentIdx) state = 'done'
      else if (idx === currentIdx) state = 'failed'
    } else if (status === 'running' || status === 'queued') {
      if (idx < currentIdx) state = 'done'
      else if (idx === currentIdx) state = 'running'
    }
    return { ...item, state }
  })
}

const pipelineRows = computed(() => ingestItems.value.map((run) => {
  const pct = progressNumber(run.progress_pct, run.status)
  return {
    ...run,
    progressValue: pct,
    progressPct: `${pct}%`,
    progressStage: formatStageLabel(run.progress_stage),
    lastLogLine: run.last_log_line || '-',
    sections: pipelineSections(run),
  }
}))

const dbLoadRows = computed(() => ingestItems.value.map((run) => {
  const rawLoadStatus = run.load_status || (run.load_to_db ? 'pending' : 'skipped')
  const loadStatus = String(rawLoadStatus).toLowerCase()
  return {
    ...run,
    loadStatus,
    loadText: run.load_to_db ? loadStatus : 'disabled',
    waitingMsg: run.load_to_db && loadStatus === 'pending' ? 'Waiting for pipeline completion' : '-',
  }
}))

const dashboardStats = computed(() => {
  const jobs = ingestItems.value
  const audits = auditRuns.value
  const active = jobs.filter((item) => ['queued', 'running', 'pausing'].includes(String(item.status ?? '').toLowerCase())).length
  const completed = jobs.filter((item) => String(item.status ?? '').toLowerCase() === 'completed').length
  const failed = jobs.filter((item) => ['failed', 'interrupted'].includes(String(item.status ?? '').toLowerCase())).length
  const recordsSuccess = audits.reduce((sum, item) => sum + Number(item.records_success || 0), 0)
  const recordsFailed = audits.reduce((sum, item) => sum + Number(item.records_failed || 0), 0)
  const totalRecords = recordsSuccess + recordsFailed
  const successRate = totalRecords > 0 ? `${Math.round((recordsSuccess / totalRecords) * 100)}%` : 'n/a'

  return [
    { label: 'Active Jobs', value: String(active), tone: active > 0 ? 'active' : 'neutral', detail: 'Queued, running or pausing now' },
    { label: 'Completed Runs', value: String(completed), tone: 'ok', detail: 'Finished ingest pipeline jobs' },
    { label: 'Failed Runs', value: String(failed), tone: failed > 0 ? 'fail' : 'neutral', detail: 'Requires retry or operator review' },
    { label: 'Validation Yield', value: successRate, tone: recordsFailed > 0 ? 'active' : 'ok', detail: `${recordsSuccess} passed · ${recordsFailed} failed` },
  ]
})

const urgentJobs = computed(() =>
  ingestItems.value
    .filter((item) => ['failed', 'interrupted', 'pausing', 'running', 'queued'].includes(String(item.status ?? '').toLowerCase()))
    .slice(0, 4),
)

async function loadIngestJobs() {
  const payload = await fetchJson('/api/v1/ingest/jobs?limit=20')
  ingestItems.value = Array.isArray(payload.items) ? payload.items : []
}

async function loadRuns() {
  const payload = await fetchJson('/api/v1/jobs?limit=20')
  auditRuns.value = Array.isArray(payload.items) ? payload.items : []
}

async function refreshDashboard() {
  if (dashboardRefreshInFlight.value) return
  dashboardRefreshInFlight.value = true
  try {
    await Promise.all([loadIngestJobs(), loadRuns()])
    touchRefreshLabel()
  } catch (error) {
    uploadMeta.value = `Dashboard refresh failed: ${error.message}`
  } finally {
    dashboardRefreshInFlight.value = false
  }
}

async function submitSearch() {
  const q = searchQuery.value.trim()
  if (q.length < 2) return
  searchMeta.value = 'Searching address graph...'
  searchItems.value = []
  try {
    const payload = await fetchJson(`/api/v1/search/autocomplete?q=${encodeURIComponent(q)}&size=10`)
    searchItems.value = Array.isArray(payload.items) ? payload.items : []
    searchMeta.value = `${payload.count || searchItems.value.length} candidate record(s) found`
  } catch (error) {
    searchMeta.value = `Search failed: ${error.message}`
  }
}

function onFileChange(event) {
  uploadFile.value = event.target?.files?.[0] || null
}

async function submitUpload() {
  if (!uploadFile.value) {
    uploadMeta.value = 'Choose a source file before dispatching an ingest run.'
    return
  }

  try {
    const file = uploadFile.value
    const savedSession = loadSavedUploadSession(file)
    const useMultipart = Boolean(savedSession?.session_id) || file.size > MULTIPART_THRESHOLD_BYTES
    const payload = useMultipart ? await uploadFileInParts(file, savedSession) : await uploadFileDirect(file)
    uploadMeta.value = useMultipart
      ? `Multipart run queued: ${payload.job_id}`
      : `Run queued: ${payload.job_id}`
    uploadFile.value = null
    const input = document.getElementById('upload-file')
    if (input) input.value = ''
    await refreshDashboard()
  } catch (error) {
    uploadMeta.value = `Upload failed: ${error.message}. Re-run with the same file to resume.`
  }
}

async function uploadFileDirect(file) {
  uploadMeta.value = `Uploading ${file.name} via direct form upload...`
  const form = new FormData()
  form.append('file', file, file.name)
  form.append('auto_start', 'true')
  form.append('load_to_db', 'true')
  form.append('resume_from_checkpoint', 'true')
  form.append('resume_failed_only', 'true')
  const res = await apiFetch('/api/v1/ingest/upload', {
    method: 'POST',
    body: form,
  })
  clearUploadSession(file)
  return await res.json()
}

async function uploadFileInParts(file, initialSession = null) {
  let session = initialSession
  if (session?.session_id) {
    try {
      const status = await fetchJson(`/api/v1/ingest/uploads/multipart/${session.session_id}`)
      if (String(status.status || '').toLowerCase() === 'completed' && status.job_id) {
        clearUploadSession(file)
        return { job_id: status.job_id, status: 'queued' }
      }
      if (String(status.status || '').toLowerCase() !== 'aborted') {
        session = { ...session, ...status }
      } else {
        clearUploadSession(file)
        session = null
      }
    } catch {
      clearUploadSession(file)
      session = null
    }
  }

  if (!session?.session_id) {
    uploadMeta.value = `Preparing multipart upload for ${file.name}...`
    const res = await apiFetch('/api/v1/ingest/uploads/multipart/initiate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        file_name: file.name,
        content_type: file.type || 'application/octet-stream',
        content_bytes: file.size,
        auto_start: true,
        load_to_db: true,
        resume_from_checkpoint: true,
        resume_failed_only: true,
      }),
    })
    session = await res.json()
    saveUploadSession(file, session)
  }

  const status = await fetchJson(`/api/v1/ingest/uploads/multipart/${session.session_id}`)
  const partSize = Number(session.part_size || status.part_size)
  const totalParts = Math.ceil(file.size / partSize)
  const uploadedParts = new Map(
    (Array.isArray(status.uploaded_parts) ? status.uploaded_parts : []).map((item) => [Number(item.part_number), item.etag]),
  )

  for (let partNumber = 1; partNumber <= totalParts; partNumber += 1) {
    if (uploadedParts.has(partNumber)) {
      uploadMeta.value = `Resuming upload: part ${partNumber}/${totalParts} already stored`
      continue
    }

    uploadMeta.value = `Uploading part ${partNumber}/${totalParts}...`
    const urlRes = await apiFetch(`/api/v1/ingest/uploads/multipart/${session.session_id}/part-url`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ part_number: partNumber }),
    })
    const { url } = await urlRes.json()
    const start = (partNumber - 1) * partSize
    const end = Math.min(file.size, start + partSize)
    const body = file.slice(start, end)
    const uploadRes = await fetch(url, {
      method: 'PUT',
      body,
      headers: { 'Content-Type': file.type || 'application/octet-stream' },
    })
    if (!uploadRes.ok) {
      const bodyText = await uploadRes.text()
      throw new Error(bodyText || `part ${partNumber} upload failed`)
    }
    const etag = (uploadRes.headers.get('etag') || uploadRes.headers.get('ETag') || '').replaceAll('"', '')
    if (!etag) {
      throw new Error(`part ${partNumber} upload completed without ETag`)
    }
    uploadedParts.set(partNumber, etag)
  }

  uploadMeta.value = 'Finalizing upload and queuing ingest job...'
  const completeRes = await apiFetch(`/api/v1/ingest/uploads/multipart/${session.session_id}/complete`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      parts: Array.from(uploadedParts.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([part_number, etag]) => ({ part_number, etag })),
    }),
  })
  const payload = await completeRes.json()
  clearUploadSession(file)
  return payload
}

async function pauseJob(jobId) {
  uploadMeta.value = `Pausing ${jobId}...`
  try {
    await apiFetch(`/api/v1/ingest/jobs/${encodeURIComponent(jobId)}/pause`, { method: 'POST' })
    uploadMeta.value = `Pause requested for ${jobId}`
    await refreshDashboard()
  } catch (error) {
    uploadMeta.value = `Pause failed: ${error.message}`
  }
}

async function resumeJob(jobId, isUploaded) {
  uploadMeta.value = `${isUploaded ? 'Starting' : 'Resuming'} ${jobId}...`
  try {
    await apiFetch(`/api/v1/ingest/jobs/${encodeURIComponent(jobId)}/start`, { method: 'POST' })
    uploadMeta.value = `Resume queued for ${jobId}`
    await refreshDashboard()
  } catch (error) {
    uploadMeta.value = `Resume failed: ${error.message}`
  }
}

function onVisibilityChange() {
  if (!document.hidden) {
    void refreshDashboard()
  }
}

onMounted(async () => {
  await refreshDashboard()
  refreshTimer = setInterval(() => {
    if (document.hidden) return
    void refreshDashboard()
  }, AUTO_REFRESH_MS)
  document.addEventListener('visibilitychange', onVisibilityChange)
})

onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  document.removeEventListener('visibilitychange', onVisibilityChange)
})
</script>

<template>
  <div class="app-shell">
    <header class="hero">
      <div class="hero-grid">
        <div class="hero-copy">
          <p class="eyebrow">NAS Operations Console</p>
          <h1>Ingestion, checkpoint recovery, and audit visibility in one place.</h1>
          <p class="hero-text">
            Run uploads from agencies, monitor worker state on Redis Streams, and track what made it through the pipeline versus
            what needs intervention.
          </p>
          <div class="hero-actions">
            <button type="button" @click="refreshDashboard">Sync Control Room</button>
            <span class="meta-chip">Auto refresh {{ AUTO_REFRESH_MS / 1000 }}s</span>
            <span class="meta-chip mono">API {{ API_BASE_URL }}</span>
          </div>
        </div>

        <aside class="hero-aside">
          <div class="signal-card">
            <p class="signal-label">Queue Mode</p>
            <p class="signal-value">Redis Stream Worker</p>
            <p class="signal-copy">Consumer-group based execution with reclaim for abandoned pending messages.</p>
          </div>
          <div class="signal-card">
            <p class="signal-label">Last Sync</p>
            <p class="signal-value">{{ lastUpdatedLabel }}</p>
            <p class="signal-copy">The dashboard refreshes automatically when the tab is visible.</p>
          </div>
        </aside>
      </div>
    </header>

    <main class="page-shell">
      <section class="kpi-grid">
        <article v-for="item in dashboardStats" :key="item.label" :class="`kpi-card tone-${item.tone}`">
          <p class="kpi-label">{{ item.label }}</p>
          <p class="kpi-value">{{ item.value }}</p>
          <p class="kpi-detail">{{ item.detail }}</p>
        </article>
      </section>

      <section class="split-grid split-grid-primary">
        <article class="panel panel-accent">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Dispatch</p>
              <h2>Upload and Run</h2>
            </div>
          </div>
          <p class="panel-copy">Send CSV, JSON, or Excel source files into the ingest queue and start the ETL path immediately.</p>
          <div class="upload-stack">
            <input id="upload-file" type="file" accept=".csv,.json,.jsonl,.ndjson,.xlsx,.xls" @change="onFileChange" />
            <button type="button" @click="submitUpload">Queue Ingest Run</button>
          </div>
          <p class="panel-meta">{{ uploadMeta || 'Files are stored in MinIO, then handed to the worker through Redis Streams.' }}</p>
        </article>

        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Lookup</p>
              <h2>Search Address Graph</h2>
            </div>
          </div>
          <form class="search-row" @submit.prevent="submitSearch">
            <input v-model="searchQuery" type="text" placeholder="Try postcode, naskod, locality or full address" minlength="2" required />
            <button type="submit">Search</button>
          </form>
          <p class="panel-meta">{{ searchMeta || 'Search returns the best normalized address candidates from the backend API.' }}</p>
          <div class="table-canvas compact">
            <div v-if="!searchItems.length" class="table-empty">No search result yet.</div>
            <div v-else class="table-wrap">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>Address</th>
                    <th>NASKod</th>
                    <th>Postcode</th>
                    <th>Locality</th>
                    <th>District</th>
                    <th>State</th>
                    <th>Score</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="item in searchItems" :key="`${item.record_id || ''}-${item.naskod || ''}`">
                    <td>{{ item.address_clean || '-' }}</td>
                    <td class="mono nowrap">{{ item.naskod || 'n/a' }}</td>
                    <td class="nowrap">{{ item.postcode || '-' }}</td>
                    <td>{{ item.locality_name || '-' }}</td>
                    <td>{{ item.district_name || '-' }}</td>
                    <td>{{ item.state_name || '-' }}</td>
                    <td class="nowrap">{{ item.confidence_score || 'n/a' }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>
      </section>

      <section class="split-grid split-grid-monitor">
        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Operators</p>
              <h2>Ingest Jobs</h2>
            </div>
            <button class="secondary" type="button" @click="refreshDashboard">Refresh</button>
          </div>
          <div class="table-canvas">
            <div v-if="!ingestItems.length" class="table-empty">No ingest jobs found.</div>
            <div v-else class="table-wrap">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>Job ID</th>
                    <th>Status</th>
                    <th>File</th>
                    <th>Source</th>
                    <th>Progress</th>
                    <th>Created</th>
                    <th>Log / Error</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="run in ingestItems" :key="run.job_id">
                    <td class="mono">{{ run.job_id }}</td>
                    <td><span :class="statusClass(run.status)">{{ run.status || 'unknown' }}</span></td>
                    <td>{{ run.file_name || '-' }}</td>
                    <td class="nowrap">{{ run.source_type || '-' }}</td>
                    <td class="nowrap">
                      {{ progressNumber(run.progress_pct, run.status) }}% · {{ formatStageLabel(run.progress_stage) }}
                    </td>
                    <td class="mono">{{ formatDate(run.created_at) }}</td>
                    <td class="mono">
                      {{ run.log_path || '-' }}
                      <div v-if="run.error" class="error-line">Error: {{ run.error }}</div>
                    </td>
                    <td>
                      <div class="action-row">
                        <button
                          v-if="String(run.status || '').toLowerCase() === 'running'"
                          class="mini-btn secondary"
                          type="button"
                          @click="pauseJob(run.job_id)"
                        >
                          Pause
                        </button>
                        <button
                          v-if="['failed','interrupted','uploaded','paused'].includes(String(run.status || '').toLowerCase())"
                          class="mini-btn secondary"
                          type="button"
                          @click="resumeJob(run.job_id, String(run.status || '').toLowerCase() === 'uploaded')"
                        >
                          {{ String(run.status || '').toLowerCase() === 'uploaded' ? 'Start' : 'Resume' }}
                        </button>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <article class="panel panel-rail">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Attention</p>
              <h2>Operational Pulse</h2>
            </div>
          </div>
          <div class="run-rail">
            <div v-if="!urgentJobs.length" class="empty-state">No urgent jobs. The queue is clear.</div>
            <article v-for="run in urgentJobs" :key="`urgent-${run.job_id}`" class="rail-card">
              <div class="rail-card-head">
                <span class="mono">{{ run.job_id }}</span>
                <span :class="statusClass(run.status)">{{ run.status || 'unknown' }}</span>
              </div>
              <p class="rail-file">{{ run.file_name || 'Unnamed file' }}</p>
              <p class="rail-copy">{{ formatStageLabel(run.progress_stage) }} · {{ progressNumber(run.progress_pct, run.status) }}%</p>
            </article>
          </div>
        </article>
      </section>

      <section class="panel">
        <div class="panel-header">
          <div>
            <p class="panel-kicker">Checkpoint View</p>
            <h2>Pipeline Stage Monitor</h2>
          </div>
        </div>
        <div v-if="!pipelineRows.length" class="empty-state">No pipeline activity yet.</div>
        <div v-else class="run-cards">
          <article v-for="run in pipelineRows" :key="`pipe-${run.job_id}`" class="run-card">
            <div class="run-card-head">
              <div>
                <p class="mono run-id">{{ run.job_id }}</p>
                <h3>{{ run.file_name || 'Pipeline run' }}</h3>
              </div>
              <span :class="statusClass(run.status)">{{ run.status || 'unknown' }}</span>
            </div>
            <div class="progress-meta">
              <span>{{ run.progressStage }}</span>
              <span>{{ run.progressPct }}</span>
            </div>
            <div class="progress-track">
              <div class="progress-fill" :style="{ width: run.progressPct }"></div>
            </div>
            <div class="pipeline-sections">
              <div
                v-for="section in run.sections"
                :key="`${run.job_id}-${section.id}`"
                :class="`stage-section stage-${section.state}`"
              >
                <span class="stage-dot"></span>
                <span>{{ section.label }}</span>
              </div>
            </div>
            <p class="run-log mono">{{ run.lastLogLine }}</p>
          </article>
        </div>
      </section>

      <section class="split-grid split-grid-secondary">
        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Database</p>
              <h2>DB Load State</h2>
            </div>
          </div>
          <div class="table-canvas compact">
            <div v-if="!dbLoadRows.length" class="table-empty">No DB load activity yet.</div>
            <div v-else class="table-wrap">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>Job ID</th>
                    <th>DB Load Status</th>
                    <th>Enabled</th>
                    <th>State</th>
                    <th>Pipeline Status</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="run in dbLoadRows" :key="`db-${run.job_id}`">
                    <td class="mono">{{ run.job_id }}</td>
                    <td><span :class="loadStatusClass(run.loadStatus)">{{ run.loadText }}</span></td>
                    <td class="nowrap">{{ run.load_to_db ? 'yes' : 'no' }}</td>
                    <td>{{ run.waitingMsg }}</td>
                    <td><span :class="statusClass(run.status)">{{ run.status || 'unknown' }}</span></td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Audit</p>
              <h2>Pipeline Run Ledger</h2>
            </div>
          </div>
          <div class="table-canvas compact">
            <div v-if="!auditRuns.length" class="table-empty">No audit runs found.</div>
            <div v-else class="table-wrap">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>Run ID</th>
                    <th>Status</th>
                    <th>Stage</th>
                    <th>Event</th>
                    <th>In</th>
                    <th>Success</th>
                    <th>Failed</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="run in auditRuns" :key="run.run_id">
                    <td class="mono">{{ run.run_id }}</td>
                    <td><span :class="statusClass(run.status)">{{ run.status || 'running' }}</span></td>
                    <td class="nowrap">{{ run.last_stage || '-' }}</td>
                    <td class="nowrap">{{ run.last_event || '-' }}</td>
                    <td class="nowrap">{{ run.records_in ?? '-' }}</td>
                    <td class="nowrap">{{ run.records_success ?? '-' }}</td>
                    <td class="nowrap">{{ run.records_failed ?? '-' }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>
      </section>
    </main>
  </div>
</template>
