<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'

const AUTO_REFRESH_MS = 2000
const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000').replace(/\/$/, '')
const PIPELINE_STAGE_ORDER = [
  { id: 'extract', label: 'Extract' },
  { id: 'transform', label: 'Transform' },
  { id: 'validate', label: 'Validate' },
  { id: 'final', label: 'Final Clean Data' },
]

const uploadFile = ref(null)
const uploadMeta = ref('')

const searchQuery = ref('')
const searchMeta = ref('')
const searchItems = ref([])

const ingestItems = ref([])
const auditRuns = ref([])
const dashboardRefreshInFlight = ref(false)
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

function statusClass(status) {
  if (status === 'ok' || status === 'completed') return 'pill ok'
  if (status === 'failed' || status === 'error' || status === 'interrupted') return 'pill fail'
  return 'pill'
}

function loadStatusClass(status) {
  const normalized = String(status ?? '').toLowerCase()
  if (normalized === 'completed') return 'pill ok'
  if (normalized === 'failed') return 'pill fail'
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
    final: 'Final',
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
  return raw
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

const pipelineRows = computed(() => ingestItems.value.map((run) => ({
  ...run,
  progressPct: Number.isFinite(Number(run.progress_pct))
    ? `${Math.max(0, Math.min(100, Number(run.progress_pct)))}%`
    : (run.status === 'completed' || run.status === 'failed' ? '100%' : '-'),
  progressStage: formatStageLabel(run.progress_stage),
  lastLogLine: run.last_log_line || '-',
  sections: pipelineSections(run),
})))

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
  } catch (error) {
    uploadMeta.value = `Dashboard refresh failed: ${error.message}`
  } finally {
    dashboardRefreshInFlight.value = false
  }
}

async function submitSearch() {
  const q = searchQuery.value.trim()
  if (q.length < 2) return
  searchMeta.value = 'Searching...'
  searchItems.value = []
  try {
    const payload = await fetchJson(`/api/v1/search/autocomplete?q=${encodeURIComponent(q)}&size=10`)
    searchItems.value = Array.isArray(payload.items) ? payload.items : []
    searchMeta.value = `${payload.count || searchItems.value.length} result(s) for "${payload.query || q}"`
  } catch (error) {
    searchMeta.value = `Search failed: ${error.message}`
  }
}

function onFileChange(event) {
  uploadFile.value = event.target?.files?.[0] || null
}

async function submitUpload() {
  if (!uploadFile.value) {
    uploadMeta.value = 'Please choose a file before running.'
    return
  }

  uploadMeta.value = 'Uploading...'
  const formData = new FormData()
  formData.append('file', uploadFile.value)
  formData.append('auto_start', 'true')
  formData.append('load_to_db', 'true')
  formData.append('resume_from_checkpoint', 'true')
  formData.append('resume_failed_only', 'true')

  try {
    const res = await apiFetch('/api/v1/ingest/upload', { method: 'POST', body: formData })
    const payload = await res.json()
    uploadMeta.value = `Uploaded. Job: ${payload.job_id}`
    uploadFile.value = null
    const input = document.getElementById('upload-file')
    if (input) input.value = ''
    await refreshDashboard()
  } catch (error) {
    uploadMeta.value = `Upload failed: ${error.message}`
  }
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
  <header class="topbar">
    <div class="topbar-inner">
      <div class="brand">
        <span class="brand-dot"></span>
        <span>admin.alamat.gov.my</span>
      </div>
    </div>
  </header>

  <main class="page-shell">
    <section class="page-title-row">
      <h1>Dashboard</h1>
      <div class="meta mono">API: {{ API_BASE_URL }}</div>
    </section>

    <section class="panel section-block">
      <h2>Upload + Process</h2>
      <div class="upload-row">
        <input id="upload-file" type="file" accept=".csv,.json,.jsonl,.ndjson,.xlsx,.xls" @change="onFileChange" />
        <button type="button" @click="submitUpload">Upload and Run</button>
      </div>
      <div class="meta">{{ uploadMeta }}</div>
    </section>

    <section class="panel section-block">
      <h2>Search Address</h2>
      <form class="search-row" @submit.prevent="submitSearch">
        <input v-model="searchQuery" type="text" placeholder="Type address, postcode, naskod..." minlength="2" required />
        <button type="submit">Search</button>
      </form>
      <div class="meta">{{ searchMeta }}</div>
      <div class="table-canvas">
        <div v-if="!searchItems.length" class="table-empty">No search result.</div>
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
    </section>

    <section class="panel section-block">
      <div class="section-head">
        <h2>Ingest Jobs</h2>
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
                  {{ Number.isFinite(Number(run.progress_pct)) ? `${Math.max(0, Math.min(100, Number(run.progress_pct)))}%` : '-' }}
                  · {{ formatStageLabel(run.progress_stage) }}
                </td>
                <td class="mono">{{ run.created_at || '-' }}</td>
                <td class="mono">
                  {{ run.log_path || '-' }}
                  <div v-if="run.error" class="mono">Error: {{ run.error }}</div>
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
    </section>

    <section class="panel section-block">
      <h2>Pipeline Stages</h2>
      <div class="table-canvas">
        <div v-if="!pipelineRows.length" class="table-empty">No pipeline activity yet.</div>
        <div v-else class="table-wrap">
          <table class="data-table">
            <thead>
              <tr>
                <th>Job ID</th>
                <th>Status</th>
                <th>Progress</th>
                <th>Current Stage</th>
                <th>Sections</th>
                <th>Live</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="run in pipelineRows" :key="`pipe-${run.job_id}`">
                <td class="mono">{{ run.job_id }}</td>
                <td><span :class="statusClass(run.status)">{{ run.status || 'unknown' }}</span></td>
                <td class="nowrap">{{ run.progressPct }}</td>
                <td class="nowrap">{{ run.progressStage }}</td>
                <td>
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
                </td>
                <td class="mono">{{ run.lastLogLine }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="panel section-block">
      <h2>DB Load</h2>
      <div class="table-canvas">
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
    </section>

    <section class="panel section-block">
      <div class="section-head">
        <h2>Pipeline Runs (Audit)</h2>
        <button class="secondary" type="button" @click="refreshDashboard">Refresh</button>
      </div>
      <div class="table-canvas">
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
    </section>
  </main>
</template>
