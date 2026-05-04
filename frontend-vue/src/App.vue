<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'

const AUTO_REFRESH_MS = 2000
const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '')
const MULTIPART_SESSION_PREFIX = 'nas.multipart.upload'
const TOKEN_STORAGE_KEY = 'nas.token'
const USERNAME_STORAGE_KEY = 'nas.username'
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

const token = ref(localStorage.getItem(TOKEN_STORAGE_KEY) || '')
const loggedInUsername = ref(localStorage.getItem(USERNAME_STORAGE_KEY) || '')
const loginUsername = ref('')
const loginPassword = ref('')
const loginError = ref('')
const loginLoading = ref(false)

const activeNav = ref('uploads')
const isDragging = ref(0)
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

function clearSession() {
  token.value = ''
  loggedInUsername.value = ''
  localStorage.removeItem(TOKEN_STORAGE_KEY)
  localStorage.removeItem(USERNAME_STORAGE_KEY)
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
}

async function login() {
  loginError.value = ''
  loginLoading.value = true
  try {
    const res = await fetch(`${API_BASE_URL}/api/v1/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: loginUsername.value, password: loginPassword.value }),
    })
    if (!res.ok) {
      const body = await res.json().catch(() => ({}))
      loginError.value = body.detail || `Login failed (${res.status})`
      return
    }
    const data = await res.json()
    token.value = data.access_token
    loggedInUsername.value = data.username || loginUsername.value
    localStorage.setItem(TOKEN_STORAGE_KEY, token.value)
    localStorage.setItem(USERNAME_STORAGE_KEY, loggedInUsername.value)
    loginUsername.value = ''
    loginPassword.value = ''
    await refreshDashboard()
  } catch (err) {
    loginError.value = err.message || 'Network error'
  } finally {
    loginLoading.value = false
  }
}

function logout() {
  clearSession()
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
}

async function apiFetch(path, init = {}) {
  const headers = { ...(init.headers || {}) }
  if (token.value) headers['Authorization'] = `Bearer ${token.value}`
  const res = await fetch(`${API_BASE_URL}${path}`, { ...init, headers })
  if (res.status === 401) {
    clearSession()
    throw new Error('Session expired — please log in again')
  }
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

function friendlyStatus(status) {
  const s = String(status ?? '').toLowerCase()
  if (s === 'completed') return 'Done'
  if (s === 'running' || s === 'queued') return 'Processing'
  if (s === 'pausing') return 'Pausing'
  if (s === 'paused') return 'Paused'
  if (s === 'failed' || s === 'interrupted') return 'Failed'
  if (s === 'uploaded') return 'Queued'
  return status || 'Unknown'
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
    starting_pipeline: 'Starting',
    initializing_spark: 'Starting',
    spark_ready: 'Starting',
    loading_db: 'Saving to DB',
    resuming: 'Resuming',
    writing_output: 'Writing',
    completed: 'Done',
    failed: 'Failed',
    failing: 'Failed',
    interrupted: 'Interrupted',
    pausing: 'Pausing',
    paused: 'Paused',
  }
  if (labels[normalized]) return labels[normalized]
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

const dashboardStats = computed(() => {
  const jobs = ingestItems.value
  const active = jobs.filter((item) => ['queued', 'running', 'pausing'].includes(String(item.status ?? '').toLowerCase())).length
  const completed = jobs.filter((item) => String(item.status ?? '').toLowerCase() === 'completed').length
  const failed = jobs.filter((item) => ['failed', 'interrupted'].includes(String(item.status ?? '').toLowerCase())).length
  return { active, completed, failed, total: jobs.length }
})

async function loadIngestJobs() {
  const payload = await fetchJson('/api/v1/ingest/jobs?limit=20')
  ingestItems.value = Array.isArray(payload.items) ? payload.items : []
}

async function loadRuns() {
  const payload = await fetchJson('/api/v1/ingest/jobs?limit=20')
  auditRuns.value = Array.isArray(payload.items) ? payload.items : []
}

async function refreshDashboard() {
  if (!token.value) return
  if (dashboardRefreshInFlight.value) return
  dashboardRefreshInFlight.value = true
  try {
    await Promise.all([loadIngestJobs(), loadRuns()])
    touchRefreshLabel()
  } catch (error) {
    uploadMeta.value = `Refresh failed: ${error.message}`
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
    searchMeta.value = `${payload.count || searchItems.value.length} result(s) found`
  } catch (error) {
    searchMeta.value = `Search failed: ${error.message}`
  }
}

function onFileChange(event) {
  uploadFile.value = event.target?.files?.[0] || null
}

function onDragEnter(event) {
  event.preventDefault()
  isDragging.value++
}

function onDragLeave() {
  isDragging.value--
}

function onDrop(event) {
  event.preventDefault()
  isDragging.value = 0
  const file = event.dataTransfer?.files?.[0] || null
  if (file) {
    uploadFile.value = file
    void submitUpload()
  }
}

async function submitUpload() {
  if (!uploadFile.value) {
    uploadMeta.value = 'Select a file to upload.'
    return
  }

  try {
    const file = uploadFile.value
    const savedSession = loadSavedUploadSession(file)
    const useMultipart = Boolean(savedSession?.session_id) || file.size > MULTIPART_THRESHOLD_BYTES
    const payload = useMultipart ? await uploadFileInParts(file, savedSession) : await uploadFileDirect(file)
    uploadMeta.value = `Upload complete — job ${payload.job_id} is processing.`
    uploadFile.value = null
    const input = document.getElementById('upload-file')
    if (input) input.value = ''
    await refreshDashboard()
  } catch (error) {
    uploadMeta.value = `Upload failed: ${error.message}. Select the same file again to resume.`
  }
}

async function uploadFileDirect(file) {
  uploadMeta.value = `Uploading ${file.name}...`
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
    uploadMeta.value = `Preparing upload for ${file.name}...`
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
      uploadMeta.value = `Resuming — part ${partNumber}/${totalParts} already stored`
      continue
    }

    uploadMeta.value = `Uploading part ${partNumber} of ${totalParts}...`
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

  uploadMeta.value = 'Finalizing upload...'
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

async function downloadFailedRows(jobId) {
  try {
    const res = await apiFetch(`/api/v1/ingest/jobs/${encodeURIComponent(jobId)}/failed-rows.csv`)
    const blob = await res.blob()
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `failed_rows_${jobId}.csv`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    setTimeout(() => URL.revokeObjectURL(url), 1000)
  } catch (error) {
    uploadMeta.value = `Download failed: ${error.message}`
  }
}

async function pauseJob(jobId) {
  uploadMeta.value = `Pausing job...`
  try {
    await apiFetch(`/api/v1/ingest/jobs/${encodeURIComponent(jobId)}/pause`, { method: 'POST' })
    uploadMeta.value = `Job paused.`
    await refreshDashboard()
  } catch (error) {
    uploadMeta.value = `Pause failed: ${error.message}`
  }
}

async function resumeJob(jobId, isUploaded) {
  uploadMeta.value = `${isUploaded ? 'Starting' : 'Resuming'} job...`
  try {
    await apiFetch(`/api/v1/ingest/jobs/${encodeURIComponent(jobId)}/start`, { method: 'POST' })
    uploadMeta.value = `Job ${isUploaded ? 'started' : 'resumed'}.`
    await refreshDashboard()
  } catch (error) {
    uploadMeta.value = `Failed: ${error.message}`
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
  <!-- ── Login screen ──────────────────────────────────── -->
  <div v-if="!token" class="login-page">
    <div class="login-card">
      <div class="login-brand">
        <svg width="28" height="28" viewBox="0 0 22 22" fill="none" aria-hidden="true">
          <rect x="2" y="2" width="8" height="8" rx="2" fill="var(--brand)"/>
          <rect x="12" y="2" width="8" height="8" rx="2" fill="var(--brand)" opacity="0.55"/>
          <rect x="2" y="12" width="8" height="8" rx="2" fill="var(--brand)" opacity="0.55"/>
          <rect x="12" y="12" width="8" height="8" rx="2" fill="var(--brand)" opacity="0.3"/>
        </svg>
        <span>NAS Portal</span>
      </div>
      <h1 class="login-title">Sign in</h1>
      <form class="login-form" @submit.prevent="login">
        <div class="login-field">
          <label for="login-username">Username</label>
          <input
            id="login-username"
            v-model="loginUsername"
            type="text"
            autocomplete="username"
            required
            :disabled="loginLoading"
          />
        </div>
        <div class="login-field">
          <label for="login-password">Password</label>
          <input
            id="login-password"
            v-model="loginPassword"
            type="password"
            autocomplete="current-password"
            required
            :disabled="loginLoading"
          />
        </div>
        <p v-if="loginError" class="login-error">{{ loginError }}</p>
        <button type="submit" :disabled="loginLoading" class="login-btn">
          {{ loginLoading ? 'Signing in…' : 'Sign in' }}
        </button>
      </form>
    </div>
  </div>

  <!-- ── Main app ──────────────────────────────────────── -->
  <div v-else class="app-shell">

    <!-- Top bar -->
    <header class="topbar">
      <div class="topbar-brand">
        <svg width="22" height="22" viewBox="0 0 22 22" fill="none" aria-hidden="true">
          <rect x="2" y="2" width="8" height="8" rx="2" fill="var(--brand)"/>
          <rect x="12" y="2" width="8" height="8" rx="2" fill="var(--brand)" opacity="0.55"/>
          <rect x="2" y="12" width="8" height="8" rx="2" fill="var(--brand)" opacity="0.55"/>
          <rect x="12" y="12" width="8" height="8" rx="2" fill="var(--brand)" opacity="0.3"/>
        </svg>
        NAS Portal
      </div>
      <div class="topbar-right">
        <span class="topbar-sync">Synced {{ lastUpdatedLabel }}</span>
        <span class="topbar-user">{{ loggedInUsername }}</span>
        <button class="secondary slim" type="button" @click="logout">Sign out</button>
      </div>
    </header>

    <!-- Workspace: sidebar + content -->
    <div class="workspace">

      <!-- Sidebar nav -->
      <nav class="sidebar">
        <button
          :class="['nav-item', activeNav === 'uploads' ? 'nav-active' : '']"
          type="button"
          @click="activeNav = 'uploads'"
        >
          <svg width="18" height="18" viewBox="0 0 18 18" fill="none" aria-hidden="true">
            <path d="M9 2v10M5 6l4-4 4 4" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M2 13v1a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-1" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
          </svg>
          Uploads
        </button>
        <button
          :class="['nav-item', activeNav === 'search' ? 'nav-active' : '']"
          type="button"
          @click="activeNav = 'search'"
        >
          <svg width="18" height="18" viewBox="0 0 18 18" fill="none" aria-hidden="true">
            <circle cx="8" cy="8" r="5.5" stroke="currentColor" stroke-width="1.8"/>
            <path d="M12.5 12.5L16 16" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
          </svg>
          Search
        </button>
        <button
          :class="['nav-item', activeNav === 'history' ? 'nav-active' : '']"
          type="button"
          @click="activeNav = 'history'"
        >
          <svg width="18" height="18" viewBox="0 0 18 18" fill="none" aria-hidden="true">
            <rect x="2" y="4" width="14" height="11" rx="2" stroke="currentColor" stroke-width="1.8"/>
            <path d="M6 4V3a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v1" stroke="currentColor" stroke-width="1.8"/>
            <path d="M5 9h8M5 12h5" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/>
          </svg>
          History
        </button>

        <div class="sidebar-stats">
          <div class="stat-row">
            <span class="stat-dot dot-active"></span>
            <span>{{ dashboardStats.active }} processing</span>
          </div>
          <div class="stat-row">
            <span class="stat-dot dot-ok"></span>
            <span>{{ dashboardStats.completed }} done</span>
          </div>
          <div v-if="dashboardStats.failed > 0" class="stat-row">
            <span class="stat-dot dot-fail"></span>
            <span>{{ dashboardStats.failed }} failed</span>
          </div>
        </div>
      </nav>

      <!-- Main content -->
      <main class="content">

        <!-- ── Uploads ──────────────────────────────────────── -->
        <div v-if="activeNav === 'uploads'" class="view">
          <div class="view-header">
            <h1>Upload Files</h1>
            <p class="view-sub">Drop CSV, Excel, or JSON files to start address processing.</p>
          </div>

          <!-- Drop zone -->
          <div
            :class="['drop-zone', isDragging > 0 ? 'drop-zone-over' : '']"
            @dragenter="onDragEnter"
            @dragover.prevent
            @dragleave="onDragLeave"
            @drop="onDrop"
          >
            <svg class="drop-icon" width="48" height="48" viewBox="0 0 48 48" fill="none" aria-hidden="true">
              <circle cx="24" cy="24" r="22" fill="var(--brand-soft)"/>
              <path d="M24 14v16M16 22l8-8 8 8" stroke="var(--brand)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M14 34h20" stroke="var(--brand)" stroke-width="2.5" stroke-linecap="round"/>
            </svg>
            <p class="drop-title">Drop your file here</p>
            <p class="drop-sub">or <label for="upload-file" class="browse-link">browse to select</label></p>
            <p class="drop-hint">CSV · Excel · JSON · Supports resume for large files</p>
            <input
              id="upload-file"
              type="file"
              accept=".csv,.json,.jsonl,.ndjson,.xlsx,.xls"
              @change="onFileChange"
            />
          </div>

          <!-- Staged file ready to upload -->
          <div v-if="uploadFile" class="staged-file">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
              <rect x="3" y="2" width="14" height="16" rx="2" stroke="var(--brand)" stroke-width="1.6"/>
              <path d="M7 7h6M7 10h6M7 13h4" stroke="var(--brand)" stroke-width="1.4" stroke-linecap="round"/>
            </svg>
            <span class="staged-name">{{ uploadFile.name }}</span>
            <button type="button" @click="submitUpload">Upload</button>
            <button class="secondary" type="button" @click="uploadFile = null">Cancel</button>
          </div>

          <p v-if="uploadMeta" class="upload-meta">{{ uploadMeta }}</p>

          <!-- File list -->
          <div class="list-header">
            <h2>Your Files</h2>
            <button class="secondary slim" type="button" @click="refreshDashboard">Refresh</button>
          </div>

          <div v-if="!ingestItems.length" class="empty-state">
            <svg width="40" height="40" viewBox="0 0 40 40" fill="none" aria-hidden="true">
              <circle cx="20" cy="20" r="18" fill="var(--bg-alt)"/>
              <path d="M13 20h14M20 13v14" stroke="var(--muted)" stroke-width="2" stroke-linecap="round" opacity="0.5"/>
            </svg>
            <p>No files yet. Upload your first file above.</p>
          </div>

          <div v-else class="file-list">
            <article
              v-for="run in pipelineRows"
              :key="run.job_id"
              class="file-card"
            >
              <div class="file-card-icon">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                  <rect x="4" y="2" width="16" height="20" rx="2" fill="var(--brand-soft)" stroke="var(--brand)" stroke-width="1.2"/>
                  <path d="M8 8h8M8 11h8M8 14h5" stroke="var(--brand)" stroke-width="1.2" stroke-linecap="round"/>
                </svg>
              </div>
              <div class="file-card-body">
                <div class="file-card-top">
                  <span class="file-card-name">{{ run.file_name || 'Unnamed file' }}</span>
                  <span :class="statusClass(run.status)">{{ friendlyStatus(run.status) }}</span>
                </div>
                <div class="file-card-stage">{{ run.progressStage }} · {{ run.progressPct }}</div>
                <div class="progress-track">
                  <div class="progress-fill" :style="{ width: run.progressPct }"></div>
                </div>
                <div v-if="run.error" class="file-card-error">{{ run.error }}</div>
              </div>
              <div class="file-card-actions">
                <button
                  v-if="String(run.status || '').toLowerCase() === 'running'"
                  class="secondary slim"
                  type="button"
                  @click="pauseJob(run.job_id)"
                >
                  Pause
                </button>
                <button
                  v-if="['failed','interrupted','uploaded','paused'].includes(String(run.status || '').toLowerCase())"
                  class="secondary slim"
                  type="button"
                  @click="resumeJob(run.job_id, String(run.status || '').toLowerCase() === 'uploaded')"
                >
                  {{ String(run.status || '').toLowerCase() === 'uploaded' ? 'Start' : 'Resume' }}
                </button>
              </div>
            </article>
          </div>
        </div>

        <!-- ── Search ───────────────────────────────────────── -->
        <div v-if="activeNav === 'search'" class="view">
          <div class="view-header">
            <h1>Search Addresses</h1>
            <p class="view-sub">Find normalized address records by postcode, locality, or full address.</p>
          </div>

          <form class="search-form" @submit.prevent="submitSearch">
            <div class="search-input-wrap">
              <svg class="search-icon" width="18" height="18" viewBox="0 0 18 18" fill="none" aria-hidden="true">
                <circle cx="8" cy="8" r="5.5" stroke="var(--muted)" stroke-width="1.6"/>
                <path d="M12.5 12.5L16 16" stroke="var(--muted)" stroke-width="1.6" stroke-linecap="round"/>
              </svg>
              <input
                v-model="searchQuery"
                type="text"
                placeholder="Search by postcode, locality, or address..."
                minlength="2"
                required
              />
            </div>
            <button type="submit">Search</button>
          </form>

          <p v-if="searchMeta" class="search-meta">{{ searchMeta }}</p>

          <div v-if="searchItems.length" class="search-results">
            <article
              v-for="item in searchItems"
              :key="`${item.record_id || ''}-${item.naskod || ''}`"
              class="result-card"
            >
              <div class="result-address">{{ item.address_clean || '-' }}</div>
              <div class="result-meta">
                <span>{{ item.locality_name || '-' }}</span>
                <span class="result-sep">·</span>
                <span>{{ item.district_name || '-' }}</span>
                <span class="result-sep">·</span>
                <span>{{ item.state_name || '-' }}</span>
                <span class="result-sep">·</span>
                <span class="mono">{{ item.postcode || '-' }}</span>
              </div>
              <div class="result-footer">
                <span class="result-code mono">{{ item.naskod || 'No code' }}</span>
                <span v-if="item.confidence_score" class="result-score">Score {{ item.confidence_score }}</span>
              </div>
            </article>
          </div>

          <div v-else-if="!searchMeta" class="empty-state">
            <svg width="40" height="40" viewBox="0 0 40 40" fill="none" aria-hidden="true">
              <circle cx="18" cy="18" r="12" stroke="var(--muted)" stroke-width="2" opacity="0.4"/>
              <path d="M27 27l8 8" stroke="var(--muted)" stroke-width="2" stroke-linecap="round" opacity="0.4"/>
            </svg>
            <p>Enter at least 2 characters to search.</p>
          </div>
        </div>

        <!-- ── History ─────────────────────────────────────── -->
        <div v-if="activeNav === 'history'" class="view">
          <div class="view-header">
            <h1>History</h1>
            <p class="view-sub">All ingest runs with their current processing status.</p>
          </div>

          <div v-if="!pipelineRows.length" class="empty-state">
            <p>No history yet.</p>
          </div>

          <div v-else class="history-list">
            <article
              v-for="run in pipelineRows"
              :key="`hist-${run.job_id}`"
              class="history-card"
            >
              <div class="history-card-left">
                <div class="file-card-name">{{ run.file_name || 'Unnamed file' }}</div>
                <div class="history-id mono">{{ run.job_id }}</div>
                <div class="history-date">{{ formatDate(run.created_at) }}</div>
              </div>

              <div class="history-card-center">
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
                <div class="progress-track slim-track">
                  <div class="progress-fill" :style="{ width: run.progressPct }"></div>
                </div>
                <div v-if="run.success_count != null || run.failed_count != null" class="row-counts">
                  <span v-if="run.success_count != null" class="count-ok">{{ run.success_count }} loaded</span>
                  <span v-if="run.warning_count" class="count-warn">{{ run.warning_count }} warnings</span>
                  <span v-if="run.failed_count" class="count-fail">{{ run.failed_count }} failed</span>
                </div>
              </div>

              <div class="history-card-right">
                <span :class="statusClass(run.status)">{{ friendlyStatus(run.status) }}</span>
                <div class="file-card-actions" style="margin-top:8px">
                  <button
                    v-if="String(run.status || '').toLowerCase() === 'running'"
                    class="secondary slim"
                    type="button"
                    @click="pauseJob(run.job_id)"
                  >
                    Pause
                  </button>
                  <button
                    v-if="['failed','interrupted','uploaded','paused'].includes(String(run.status || '').toLowerCase())"
                    class="secondary slim"
                    type="button"
                    @click="resumeJob(run.job_id, String(run.status || '').toLowerCase() === 'uploaded')"
                  >
                    {{ String(run.status || '').toLowerCase() === 'uploaded' ? 'Start' : 'Resume' }}
                  </button>
                  <button
                    v-if="run.failed_count > 0"
                    class="secondary slim"
                    type="button"
                    @click="downloadFailedRows(run.job_id)"
                  >
                    Download failed
                  </button>
                </div>
              </div>
            </article>
          </div>
        </div>

      </main>
    </div>
  </div>
  <!-- end v-else (main app) -->
</template>
