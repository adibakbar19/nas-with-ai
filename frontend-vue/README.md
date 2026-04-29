# NAS Vue Frontend

Standalone Vue 3 + Vite frontend for NAS dashboard.

## 1) Configure API

Create env file:

```bash
cp .env.example .env
```

For local Docker deployment, leave it empty so nginx proxies `/api` to the backend container:

```bash
VITE_API_BASE_URL=
```

For production with a separate external API host:

```bash
VITE_API_BASE_URL=https://api.alamat.gov.my
```

Optional upload tuning:

```bash
# Files at or below this size use direct upload.
# Files above this size use the resumable multipart flow.
VITE_MULTIPART_THRESHOLD_BYTES=33554432
```

## 2) Run Local

```bash
npm install
npm run dev
```

Open:
- `http://localhost:5173`

## 3) Build

```bash
npm run build
```

Output directory:
- `dist/`

## 4) Deploy To Bucket + CDN

Example (AWS S3):

```bash
aws s3 sync dist/ s3://your-frontend-bucket --delete
```

Serve with CloudFront (recommended) and route to domain like:
- `https://admin.alamat.gov.my`

## 5) Run In Docker

Frontend is containerized separately from the backend stack on purpose, so you can replace or integrate a different frontend later without changing `docker-compose.yml`.

Build and run:

```bash
docker compose -f docker-compose.frontend.yml up -d --build
```

Defaults:
- frontend URL: `http://localhost:8088`
- backend API path: same-origin `/api` via nginx proxy to `api:8000`

Override build-time API URL only if the frontend must call a different external API origin:

```bash
VITE_API_BASE_URL=https://api.alamat.gov.my docker compose -f docker-compose.frontend.yml up -d --build
```

Override host port:

```bash
FRONTEND_PORT=8089 docker compose -f docker-compose.frontend.yml up -d --build
```

## 6) Backend CORS

Backend `.env` must include your frontend origin:

```bash
CORS_ALLOW_ORIGINS=https://admin.alamat.gov.my
```

Then restart backend.
