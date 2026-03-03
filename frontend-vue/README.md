# NAS Vue Frontend

Standalone Vue 3 + Vite frontend for NAS dashboard.

## 1) Configure API

Create env file:

```bash
cp .env.example .env
```

Set backend URL:

```bash
VITE_API_BASE_URL=http://localhost:8000
```

For production:

```bash
VITE_API_BASE_URL=https://api.alamat.gov.my
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

## 5) Backend CORS

Backend `.env` must include your frontend origin:

```bash
CORS_ALLOW_ORIGINS=https://admin.alamat.gov.my
```

Then restart backend.
