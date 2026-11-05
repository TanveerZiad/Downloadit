# Downloadit

Downloadit is a modern, high-speed download manager for direct files, media, and documents with an artistic minimal UI. It supports both **Autonomous Vercel Serverless deployment** (running 100% in-browser with zero background servers or terminals) and **Native High-Performance Rust backend** mode.

---

## Features

- **Autonomous Vercel / Cloud Deployment**: Deploys directly to Vercel with zero background terminal or server process needed.
- **Client-Side Streaming Engine**: Browser downloads stream directly to disk or your selected folder via the HTML5 File System Access API.
- **Minimal Artistic UI**: Subtle dotted grid background canvas with full Dark & Light mode toggle.
- **Custom Folder Save Picker**: Choose any folder on your machine where completed files are directly saved.
- **Multi-Connection Segmented Downloads**: High-speed chunked transfers with live concurrent segment visualizations and byte-range tooltips.
- **Interactive Controls**: Instant `⏸ Pause`, `▶ Resume` (HTTP Range), `↺ Retry`, `✕ Cancel`, `↓ Save`, and `⧉ Copy URL` on each card.
- **Live Telemetry**: Real-time speed calculation (`MB/s`), accurate ETA, aggregate queue speed (`⚡ X MB/s`), and filter tabs (`All`, `Active`, `Paused`, `Completed`).
- **Resilient URL Analysis**: Server-side metadata extraction bypassing CORS headers for reliable file name, size, and MIME detection.

---

## Vercel Deployment (Zero-Server / Autonomous)

Downloadit runs on Vercel as a standalone web application without requiring any persistent terminal or server process.

- **Serverless API (`api/`)**:
  - `api/health.js`: Health and feature discovery endpoint (`/api/v1/health`).
  - `api/analyze.js`: Inspects link metadata (`HEAD`/`GET` range), extracting filename, MIME type, and size.
  - `api/proxy.js`: Streaming CORS proxy with HTTP Range support for pause/resume when external hosts block direct browser requests.
  - `api/downloads.js`: Serverless queue endpoint.
- **Client-Side Engine (`web/js/client-engine.js`)**:
  - Streams chunks directly inside the user's browser, eliminating serverless execution timeouts and storage caps.
  - Saves files directly into the folder picked with the folder icon or browser downloads.
  - Persists queue state in `localStorage` across page reloads.

### Deploy in 1 Click:
1. Push this repository to your GitHub.
2. In your [Vercel Dashboard](https://vercel.com/new), import the repository.
3. Click **Deploy**.

---

## Local Development

To test locally with Vercel simulation:
```bash
npm run dev
```
Open [http://localhost:3000](http://localhost:3000) in your browser.

---

## Author
Developed by **Tanveer Ziad** ([@TanveerZiad](https://github.com/TanveerZiad))
