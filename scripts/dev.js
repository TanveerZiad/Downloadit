import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import healthHandler from '../api/health.js';
import analyzeHandler from '../api/analyze.js';
import proxyHandler from '../api/proxy.js';
import downloadsHandler from '../api/downloads.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, '..');
const PUBLIC_DIR = path.join(ROOT, 'public');
const WEB_DIR = path.join(ROOT, 'web');

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.wasm': 'application/wasm',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8',
};

// Adapt Node http req/res to Vercel Serverless Function signature
function adaptHandler(handler) {
  return async (req, res) => {
    const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    req.query = Object.fromEntries(urlObj.searchParams.entries());

    let rawBody = '';
    req.on('data', chunk => { rawBody += chunk; });
    req.on('end', async () => {
      if (rawBody) {
        try {
          req.body = JSON.parse(rawBody);
        } catch (_) {
          req.body = rawBody;
        }
      } else {
        req.body = {};
      }

      // Add Vercel helper methods
      res.status = (code) => {
        res.statusCode = code;
        return res;
      };
      res.json = (data) => {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(data));
        return res;
      };

      try {
        await handler(req, res);
      } catch (err) {
        console.error('API Error:', err);
        if (!res.headersSent) {
          res.statusCode = 500;
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ error: err.message || 'Internal server error' }));
        }
      }
    });
  };
}

const server = http.createServer((req, res) => {
  const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = urlObj.pathname;

  // 1. API routes (matching Vercel routing)
  if (pathname === '/api/health' || pathname === '/api/v1/health') {
    return adaptHandler(healthHandler)(req, res);
  }
  if (pathname === '/api/analyze' || pathname === '/api/v1/analyze') {
    return adaptHandler(analyzeHandler)(req, res);
  }
  if (pathname === '/api/proxy' || pathname === '/api/v1/proxy') {
    return adaptHandler(proxyHandler)(req, res);
  }
  if (pathname.startsWith('/api/downloads') || pathname.startsWith('/api/v1/downloads')) {
    return adaptHandler(downloadsHandler)(req, res);
  }

  // 2. Static file serving (from public or web)
  let relativePath = pathname === '/' ? '/index.html' : pathname;
  let filePath = path.join(PUBLIC_DIR, relativePath);

  if (!fs.existsSync(filePath)) {
    filePath = path.join(WEB_DIR, relativePath);
  }

  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';
    res.statusCode = 200;
    res.setHeader('Content-Type', contentType);
    res.setHeader('Access-Control-Allow-Origin', '*');
    fs.createReadStream(filePath).pipe(res);
  } else {
    // 404
    res.statusCode = 404;
    res.setHeader('Content-Type', 'text/plain');
    res.end('Not Found');
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n======================================================`);
  console.log(`🚀 Downloadit Vercel Simulation Server is running!`);
  console.log(`📡 Local URL: http://localhost:${PORT}`);
  console.log(`🌐 Ready to test in your browser with zero terminal backend!`);
  console.log(`======================================================\n`);
});

