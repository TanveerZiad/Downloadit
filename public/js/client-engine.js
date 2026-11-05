/**
 * Downloadit Client-Side Streaming Engine
 * 
 * Enables Downloadit to run autonomously in the browser on Vercel and serverless platforms
 * with ZERO background server or terminal process required.
 * 
 * - Streams file chunks directly into user's chosen folder via File System Access API
 * - Automatic CORS fallback via /api/v1/proxy
 * - Full Pause, Resume (HTTP Range), Speed (MB/s), ETA calculation
 * - Multi-segment progress visualizer support
 * - LocalStorage persistence of download queue and completed files
 */

(function() {
  'use strict';

  const STORAGE_KEY = 'downloadit-jobs';
  let isServerlessMode = false;
  const activeControllers = new Map(); // id -> AbortController
  const activeWritables = new Map();   // id -> FileSystemWritableFileStream
  const activeEventSources = new Map(); // id -> Set<ClientEventSource>
  const fileBlobs = new Map();         // id -> Blob

  // Load jobs from localStorage
  function loadJobs() {
    try {
      const data = localStorage.getItem(STORAGE_KEY);
      if (data) {
        return JSON.parse(data);
      }
    } catch (_) {}
    return [];
  }

  // Save jobs to localStorage
  function saveJobs(jobs) {
    try {
      // Don't serialize blobs or huge segment arrays
      const cleaned = jobs.map(j => ({
        id: j.id,
        name: j.name,
        mime: j.mime || 'application/octet-stream',
        size: j.size || 0,
        downloaded: j.downloaded || 0,
        status: j.status === 'downloading' ? 'paused' : j.status,
        scan: j.scan || 'clean',
        error: j.error || '',
        updated: j.updated || Math.floor(Date.now() / 1000),
        created: j.created || Math.floor(Date.now() / 1000),
        speed: 0,
        eta: 0,
        segments: j.segments || [],
        url: j.url || ''
      }));
      localStorage.setItem(STORAGE_KEY, JSON.stringify(cleaned));
    } catch (_) {}
  }

  // Broadcast job update to any active EventSource listeners (including WASM)
  function broadcastJob(job) {
    const listeners = activeEventSources.get(job.id);
    if (listeners) {
      const jsonStr = JSON.stringify(job);
      listeners.forEach(es => {
        try {
          const event = new MessageEvent('message', { data: jsonStr });
          es.dispatchEvent(event);
          if (typeof es.onmessage === 'function') {
            es.onmessage(event);
          }
        } catch (e) {
          console.warn('EventSource dispatch error:', e);
        }
      });
    }
  }

  // Client-side EventSource mock for serverless mode
  class ClientEventSource extends EventTarget {
    constructor(url) {
      super();
      this.url = url;
      this.readyState = 1; // OPEN
      this.onmessage = null;
      this.onerror = null;
      this.onopen = null;

      // Extract job id: /api/v1/downloads/{id}/events
      const match = url.match(/\/downloads\/([^/]+)\/events/);
      this.jobId = match ? match[1] : null;

      if (this.jobId) {
        if (!activeEventSources.has(this.jobId)) {
          activeEventSources.set(this.jobId, new Set());
        }
        activeEventSources.get(this.jobId).add(this);

        // Immediately send current state if job exists
        const jobs = loadJobs();
        const job = jobs.find(j => j.id === this.jobId);
        if (job) {
          setTimeout(() => {
            const event = new MessageEvent('message', { data: JSON.stringify(job) });
            this.dispatchEvent(event);
            if (typeof this.onmessage === 'function') {
              this.onmessage(event);
            }
          }, 10);
        }
      }
    }

    close() {
      this.readyState = 2; // CLOSED
      if (this.jobId && activeEventSources.has(this.jobId)) {
        activeEventSources.get(this.jobId).delete(this);
      }
    }
  }

  // Start in-browser streaming download
  async function startBrowserDownload(job, rangeStart = 0) {
    const controller = new AbortController();
    activeControllers.set(job.id, controller);

    let writable = activeWritables.get(job.id) || null;
    let folderHandle = window.currentFolderHandle || null;

    // Check if we can write to selected directory
    if (!writable && folderHandle && typeof folderHandle.getFileHandle === 'function') {
      try {
        const fileHandle = await folderHandle.getFileHandle(job.name, { create: true });
        writable = await fileHandle.createWritable({ keepExistingData: rangeStart > 0 });
        if (rangeStart > 0) {
          await writable.seek(rangeStart);
        }
        activeWritables.set(job.id, writable);
      } catch (err) {
        console.warn('Could not create writable in directory:', err);
        writable = null;
      }
    }

    let fetchUrl = job.url;
    let response;
    let usedProxy = false;

    const headers = {};
    if (rangeStart > 0) {
      headers['Range'] = `bytes=${rangeStart}-`;
    }

    // 1. Try direct fetch
    try {
      response = await fetch(fetchUrl, {
        headers: headers,
        mode: 'cors',
        signal: controller.signal
      });
      if (!response.ok && response.status !== 206) {
        throw new Error(`Direct fetch HTTP ${response.status}`);
      }
    } catch (err) {
      // 2. Direct fetch failed (CORS or network) -> route through /api/v1/proxy
      console.log('Direct fetch blocked by CORS or network, routing through /api/v1/proxy:', err.message);
      usedProxy = true;
      try {
        fetchUrl = `/api/v1/proxy?url=${encodeURIComponent(job.url)}`;
        response = await fetch(fetchUrl, {
          headers: headers,
          signal: controller.signal
        });
      } catch (proxyErr) {
        job.status = 'failed';
        job.error = `Download failed: ${proxyErr.message}`;
        broadcastJob(job);
        const jobs = loadJobs();
        const idx = jobs.findIndex(j => j.id === job.id);
        if (idx >= 0) { jobs[idx] = job; saveJobs(jobs); }
        return;
      }
    }

    if (!response || (!response.ok && response.status !== 206)) {
      job.status = 'failed';
      job.error = `Server returned HTTP ${response ? response.status : 'error'}`;
      broadcastJob(job);
      const jobs = loadJobs();
      const idx = jobs.findIndex(j => j.id === job.id);
      if (idx >= 0) { jobs[idx] = job; saveJobs(jobs); }
      return;
    }

    // Update total size if known
    const contentRange = response.headers.get('content-range');
    if (contentRange) {
      const match = contentRange.match(/\/(\d+)/);
      if (match && match[1]) {
        job.size = parseInt(match[1], 10);
      }
    } else {
      const cl = response.headers.get('content-length');
      if (cl && (!job.size || job.size <= 0)) {
        job.size = (parseInt(cl, 10) || 0) + rangeStart;
      }
    }

    if (!job.segments || job.segments.length === 0) {
      job.segments = [{
        start: 0,
        end: job.size > 0 ? job.size : 0,
        pos: rangeStart
      }];
    } else {
      job.segments[0].pos = rangeStart;
      if (job.size > 0) job.segments[0].end = job.size;
    }

    job.status = 'downloading';
    broadcastJob(job);

    const reader = response.body.getReader();
    let downloaded = rangeStart;
    let lastTick = performance.now();
    let bytesSinceTick = 0;
    const memoryChunks = [];

    try {
      while (true) {
        if (job.status !== 'downloading') {
          try { await reader.cancel(); } catch (_) {}
          break;
        }

        const { done, value } = await reader.read();
        if (done) break;

        if (writable) {
          await writable.write(value);
        } else {
          memoryChunks.push(value);
        }

        downloaded += value.length;
        bytesSinceTick += value.length;
        job.downloaded = downloaded;
        if (job.segments.length > 0) {
          job.segments[0].pos = downloaded;
        }

        const now = performance.now();
        const elapsed = (now - lastTick) / 1000;
        if (elapsed >= 0.3) {
          job.speed = Math.round(bytesSinceTick / elapsed);
          if (job.size > 0 && job.speed > 0) {
            job.eta = Math.max(0, Math.round((job.size - job.downloaded) / job.speed));
          }
          lastTick = now;
          bytesSinceTick = 0;
          broadcastJob(job);
        }
      }

      if (job.status === 'downloading') {
        // Finished successfully!
        if (writable) {
          try {
            await writable.close();
            activeWritables.delete(job.id);
          } catch (e) {
            console.warn('Writable close error:', e);
          }
        } else if (memoryChunks.length > 0) {
          const blob = new Blob(memoryChunks, { type: job.mime });
          fileBlobs.set(job.id, blob);

          // Trigger browser download if no folder was chosen
          try {
            const blobUrl = URL.createObjectURL(blob);
            const anchor = document.createElement('a');
            anchor.href = blobUrl;
            anchor.download = job.name;
            document.body.appendChild(anchor);
            anchor.click();
            setTimeout(() => {
              document.body.removeChild(anchor);
              URL.revokeObjectURL(blobUrl);
            }, 30000);
          } catch (_) {}
        }

        job.status = 'completed';
        job.downloaded = downloaded;
        if (job.size <= 0) job.size = downloaded;
        if (job.segments.length > 0) {
          job.segments[0].pos = job.size;
          job.segments[0].end = job.size;
        }
        job.speed = 0;
        job.eta = 0;
        job.updated = Math.floor(Date.now() / 1000);
        broadcastJob(job);

        const jobs = loadJobs();
        const idx = jobs.findIndex(j => j.id === job.id);
        if (idx >= 0) {
          jobs[idx] = job;
          saveJobs(jobs);
        }

        if (window.toast) {
          window.toast(`Download completed: ${job.name}`);
        }
      }
    } catch (err) {
      if (err.name === 'AbortError') {
        // User paused or cancelled
        return;
      }
      console.error('Download stream error:', err);
      job.status = 'failed';
      job.error = `Stream interrupted: ${err.message}`;
      job.speed = 0;
      job.eta = 0;
      broadcastJob(job);
      const jobs = loadJobs();
      const idx = jobs.findIndex(j => j.id === job.id);
      if (idx >= 0) { jobs[idx] = job; saveJobs(jobs); }
    } finally {
      activeControllers.delete(job.id);
    }
  }

  // Intercept window.fetch for /api/v1/downloads* in serverless mode
  const originalFetch = window.fetch;
  window.fetch = async function(input, init) {
    const url = typeof input === 'string' ? input : (input && input.url ? input.url : '');
    const method = (init && init.method ? init.method : 'GET').toUpperCase();

    // Capture analysis metadata for seamless client-side streaming
    if (url.includes('/api/v1/analyze')) {
      const res = await originalFetch.apply(this, arguments);
      try {
        const clone = res.clone();
        clone.json().then(data => {
          window.lastAnalysis = data;
        }).catch(() => {});
      } catch (_) {}
      return res;
    }

    // Check if it's an API request in Serverless Mode
    if (isServerlessMode && url.includes('/api/v1/downloads')) {
      // 1. GET /api/v1/downloads -> list jobs
      if (url.endsWith('/api/v1/downloads') && method === 'GET') {
        const jobs = loadJobs();
        return new Response(JSON.stringify({ downloads: jobs }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // 2. POST /api/v1/downloads -> create job & start in-browser stream
      if (url.endsWith('/api/v1/downloads') && method === 'POST') {
        let payload = {};
        if (init && init.body) {
          try { payload = JSON.parse(init.body); } catch (_) {}
        }

        const jobId = 'job_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
        const jobs = loadJobs();

        // Get analysis if available from DOM
        let detectedSize = 0;
        let detectedMime = 'application/octet-stream';
        if (window.lastAnalysis) {
          detectedSize = window.lastAnalysis.size || 0;
          detectedMime = window.lastAnalysis.mime || 'application/octet-stream';
        }

        const newJob = {
          id: jobId,
          name: payload.name || 'download.bin',
          mime: detectedMime,
          size: detectedSize,
          downloaded: 0,
          status: 'downloading',
          scan: 'clean',
          error: '',
          updated: Math.floor(Date.now() / 1000),
          created: Math.floor(Date.now() / 1000),
          speed: 0,
          eta: 0,
          segments: [{ start: 0, end: detectedSize, pos: 0 }],
          url: payload.url || ''
        };

        jobs.unshift(newJob);
        saveJobs(jobs);

        // Start background download stream
        setTimeout(() => {
          startBrowserDownload(newJob, 0);
        }, 50);

        return new Response(JSON.stringify(newJob), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // 3. POST /api/v1/downloads/:id/pause
      if (url.includes('/pause') && method === 'POST') {
        const match = url.match(/\/downloads\/([^/]+)\/pause/);
        const id = match ? match[1] : null;
        if (id) {
          const controller = activeControllers.get(id);
          if (controller) controller.abort();
          activeControllers.delete(id);

          const jobs = loadJobs();
          const job = jobs.find(j => j.id === id);
          if (job) {
            job.status = 'paused';
            job.speed = 0;
            job.eta = 0;
            saveJobs(jobs);
            broadcastJob(job);
          }
        }
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // 4. POST /api/v1/downloads/:id/resume
      if (url.includes('/resume') && method === 'POST') {
        const match = url.match(/\/downloads\/([^/]+)\/resume/);
        const id = match ? match[1] : null;
        if (id) {
          const jobs = loadJobs();
          const job = jobs.find(j => j.id === id);
          if (job) {
            job.status = 'downloading';
            saveJobs(jobs);
            broadcastJob(job);
            setTimeout(() => {
              startBrowserDownload(job, job.downloaded || 0);
            }, 50);
          }
        }
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // 5. DELETE /api/v1/downloads/:id
      if (method === 'DELETE') {
        const match = url.match(/\/downloads\/([^/]+)/);
        const id = match ? match[1] : null;
        if (id) {
          const controller = activeControllers.get(id);
          if (controller) controller.abort();
          activeControllers.delete(id);

          let jobs = loadJobs();
          jobs = jobs.filter(j => j.id !== id);
          saveJobs(jobs);
        }
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // 6. GET /api/v1/downloads/:id/file -> serve file for saving
      if (url.includes('/file') && method === 'GET') {
        const match = url.match(/\/downloads\/([^/]+)\/file/);
        const id = match ? match[1] : null;
        if (id && fileBlobs.has(id)) {
          const blob = fileBlobs.get(id);
          return new Response(blob, {
            status: 200,
            headers: {
              'Content-Type': blob.type || 'application/octet-stream',
              'Content-Length': String(blob.size)
            }
          });
        }
      }
    }

    return originalFetch.apply(this, arguments);
  };

  // Intercept window.EventSource in serverless mode
  const OriginalEventSource = window.EventSource;
  window.EventSource = function(url, options) {
    if (isServerlessMode && typeof url === 'string' && url.includes('/downloads/') && url.endsWith('/events')) {
      return new ClientEventSource(url);
    }
    return new OriginalEventSource(url, options);
  };

  // Check backend server availability
  async function checkServerMode() {
    try {
      const res = await originalFetch('/api/v1/health', { method: 'GET' });
      if (res.ok) {
        const data = await res.json();
        if (data.serverless === true || !data.features || !data.features.includes('video-extraction')) {
          // Running on Vercel / Serverless!
          enableServerlessMode();
        } else {
          // Local Rust server running
          console.log('Downloadit running with local Rust daemon.');
        }
        return;
      }
    } catch (_) {}

    // Not reachable -> running on static / serverless
    enableServerlessMode();
  }

  function enableServerlessMode() {
    isServerlessMode = true;
    console.log('Downloadit running in Autonomous Serverless & In-Browser Download mode.');
    
    // Update badge in top bar
    const connEl = document.querySelector('.topbar-right .connection');
    if (connEl) {
      connEl.innerHTML = '<i style="background:#22c55e;box-shadow:0 0 8px #22c55e;"></i> Web &amp; Serverless';
      connEl.setAttribute('title', 'Running in serverless mode — downloads stream directly to your device with zero terminal required');
    }

    // Attach handler for "Clear completed"
    const clearBtn = document.getElementById('clear-completed');
    if (clearBtn) {
      clearBtn.addEventListener('click', () => {
        let jobs = loadJobs();
        jobs = jobs.filter(j => j.status !== 'completed');
        saveJobs(jobs);
        // Dispatch synthetic refresh
        window.location.reload();
      });
    }
  }

  // Hook into form analysis to store lastAnalysis metadata
  document.addEventListener('DOMContentLoaded', () => {
    checkServerMode();

    const analyzeForm = document.getElementById('analyze-form');
    if (analyzeForm) {
      analyzeForm.addEventListener('submit', () => {
        const input = document.getElementById('url-input');
        if (input && input.value) {
          window.lastSubmittedUrl = input.value.trim();
        }
      });
    }
  });

  // Export engine utilities
  window.DownloaditEngine = {
    isServerless: () => isServerlessMode,
    loadJobs,
    saveJobs,
    setFolderHandle: (handle) => {
      window.currentFolderHandle = handle;
    }
  };

})();
