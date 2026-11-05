export default async function handler(req, res) {
  // Set CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Range');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Parse request body
  let body = req.body;
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch (e) {
      body = {};
    }
  }

  const targetUrl = (body && body.url ? body.url : (req.query && req.query.url ? req.query.url : '')).trim();

  if (!targetUrl) {
    return res.status(400).json({ error: 'Please provide a valid file URL.' });
  }

  let parsedUrl;
  try {
    parsedUrl = new URL(targetUrl);
    if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
      return res.status(400).json({ error: 'Only HTTP and HTTPS links are supported.' });
    }
  } catch (err) {
    return res.status(400).json({ error: 'Invalid URL format.' });
  }

  const defaultHeaders = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Encoding': 'identity',
  };

  try {
    let response;
    let headSucceeded = false;

    // Try HEAD first
    try {
      const headController = new AbortController();
      const timeoutId = setTimeout(() => headController.abort(), 8000);
      response = await fetch(targetUrl, {
        method: 'HEAD',
        headers: defaultHeaders,
        redirect: 'follow',
        signal: headController.signal
      });
      clearTimeout(timeoutId);

      if (response.ok || (response.status >= 200 && response.status < 400)) {
        headSucceeded = true;
      }
    } catch (e) {
      headSucceeded = false;
    }

    // If HEAD failed or gave 405 Method Not Allowed, try GET with Range: bytes=0-0
    if (!headSucceeded || !response) {
      const getController = new AbortController();
      const timeoutId = setTimeout(() => getController.abort(), 8000);
      response = await fetch(targetUrl, {
        method: 'GET',
        headers: { ...defaultHeaders, 'Range': 'bytes=0-0' },
        redirect: 'follow',
        signal: getController.signal
      });
      clearTimeout(timeoutId);

      // Cancel the body download immediately to save bandwidth
      if (response.body) {
        try {
          const reader = response.body.getReader();
          reader.cancel();
        } catch (_) {}
      }
    }

    if (!response || (!response.ok && response.status !== 206)) {
      return res.status(400).json({
        error: `Could not reach file: HTTP ${response ? response.status : 'error'}`
      });
    }

    const headers = response.headers;
    const finalUrl = response.url || targetUrl;

    // Extract filename from Content-Disposition
    let filename = '';
    const contentDisposition = headers.get('content-disposition');
    if (contentDisposition) {
      // RFC 5987 / RFC 6266 filename*=UTF-8''...
      const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;\s]+)/i);
      if (utf8Match && utf8Match[1]) {
        try {
          filename = decodeURIComponent(utf8Match[1]);
        } catch (_) {
          filename = utf8Match[1];
        }
      } else {
        const standardMatch = contentDisposition.match(/filename=["']?([^"';\n]+)["']?/i);
        if (standardMatch && standardMatch[1]) {
          filename = standardMatch[1].trim();
        }
      }
    }

    // Fallback: extract filename from URL path
    if (!filename) {
      try {
        const u = new URL(finalUrl);
        const segments = u.pathname.split('/').filter(Boolean);
        if (segments.length > 0) {
          filename = decodeURIComponent(segments[segments.length - 1]);
        }
      } catch (_) {}
    }

    // Clean filename
    if (filename) {
      filename = filename.replace(/[<>:"/\\|?*]/g, '_').trim();
    }
    if (!filename || filename === '.' || filename === '/') {
      filename = 'download.bin';
    }

    // Extract file size
    let size = -1;
    const contentRange = headers.get('content-range');
    if (contentRange) {
      const totalMatch = contentRange.match(/\/(\d+)/);
      if (totalMatch && totalMatch[1]) {
        size = parseInt(totalMatch[1], 10);
      }
    }
    if (size <= 0) {
      const contentLength = headers.get('content-length');
      if (contentLength) {
        const parsed = parseInt(contentLength, 10);
        if (!isNaN(parsed) && parsed > 0) {
          size = parsed;
        }
      }
    }

    // Content Type / MIME
    let mime = (headers.get('content-type') || 'application/octet-stream').split(';')[0].trim().toLowerCase();

    // Determine kind
    const ext = filename.includes('.') ? filename.split('.').pop().toLowerCase() : '';
    let kind = 'file';

    const videoExts = ['mp4', 'mkv', 'webm', 'avi', 'mov', 'flv', 'm4v', '3gp', 'ts'];
    const audioExts = ['mp3', 'wav', 'flac', 'aac', 'm4a', 'ogg', 'wma', 'opus'];
    const archiveExts = ['zip', 'rar', '7z', 'tar', 'gz', 'bz2', 'xz', 'tgz', 'zst'];
    const docExts = ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt', 'rtf', 'csv', 'epub'];
    const installExts = ['exe', 'msi', 'dmg', 'pkg', 'deb', 'rpm', 'apk', 'iso', 'img', 'appimage'];
    const imgExts = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'bmp', 'ico', 'avif'];

    if (mime.startsWith('video/') || videoExts.includes(ext)) {
      kind = 'video';
    } else if (mime.startsWith('audio/') || audioExts.includes(ext)) {
      kind = 'audio';
    } else if (archiveExts.includes(ext) || mime.includes('zip') || mime.includes('compressed') || mime.includes('tar') || mime.includes('archive')) {
      kind = 'archive';
    } else if (docExts.includes(ext) || mime.includes('pdf') || mime.includes('word') || mime.includes('document')) {
      kind = 'document';
    } else if (installExts.includes(ext)) {
      kind = 'installer';
    } else if (mime.startsWith('image/') || imgExts.includes(ext)) {
      kind = 'image';
    }

    // Formatted size
    let sizeLabel = 'Unknown size';
    if (size > 0) {
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      let v = size;
      let i = 0;
      while (v >= 1024 && i < units.length - 1) {
        v /= 1024;
        i++;
      }
      sizeLabel = `${v.toFixed(1)} ${units[i]}`;
    }

    const acceptRanges = headers.get('accept-ranges');
    const resumable = acceptRanges === 'bytes' || Boolean(contentRange) || size > 0;

    return res.status(200).json({
      url: finalUrl,
      name: filename,
      size: size,
      size_label: sizeLabel,
      mime: mime,
      kind: kind,
      resumable: resumable,
      connections: resumable ? 4 : 1,
      qualities: []
    });

  } catch (err) {
    return res.status(400).json({
      error: `Could not analyze link: ${err.message || 'Connection failed'}`
    });
  }
}

