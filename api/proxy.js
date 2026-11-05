import { Readable } from 'node:stream';

export default async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Range, Content-Type');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Content-Disposition, Accept-Ranges, Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const targetUrl = (req.query && req.query.url ? req.query.url : '').trim();
  if (!targetUrl) {
    return res.status(400).json({ error: 'Missing url parameter.' });
  }

  try {
    const forwardHeaders = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Accept': '*/*',
    };

    if (req.headers['range']) {
      forwardHeaders['Range'] = req.headers['range'];
    }

    const upstream = await fetch(targetUrl, {
      method: req.method === 'HEAD' ? 'HEAD' : 'GET',
      headers: forwardHeaders,
      redirect: 'follow',
    });

    res.status(upstream.status);

    const copyHeader = (name) => {
      const val = upstream.headers.get(name);
      if (val) res.setHeader(name, val);
    };

    copyHeader('content-type');
    copyHeader('content-length');
    copyHeader('content-range');
    copyHeader('content-disposition');
    copyHeader('accept-ranges');
    copyHeader('last-modified');
    copyHeader('etag');

    if (req.method === 'HEAD' || !upstream.body) {
      return res.end();
    }

    // Stream body to response
    const nodeStream = Readable.fromWeb(upstream.body);
    nodeStream.pipe(res);

    nodeStream.on('error', (err) => {
      console.error('Proxy stream error:', err);
      if (!res.headersSent) {
        res.status(500).end();
      } else {
        res.end();
      }
    });

    req.on('close', () => {
      nodeStream.destroy();
    });

  } catch (err) {
    console.error('Proxy error:', err);
    if (!res.headersSent) {
      res.status(502).json({ error: `Proxy failed: ${err.message}` });
    } else {
      res.end();
    }
  }
}

