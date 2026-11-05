export default function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method === 'GET') {
    return res.status(200).json({ downloads: [] });
  }

  if (req.method === 'POST') {
    let body = req.body;
    if (typeof body === 'string') {
      try { body = JSON.parse(body); } catch (_) { body = {}; }
    }
    const id = 'job_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    return res.status(200).json({
      id: id,
      name: (body && body.name) ? body.name : 'download.bin',
      url: (body && body.url) ? body.url : '',
      size: 0,
      downloaded: 0,
      status: 'downloading',
      scan: 'clean',
      error: '',
      created: Date.now(),
      updated: Date.now(),
      speed: 0,
      eta: 0,
      segments: []
    });
  }

  return res.status(200).json({ ok: true });
}

