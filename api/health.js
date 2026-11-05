export default function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  return res.status(200).json({
    status: 'ok',
    service: 'Downloadit Serverless API',
    features: [
      'browser-direct-download',
      'metadata-analyzer',
      'streaming-cors-proxy',
      'resumable-chunks',
      'folder-save-access'
    ],
    serverless: true,
    ytdlp_available: false
  });
}

