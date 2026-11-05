import fs from 'fs';
fs.cpSync('web', 'public', { recursive: true });
console.log('Static web assets prepared in public/ for Vercel deployment.');

