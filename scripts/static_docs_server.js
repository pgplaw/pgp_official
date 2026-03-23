const http = require('http');
const fs = require('fs');
const path = require('path');

const docsRoot = path.resolve(__dirname, '..', 'docs');
const portArg = process.argv.find((entry) => entry.startsWith('--port='));
const port = Number.parseInt(
  process.env.PLAYWRIGHT_PORT || (portArg ? portArg.slice('--port='.length) : '4173'),
  10,
);

const mimeTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.jpeg', 'image/jpeg'],
  ['.jpg', 'image/jpeg'],
  ['.js', 'application/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.png', 'image/png'],
  ['.svg', 'image/svg+xml'],
  ['.webmanifest', 'application/manifest+json; charset=utf-8'],
  ['.webp', 'image/webp'],
  ['.woff', 'font/woff'],
  ['.woff2', 'font/woff2'],
]);

function resolveTargetPath(urlPath) {
  const pathname = decodeURIComponent((urlPath || '/').split('?')[0].split('#')[0]);
  const normalizedPath = pathname === '/' ? '/index.html' : pathname;
  const resolvedPath = path.resolve(docsRoot, `.${normalizedPath}`);
  if (!resolvedPath.startsWith(docsRoot)) {
    return null;
  }
  return resolvedPath;
}

function sendFile(response, filePath) {
  const contentType = mimeTypes.get(path.extname(filePath).toLowerCase()) || 'application/octet-stream';
  response.writeHead(200, {
    'Content-Type': contentType,
    'Cache-Control': 'no-cache, no-store, must-revalidate',
  });
  fs.createReadStream(filePath).pipe(response);
}

const server = http.createServer((request, response) => {
  const resolvedPath = resolveTargetPath(request.url);
  if (!resolvedPath) {
    response.writeHead(403);
    response.end('Forbidden');
    return;
  }

  let filePath = resolvedPath;
  if (fs.existsSync(filePath) && fs.statSync(filePath).isDirectory()) {
    filePath = path.join(filePath, 'index.html');
  }

  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    response.writeHead(404);
    response.end('Not found');
    return;
  }

  sendFile(response, filePath);
});

server.listen(port, '127.0.0.1', () => {
  process.stdout.write(`static-docs-server listening on http://127.0.0.1:${port}\n`);
});
