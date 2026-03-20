const CACHE_NAME = 'telegram-pages-mirror-v17';
const STATIC_ASSETS = [
  './',
  './app.js',
  './style.css',
  './manifest.webmanifest',
  './data/channels/index.json',
  './data/channels/pgp-official/media/channel-avatar.jpg',
];

function isSuccessfulResponse(response) {
  return Boolean(response) && response.status === 200 && response.type !== 'opaque';
}

function getCacheKey(request) {
  const url = new URL(request.url);
  url.searchParams.delete('t');
  return url.toString();
}

async function cacheResponse(request, response) {
  if (!isSuccessfulResponse(response)) return response;

  const cache = await caches.open(CACHE_NAME);
  await cache.put(getCacheKey(request), response.clone());
  return response;
}

async function matchCached(request) {
  const cache = await caches.open(CACHE_NAME);
  return cache.match(getCacheKey(request));
}

async function staleWhileRevalidate(request) {
  const cached = await matchCached(request);
  const networkFetch = fetch(request)
    .then((response) => cacheResponse(request, response))
    .catch(() => null);

  if (cached) {
    void networkFetch;
    return cached;
  }

  return networkFetch.then((response) => response || Response.error());
}

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    return cacheResponse(request, response);
  } catch {
    const cached = await matchCached(request);
    return cached || Response.error();
  }
}

async function cacheFirst(request) {
  const cached = await matchCached(request);
  if (cached) {
    return cached;
  }

  const response = await fetch(request);
  return cacheResponse(request, response);
}

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const request = event.request;
  const url = new URL(request.url);
  const isLocalAsset = url.origin === self.location.origin;
  if (!isLocalAsset) {
    return;
  }

  const isDataRequest = url.pathname.includes('/data/');
  const isMediaRequest = /\/data\/channels\/.+\/media\//i.test(url.pathname);
  const isShellAsset =
    request.mode === 'navigate' ||
    /\.(?:html|css|js|webmanifest)$/i.test(url.pathname);

  if (isDataRequest) {
    event.respondWith(networkFirst(request));
    return;
  }

  if (isShellAsset) {
    event.respondWith(staleWhileRevalidate(request));
    return;
  }

  if (isMediaRequest || /\.(?:svg|png|jpg|jpeg|gif|webp)$/i.test(url.pathname)) {
    event.respondWith(cacheFirst(request));
    return;
  }

  event.respondWith(staleWhileRevalidate(request));
});
