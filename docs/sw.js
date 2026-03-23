const CACHE_VERSION = 'v21';
const SHELL_CACHE_NAME = `telegram-pages-mirror-shell-${CACHE_VERSION}`;
const DATA_CACHE_NAME = `telegram-pages-mirror-data-${CACHE_VERSION}`;
const MEDIA_CACHE_NAME = `telegram-pages-mirror-media-${CACHE_VERSION}`;
const MEDIA_META_CACHE_NAME = `telegram-pages-mirror-media-meta-${CACHE_VERSION}`;
const CACHE_NAMES = [SHELL_CACHE_NAME, DATA_CACHE_NAME, MEDIA_CACHE_NAME, MEDIA_META_CACHE_NAME];
const MEDIA_CACHE_MAX_ENTRIES = 360;
const MEDIA_CACHE_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;
const MEDIA_CACHE_TRIM_INTERVAL_MS = 30 * 60 * 1000;
const STATIC_ASSETS = [
  './',
  './app.js',
  './style.css',
  './assets/fonts/fonts.css',
  './assets/fonts/manrope-cyrillic.woff2',
  './assets/fonts/manrope-latin.woff2',
  './assets/fonts/ibm-plex-mono-400-cyrillic.woff2',
  './assets/fonts/ibm-plex-mono-400-latin.woff2',
  './assets/fonts/ibm-plex-mono-500-cyrillic.woff2',
  './assets/fonts/ibm-plex-mono-500-latin.woff2',
  './manifest.webmanifest',
  './data/channels/index.json',
  './data/channels/pgp-official/media/channel-avatar.jpg',
];
let mediaTrimPromise = null;
let lastMediaTrimAt = 0;

function isSuccessfulResponse(response) {
  return Boolean(response) && response.status === 200 && response.type !== 'opaque';
}

function getCacheKey(request) {
  const url = new URL(request.url);
  url.searchParams.delete('t');
  return url.toString();
}

function getMediaMetaKey(request) {
  return `__media_meta__:${getCacheKey(request)}`;
}

async function writeMediaMeta(request) {
  const metaCache = await caches.open(MEDIA_META_CACHE_NAME);
  const key = getMediaMetaKey(request);
  const existing = await metaCache.match(key);
  let cachedAt = Date.now();

  if (existing) {
    try {
      const payload = await existing.json();
      if (payload && Number.isFinite(payload.cachedAt)) {
        cachedAt = payload.cachedAt;
      }
    } catch {
      cachedAt = Date.now();
    }
  }

  await metaCache.put(
    key,
    new Response(
      JSON.stringify({
        cachedAt,
        lastAccessedAt: Date.now(),
      }),
      {
        headers: {
          'content-type': 'application/json',
        },
      }
    )
  );
}

async function trimMediaCache(force = false) {
  const now = Date.now();
  if (!force && now - lastMediaTrimAt < MEDIA_CACHE_TRIM_INTERVAL_MS) {
    return;
  }
  if (mediaTrimPromise) {
    return mediaTrimPromise;
  }

  lastMediaTrimAt = now;
  mediaTrimPromise = (async () => {
    const mediaCache = await caches.open(MEDIA_CACHE_NAME);
    const metaCache = await caches.open(MEDIA_META_CACHE_NAME);
    const requests = await mediaCache.keys();
    const entries = [];

    for (const request of requests) {
      const fallbackTimestamp = Date.now();
      let cachedAt = fallbackTimestamp;
      let lastAccessedAt = fallbackTimestamp;

      try {
        const metaResponse = await metaCache.match(getMediaMetaKey(request));
        if (metaResponse) {
          const payload = await metaResponse.json();
          if (payload && Number.isFinite(payload.cachedAt)) {
            cachedAt = payload.cachedAt;
          }
          if (payload && Number.isFinite(payload.lastAccessedAt)) {
            lastAccessedAt = payload.lastAccessedAt;
          }
        }
      } catch {
        cachedAt = fallbackTimestamp;
        lastAccessedAt = fallbackTimestamp;
      }

      entries.push({
        request,
        key: getMediaMetaKey(request),
        cachedAt,
        lastAccessedAt,
      });
    }

    const expired = entries.filter((entry) => now - Math.max(entry.lastAccessedAt, entry.cachedAt) > MEDIA_CACHE_MAX_AGE_MS);
    const active = entries
      .filter((entry) => !expired.includes(entry))
      .sort((left, right) => left.lastAccessedAt - right.lastAccessedAt);
    const overflow = Math.max(0, active.length - MEDIA_CACHE_MAX_ENTRIES);
    const victims = [...expired, ...active.slice(0, overflow)];

    await Promise.all(
      victims.flatMap((entry) => [
        mediaCache.delete(entry.request),
        metaCache.delete(entry.key),
      ])
    );
  })().finally(() => {
    mediaTrimPromise = null;
  });

  return mediaTrimPromise;
}

async function cacheResponse(request, response, { cacheName, trackMedia = false } = {}) {
  if (!isSuccessfulResponse(response)) return response;

  const cache = await caches.open(cacheName);
  await cache.put(getCacheKey(request), response.clone());
  if (trackMedia) {
    await writeMediaMeta(request);
    void trimMediaCache();
  }
  return response;
}

async function matchCached(request, { cacheName, trackMedia = false } = {}) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(getCacheKey(request));
  if (cached && trackMedia) {
    void writeMediaMeta(request);
    void trimMediaCache();
  }
  return cached;
}

async function staleWhileRevalidate(request, options) {
  const cached = await matchCached(request, options);
  const networkFetch = fetch(request)
    .then((response) => cacheResponse(request, response, options))
    .catch(() => null);

  if (cached) {
    void networkFetch;
    return cached;
  }

  return networkFetch.then((response) => response || Response.error());
}

async function networkFirst(request, options) {
  try {
    const response = await fetch(request);
    return cacheResponse(request, response, options);
  } catch {
    const cached = await matchCached(request, options);
    return cached || Response.error();
  }
}

async function cacheFirst(request, options) {
  const cached = await matchCached(request, options);
  if (cached) {
    return cached;
  }

  const response = await fetch(request);
  return cacheResponse(request, response, options);
}

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(SHELL_CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((key) => !CACHE_NAMES.includes(key)).map((key) => caches.delete(key))))
      .then(() => trimMediaCache(true))
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

  const isMediaRequest = /\/data\/channels\/.+\/media\//i.test(url.pathname);
  const isDataRequest = url.pathname.includes('/data/') && !isMediaRequest;
  const isShellAsset =
    request.mode === 'navigate' ||
    /\.(?:html|css|js|webmanifest)$/i.test(url.pathname);

  if (isMediaRequest || /\.(?:svg|png|jpg|jpeg|gif|webp)$/i.test(url.pathname)) {
    event.respondWith(cacheFirst(request, { cacheName: MEDIA_CACHE_NAME, trackMedia: true }));
    return;
  }

  if (isDataRequest) {
    event.respondWith(networkFirst(request, { cacheName: DATA_CACHE_NAME }));
    return;
  }

  if (isShellAsset) {
    event.respondWith(staleWhileRevalidate(request, { cacheName: SHELL_CACHE_NAME }));
    return;
  }

  event.respondWith(staleWhileRevalidate(request, { cacheName: SHELL_CACHE_NAME }));
});
