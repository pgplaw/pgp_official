'use strict';

const CHANNELS_INDEX_URL = 'data/channels/index.json';
const DEFAULT_PAGE_SIZE = 16;
const AUTO_REFRESH_INTERVAL_MINUTES = 5;
const SYNC_STATUS_POLL_INTERVAL_MS = 30 * 1000;
const LONG_PRESS_COPY_DELAY_MS = 650;
const CHANNEL_CAROUSEL_TRANSITION_MS = 430;
const CHANNEL_CONTENT_FADE_OUT_MS = 220;
const CHANNEL_CONTENT_FADE_IN_DELAY_MS = 36;
const CHANNEL_MOBILE_CONTENT_FADE_OUT_MS = 120;
const CHANNEL_MOBILE_CONTENT_FADE_IN_DELAY_MS = 12;
const CHANNEL_DESKTOP_CONTENT_FADE_OUT_MS = 150;
const CHANNEL_DESKTOP_CONTENT_FADE_IN_DELAY_MS = 18;
const VIEWER_TRANSITION_MS = 360;
const FEED_CACHE_MAX_ENTRIES = 6;
const SCROLL_TOP_VISIBILITY_THRESHOLD_MIN = 360;
const SCROLL_TOP_VISIBILITY_THRESHOLD_MAX = 720;
const POST_ANCHOR_GAP_DESKTOP = 10;
const POST_ANCHOR_GAP_MOBILE = 8;

const state = {
  catalog: null,
  activeChannelKey: null,
  feed: null,
  posts: [],
  rendered: 0,
  loadedPages: new Set(),
  totalPages: 1,
  totalPosts: 0,
  pageSize: DEFAULT_PAGE_SIZE,
  activeFeedBuildId: '',
  feedRenderVersion: 0,
  pageLoadPromises: new Map(),
  nextPagePrefetchHandle: null,
  channelFeedCache: new Map(),
  channelFeedPrefetchPromises: new Map(),
  channelFeedPrefetchHandle: null,
  activeFeedManual: false,
  viewerItems: [],
  viewerIndex: 0,
  mediaRegistry: {},
  syncStatusPollId: null,
  deferredInstallPrompt: null,
  copyToastTimeoutId: null,
  postHighlightTimeoutId: null,
  appendNextPagePromise: null,
  channelAccentCache: {},
  channelCarouselTouch: null,
  channelCarouselAnimating: false,
  channelCarouselTransition: null,
  channelCarouselAutotest: null,
  channelCarouselListOpen: false,
  channelCarouselClickSuppressUntil: 0,
  viewerPointer: null,
  viewerAnimating: false,
  viewerTransitionTimerId: null,
  scrollTopButtonVisible: false,
  scrollTopButtonSyncFrameId: null,
  postAnchorOffsetSyncFrameId: null,
};

const elements = {
  channelMenu: document.getElementById('channelMenu'),
  channelCarousel: document.getElementById('channelCarousel'),
  channelNav: document.querySelector('.channel-nav'),
  siteShell: document.querySelector('.site-shell'),
  siteTitle: document.getElementById('siteTitle'),
  siteDescription: document.getElementById('siteDescription'),
  channelAvatarWrap: document.getElementById('channelAvatarWrap'),
  channelAvatar: document.getElementById('channelAvatar'),
  channelLink: document.getElementById('channelLink'),
  updatedText: document.getElementById('updatedText'),
  refreshButton: document.getElementById('refreshButton'),
  themeToggle: document.getElementById('themeToggle'),
  feedView: document.getElementById('feedView'),
  postFeed: document.getElementById('postFeed'),
  loadingState: document.getElementById('loadingState'),
  emptyState: document.getElementById('emptyState'),
  errorState: document.getElementById('errorState'),
  errorMessage: document.getElementById('errorMessage'),
  loadMoreWrap: document.getElementById('loadMoreWrap'),
  loadMoreButton: document.getElementById('loadMoreButton'),
  commentsView: document.getElementById('commentsView'),
  commentsTitle: document.getElementById('commentsTitle'),
  commentsStatus: document.getElementById('commentsStatus'),
  commentsList: document.getElementById('commentsList'),
  commentsLoading: document.getElementById('commentsLoading'),
  commentsEmpty: document.getElementById('commentsEmpty'),
  backButton: document.getElementById('backButton'),
  viewer: document.getElementById('viewer'),
  viewerClose: document.getElementById('viewerClose'),
  viewerPrev: document.getElementById('viewerPrev'),
  viewerNext: document.getElementById('viewerNext'),
  viewerContent: document.getElementById('viewerContent'),
  installAppButton: document.getElementById('installAppButton'),
  copyToast: document.getElementById('copyToast'),
  scrollTopButton: document.getElementById('scrollTopButton'),
};

const IMAGE_DEBUG_ENABLED = (() => {
  try {
    const params = new URLSearchParams(window.location.search);
    return params.get('imageDebug') === '1' || window.localStorage.getItem('pep-image-debug') === '1';
  } catch {
    return false;
  }
})();

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatDate(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('ru-RU', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function timeAgo(iso) {
  if (!iso) return '—';
  const deltaMinutes = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
  if (deltaMinutes < 2) return 'только что';
  if (deltaMinutes < 60) return `${deltaMinutes} мин назад`;
  const deltaHours = Math.floor(deltaMinutes / 60);
  if (deltaHours < 24) return `${deltaHours} ч назад`;
  const deltaDays = Math.floor(deltaHours / 24);
  return `${deltaDays} дн назад`;
}

function compactNumber(value) {
  const number = Number(value || 0);
  if (!number) return '0';
  if (number >= 1000000) return `${(number / 1000000).toFixed(1)}M`;
  if (number >= 1000) return `${(number / 1000).toFixed(1)}K`;
  return String(number);
}

function linkifyText(text) {
  const source = String(text || '');
  const pattern = /(?<![@\w])((?:https?:\/\/)?(?:www\.)?(?:[a-zа-яё0-9-]+\.)+[a-zа-яё]{2,}(?:\/[^\s<]*)?)/giu;

  let lastIndex = 0;
  let result = '';

  for (const match of source.matchAll(pattern)) {
    const matchedText = match[0];
    const startIndex = match.index ?? 0;

    result += escapeHtml(source.slice(lastIndex, startIndex));

    let visibleText = matchedText;
    let trailingPunctuation = '';

    while (/[),.!?:;]$/.test(visibleText)) {
      trailingPunctuation = visibleText.slice(-1) + trailingPunctuation;
      visibleText = visibleText.slice(0, -1);
    }

    const href = /^(?:https?:)?\/\//i.test(visibleText)
      ? visibleText
      : `https://${visibleText}`;

    result += `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(visibleText)}</a>`;
    result += escapeHtml(trailingPunctuation);
    lastIndex = startIndex + matchedText.length;
  }

  result += escapeHtml(source.slice(lastIndex));
  return result.replace(/\r?\n/g, '<br>');
}

function linkifyTelegramAwareText(text) {
  const source = String(text || '');
  const pattern = /(?<![@.\w])((?:https?:\/\/)?(?:www\.)?(?:[a-zР°-СЏС‘0-9-]+\.)+[a-zР°-СЏС‘]{2,}(?:\/[^\s<]*)?)|(?<![@.\w/])(@[A-Za-z][A-Za-z0-9_]{4,31})/giu;

  let lastIndex = 0;
  let result = '';

  for (const match of source.matchAll(pattern)) {
    const matchedText = match[0];
    const startIndex = match.index ?? 0;
    const matchedUrl = match[1];
    const matchedHandle = match[2];

    result += escapeHtml(source.slice(lastIndex, startIndex));

    if (matchedUrl) {
      let visibleText = matchedUrl;
      let trailingPunctuation = '';

      while (/[),.!?:;]$/.test(visibleText)) {
        trailingPunctuation = visibleText.slice(-1) + trailingPunctuation;
        visibleText = visibleText.slice(0, -1);
      }

      const href = /^(?:https?:)?\/\//i.test(visibleText)
        ? visibleText
        : `https://${visibleText}`;

      result += `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(visibleText)}</a>`;
      result += escapeHtml(trailingPunctuation);
    } else if (matchedHandle) {
      const username = matchedHandle.slice(1);
      result += `<a href="https://t.me/${escapeHtml(username)}" target="_blank" rel="noopener noreferrer">${escapeHtml(matchedHandle)}</a>`;
    } else {
      result += escapeHtml(matchedText);
    }

    lastIndex = startIndex + matchedText.length;
  }

  result += escapeHtml(source.slice(lastIndex));
  return result.replace(/\r?\n/g, '<br>');
}

function normalizeComparableUrl(value) {
  try {
    const url = new URL(String(value || '').trim(), window.location.href);
    if (!/^https?:$/i.test(url.protocol)) return null;
    const pathname = url.pathname.replace(/\/+$/, '') || '/';
    return `${url.protocol}//${url.host.toLowerCase()}${pathname}${url.search}`;
  } catch {
    return null;
  }
}

function isUrlLikeLabel(value, href = '') {
  const label = String(value || '').trim();
  if (!label) return false;
  if (/^(?:https?:\/\/|www\.)\S+$/i.test(label)) return true;

  const normalizedLabel = normalizeComparableUrl(label);
  const normalizedHref = normalizeComparableUrl(href);
  return Boolean(normalizedLabel && normalizedHref && normalizedLabel === normalizedHref);
}

function extractInlineEmojiValue(element) {
  if (!element || element.nodeType !== Node.ELEMENT_NODE) return '';

  const candidates = [
    element.getAttribute('alt'),
    element.getAttribute('data-content'),
    element.getAttribute('aria-label'),
    element.getAttribute('emoji-text'),
    element.getAttribute('title'),
    element.textContent,
  ];

  for (const candidate of candidates) {
    const value = String(candidate || '').trim();
    if (!value) continue;
    if (/^https?:\/\//i.test(value)) continue;
    return value;
  }

  return '';
}

function isInlineEmojiElement(element) {
  if (!element || element.nodeType !== Node.ELEMENT_NODE) return false;

  const tagName = element.tagName.toLowerCase();
  if (tagName === 'tg-emoji') return true;

  const className = element.getAttribute('class') || '';
  const src = element.getAttribute('src') || '';
  const hasEmojiClass = /\b(?:emoji|custom-emoji|tg-emoji|animated-emoji)\b/i.test(className);

  if (tagName === 'img') {
    return hasEmojiClass || /emoji/i.test(src) || Boolean(extractInlineEmojiValue(element));
  }

  return hasEmojiClass;
}

function replaceInlineEmojiElements(root) {
  if (!root) return;

  root.querySelectorAll('img, tg-emoji, span, i').forEach((element) => {
    if (!isInlineEmojiElement(element)) return;

    const replacement = extractInlineEmojiValue(element);
    element.replaceWith(document.createTextNode(replacement));
  });
}

function shouldInsertSpaceBetweenAnchorSegments(leftText, rightText) {
  const left = String(leftText || '');
  const right = String(rightText || '');
  if (!left || !right) return false;

  return /[0-9A-Za-zА-Яа-яЁё»"')\]]$/.test(left) && /^[0-9A-Za-zА-Яа-яЁё«"(]/.test(right);
}

function mergeAdjacentDuplicateAnchors(root) {
  if (!root) return;

  const anchors = [...root.querySelectorAll('a[href]')];
  anchors.forEach((anchor) => {
    if (!anchor.parentNode) return;

    let currentHref = normalizeComparableUrl(anchor.getAttribute('href') || anchor.href);
    if (!currentHref) return;

    while (true) {
      let separatorNode = null;
      let nextNode = anchor.nextSibling;

      if (nextNode && nextNode.nodeType === Node.TEXT_NODE && /^[\s\u00A0]*$/.test(nextNode.textContent || '')) {
        separatorNode = nextNode;
        nextNode = nextNode.nextSibling;
      }

      if (!nextNode || nextNode.nodeType !== Node.ELEMENT_NODE || nextNode.tagName !== 'A') {
        break;
      }

      const nextHref = normalizeComparableUrl(nextNode.getAttribute('href') || nextNode.href);
      if (!nextHref || nextHref !== currentHref) {
        break;
      }

      const separatorText = separatorNode ? (separatorNode.textContent || '') : '';
      const joiner = separatorText || (
        shouldInsertSpaceBetweenAnchorSegments(anchor.textContent, nextNode.textContent) ? ' ' : ''
      );

      if (joiner) {
        anchor.appendChild(document.createTextNode(joiner));
      }

      while (nextNode.firstChild) {
        anchor.appendChild(nextNode.firstChild);
      }

      if (separatorNode) {
        separatorNode.remove();
      }
      nextNode.remove();
      currentHref = normalizeComparableUrl(anchor.getAttribute('href') || anchor.href);
      if (!currentHref) break;
    }
  });
}

function normalizePostHtml(html) {
  if (!html || typeof document === 'undefined') return html || '';

  const template = document.createElement('template');
  template.innerHTML = html;
  replaceInlineEmojiElements(template.content);
  const anchors = [...template.content.querySelectorAll('a[href]')];
  const namedHrefs = new Set();

  anchors.forEach((anchor) => {
    const rawHref = anchor.getAttribute('href') || anchor.href;
    const href = normalizeComparableUrl(rawHref);
    const label = anchor.textContent.trim();
    if (href && label && !isUrlLikeLabel(label, rawHref)) {
      namedHrefs.add(href);
    }
  });

  anchors.forEach((anchor) => {
    const rawHref = anchor.getAttribute('href') || anchor.href;
    const href = normalizeComparableUrl(rawHref);
    const label = anchor.textContent.trim();
    if (!href || !isUrlLikeLabel(label, rawHref)) return;

    const nextSibling = anchor.nextSibling;
    const previousSibling = anchor.previousSibling;
    const attachedText =
      nextSibling &&
      nextSibling.nodeType === Node.TEXT_NODE &&
      /^[^\s]/.test(nextSibling.textContent || '');
    const namedDuplicate = namedHrefs.has(href);
    if (!attachedText && !namedDuplicate) return;

    const shouldRestoreBreak =
      attachedText &&
      previousSibling &&
      previousSibling.nodeType === Node.ELEMENT_NODE &&
      previousSibling.tagName === 'BR';

    if (nextSibling && nextSibling.nodeType === Node.TEXT_NODE) {
      nextSibling.textContent = (nextSibling.textContent || '').replace(/^\s+/, '');
    }
    anchor.remove();
    if (shouldRestoreBreak && nextSibling?.parentNode) {
      nextSibling.parentNode.insertBefore(document.createElement('br'), nextSibling);
    }
  });

  mergeAdjacentDuplicateAnchors(template.content);

  return template.innerHTML
    .replace(/<\/a><br>(?=(?:\s|&nbsp;)*(?:▫️|🔘|📌|👉|#|[A-ZА-ЯЁ]))/g, '</a><br><br>')
    .replace(/(?:<br\s*\/?>\s*){3,}/gi, '<br><br>')
    .trim();
}

function normalizePostHtmlSpacing(html) {
  return String(html || '')
    .replace(/(?<=[0-9A-Za-zА-Яа-яЁё«»„“"'()])(?=<a\b)/g, ' ')
    .replace(/(?<=<\/a>)(?=[0-9A-Za-zА-Яа-яЁё«»„“"'(])/g, ' ');
}

function normalizePhoto(photo) {
  if (!photo) return null;
  if (typeof photo === 'string') {
    return { thumb_url: photo, feed_url: photo, full_url: photo };
  }

  const parseDimension = (value) => {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  };

  const thumbUrl = photo.thumb_url || photo.thumb || photo.url || photo.full_url || photo.full;
  const feedUrl = photo.feed_url || photo.feed || photo.full_url || photo.full || photo.url || thumbUrl;
  const fullUrl = photo.full_url || photo.full || photo.url || photo.thumb_url || photo.thumb;
  if (!thumbUrl && !fullUrl) return null;

  return {
    thumb_url: thumbUrl || fullUrl,
    feed_url: feedUrl || fullUrl || thumbUrl,
    full_url: fullUrl || thumbUrl,
    source_url: typeof photo.source_url === 'string' ? photo.source_url : '',
    thumb_width: parseDimension(photo.thumb_width),
    thumb_height: parseDimension(photo.thumb_height),
    feed_width: parseDimension(photo.feed_width),
    feed_height: parseDimension(photo.feed_height),
    full_width: parseDimension(photo.full_width),
    full_height: parseDimension(photo.full_height),
    source_width: parseDimension(photo.source_width),
    source_height: parseDimension(photo.source_height),
  };
}

function normalizeVideo(video) {
  if (!video) return null;
  if (typeof video === 'string') {
    const normalizedUrl = String(video).trim();
    return normalizedUrl ? { url: normalizedUrl, source_url: normalizedUrl } : null;
  }
  if (typeof video !== 'object') return null;

  const parseDimension = (value) => {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  };

  const url = String(video.url || video.video_url || video.src || video.full_url || '').trim();
  if (!url) return null;

  return {
    url,
    source_url: typeof video.source_url === 'string'
      ? video.source_url.trim()
      : (/^https?:\/\//i.test(url) ? url : ''),
    width: parseDimension(video.width),
    height: parseDimension(video.height),
    poster: normalizePhoto(video.poster || video.video_poster),
  };
}

function getAttachedVideos(post) {
  return Array.isArray(post?.videos) ? post.videos.map(normalizeVideo).filter(Boolean) : [];
}

function normalizeLinkPreview(preview) {
  if (!preview || typeof preview !== 'object') return null;

  const href = String(preview.href || '').trim();
  if (!href) return null;

  const entry = {
    href,
    title: String(preview.title || '').trim(),
    description: String(preview.description || '').trim(),
    site_name: String(preview.site_name || '').trim(),
    host: String(preview.host || '').trim(),
    is_video: Boolean(preview.is_video),
    image: normalizePhoto(preview.image),
  };

  if (!entry.title && !entry.description && !entry.image) return null;
  return entry;
}

function isTruthyMediaFlag(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value > 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    return normalized === 'true' || normalized === '1' || normalized === 'yes';
  }
  return false;
}

function hasVisiblePostBody(post) {
  const html = String(post?.text_html || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
  const text = String(post?.text || '').replace(/\s+/g, ' ').trim();
  return Boolean(html || text);
}

function hasPhysicalPostMedia(post) {
  const photoCount = Array.isArray(post?.photos) ? post.photos.filter(Boolean).length : 0;
  return photoCount > 0 || Boolean(post?.video_url) || getAttachedVideos(post).length > 0;
}

function isSquareLikeDimensions(width, height, tolerance = 0.12) {
  const parsedWidth = Number.parseInt(width, 10);
  const parsedHeight = Number.parseInt(height, 10);
  if (!Number.isFinite(parsedWidth) || !Number.isFinite(parsedHeight) || parsedWidth <= 0 || parsedHeight <= 0) {
    return false;
  }

  return Math.abs(parsedWidth - parsedHeight) / Math.max(parsedWidth, parsedHeight) <= tolerance;
}

function hasSquareRoundVideoHint(post) {
  if (!post) return false;

  if (isSquareLikeDimensions(post.video_width, post.video_height)) {
    return true;
  }

  const poster = normalizePhoto(post.video_poster);
  if (!poster) return false;

  const posterWidth = poster.feed_width || poster.full_width || poster.thumb_width || poster.source_width;
  const posterHeight = poster.feed_height || poster.full_height || poster.thumb_height || poster.source_height;
  return isSquareLikeDimensions(posterWidth, posterHeight);
}

function isRoundVideoPost(post) {
  if (!post?.video_url) return false;

  const photoCount = Array.isArray(post.photos) ? post.photos.filter(Boolean).length : 0;
  const attachedVideoCount = getAttachedVideos(post).length;
  if (photoCount > 0 || attachedVideoCount > 0) return false;

  const explicitHint = isTruthyMediaFlag(post.video_note) || Boolean(post.video_poster);
  const urlHint = /video-note/i.test(String(post.video_url || ''));
  const squareHint = hasSquareRoundVideoHint(post);
  const noCompetingContent = !post.reply_to && !post.forwarded_from && !hasVisiblePostBody(post);

  return explicitHint || urlHint || squareHint || noCompetingContent;
}

function buildCopyPostButtonMarkup(postAnchorUrl) {
  return `
    <button
      class="post-card__copy icon-button icon-button--post-copy"
      type="button"
      data-copy-post-url="${escapeHtml(postAnchorUrl)}"
      aria-label="Скопировать ссылку на пост"
      title="Скопировать ссылку на пост"
    >
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <rect x="9" y="9" width="10" height="10" rx="2"></rect>
        <path d="M7 15H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h7a2 2 0 0 1 2 2v1"></path>
      </svg>
    </button>
  `;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function componentToHex(value) {
  return clamp(Math.round(value), 0, 255).toString(16).padStart(2, '0');
}

function rgbToHex(r, g, b) {
  return `#${componentToHex(r)}${componentToHex(g)}${componentToHex(b)}`;
}

function hexToRgb(hex) {
  const value = String(hex || '').trim().replace(/^#/, '');
  if (!/^[\da-f]{6}$/i.test(value)) return null;
  return {
    r: Number.parseInt(value.slice(0, 2), 16),
    g: Number.parseInt(value.slice(2, 4), 16),
    b: Number.parseInt(value.slice(4, 6), 16),
  };
}

function mixHexColors(baseHex, targetHex, ratio) {
  const base = hexToRgb(baseHex);
  const target = hexToRgb(targetHex);
  if (!base || !target) return baseHex;
  const weight = clamp(ratio, 0, 1);
  return rgbToHex(
    base.r + (target.r - base.r) * weight,
    base.g + (target.g - base.g) * weight,
    base.b + (target.b - base.b) * weight,
  );
}

function hexToRgba(hex, alpha) {
  const rgb = hexToRgb(hex);
  if (!rgb) return `rgba(145, 39, 141, ${alpha})`;
  return `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${alpha})`;
}

function normalizeAccentHex(hex) {
  const rgb = hexToRgb(hex);
  if (!rgb) return '#91278d';

  const max = Math.max(rgb.r, rgb.g, rgb.b);
  const min = Math.min(rgb.r, rgb.g, rgb.b);
  const brightness = (rgb.r + rgb.g + rgb.b) / 3;
  const chroma = max - min;

  let normalized = rgbToHex(rgb.r, rgb.g, rgb.b);

  if (chroma < 24) {
    normalized = mixHexColors(normalized, '#91278d', 0.42);
  }

  if (brightness > 214) {
    normalized = mixHexColors(normalized, '#2b233f', 0.34);
  } else if (brightness < 62) {
    normalized = mixHexColors(normalized, '#ffffff', 0.2);
  }

  return normalized;
}

function buildChannelAccentStyle(channel) {
  const accentHex = normalizeAccentHex(channel.accent_color || state.channelAccentCache[channel.key] || '#91278d');
  const strongHex = mixHexColors(accentHex, '#1f1934', 0.24);
  const borderColor = hexToRgba(accentHex, 0.34);
  const glowColor = hexToRgba(accentHex, 0.28);
  const ringColor = hexToRgba(accentHex, 0.16);

  return [
    `--channel-active:${accentHex}`,
    `--channel-active-strong:${strongHex}`,
    `--channel-active-border:${borderColor}`,
    `--channel-active-glow:${glowColor}`,
    `--channel-active-ring:${ringColor}`,
  ].join(';');
}

async function extractAccentColorFromImage(src) {
  if (!src) return null;

  return new Promise((resolve) => {
    const image = new Image();
    image.decoding = 'async';
    image.crossOrigin = 'anonymous';

    image.onload = () => {
      try {
        const canvas = document.createElement('canvas');
        canvas.width = 24;
        canvas.height = 24;
        const context = canvas.getContext('2d', { willReadFrequently: true });
        if (!context) {
          resolve(null);
          return;
        }

        context.drawImage(image, 0, 0, canvas.width, canvas.height);
        const data = context.getImageData(0, 0, canvas.width, canvas.height).data;

        let weightedR = 0;
        let weightedG = 0;
        let weightedB = 0;
        let weightedTotal = 0;
        let fallbackR = 0;
        let fallbackG = 0;
        let fallbackB = 0;
        let fallbackTotal = 0;

        for (let index = 0; index < data.length; index += 4) {
          const alpha = data[index + 3];
          if (alpha < 160) continue;

          const red = data[index];
          const green = data[index + 1];
          const blue = data[index + 2];

          fallbackR += red;
          fallbackG += green;
          fallbackB += blue;
          fallbackTotal += 1;

          const max = Math.max(red, green, blue);
          const min = Math.min(red, green, blue);
          const chroma = max - min;
          const brightness = (red + green + blue) / 3;

          if (brightness < 22 || brightness > 245) continue;

          const weight = Math.max(chroma, 18);
          weightedR += red * weight;
          weightedG += green * weight;
          weightedB += blue * weight;
          weightedTotal += weight;
        }

        if (weightedTotal > 0) {
          resolve(rgbToHex(weightedR / weightedTotal, weightedG / weightedTotal, weightedB / weightedTotal));
          return;
        }

        if (fallbackTotal > 0) {
          resolve(rgbToHex(fallbackR / fallbackTotal, fallbackG / fallbackTotal, fallbackB / fallbackTotal));
          return;
        }

        resolve(null);
      } catch (_) {
        resolve(null);
      }
    };

    image.onerror = () => resolve(null);
    image.src = src;
  });
}

async function ensureChannelAccent(channel) {
  const channelKey = channel?.key || state.activeChannelKey;
  const avatarPath = channel?.avatar_path;
  if (!channelKey) return;

  if (channel?.accent_color) {
    state.channelAccentCache[channelKey] = normalizeAccentHex(channel.accent_color);
    return;
  }

  if (!avatarPath || state.channelAccentCache[channelKey]) return;

  const accentHex = await extractAccentColorFromImage(avatarPath);
  if (!accentHex) return;

  state.channelAccentCache[channelKey] = accentHex;
  if (channelKey === state.activeChannelKey && !state.channelCarouselAnimating) {
    renderChannelMenu();
  }
}

function getChannelKeyFromLocation() {
  const url = new URL(window.location.href);
  return url.searchParams.get('channel');
}

function getCatalogChannels() {
  return state.catalog?.channels || [];
}

function getCatalogSite() {
  return state.catalog?.site || {};
}

function scrollPageToTop() {
  window.scrollTo({ top: 0, behavior: 'auto' });
  document.documentElement.scrollTop = 0;
  document.body.scrollTop = 0;
  queueScrollTopButtonVisibilitySync();
  queuePostAnchorOffsetSync();
}

function smoothScrollPageToTop() {
  window.scrollTo({
    top: 0,
    behavior: prefersReducedMotion() ? 'auto' : 'smooth',
  });
}

function getScrollTopVisibilityThreshold() {
  return clamp(
    Math.round(window.innerHeight * 0.72),
    SCROLL_TOP_VISIBILITY_THRESHOLD_MIN,
    SCROLL_TOP_VISIBILITY_THRESHOLD_MAX,
  );
}

function setScrollTopButtonVisibility(visible) {
  const button = elements.scrollTopButton;
  if (!button || state.scrollTopButtonVisible === visible) return;

  state.scrollTopButtonVisible = visible;
  button.classList.toggle('is-visible', visible);
  button.setAttribute('aria-hidden', visible ? 'false' : 'true');
}

function syncScrollTopButtonVisibility() {
  state.scrollTopButtonSyncFrameId = null;
  if (!elements.scrollTopButton) return;

  const pageScrollable = document.documentElement.scrollHeight - window.innerHeight > 240;
  const scrolled = window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0;
  const shouldShow = pageScrollable && scrolled >= getScrollTopVisibilityThreshold();
  setScrollTopButtonVisibility(shouldShow);
}

function queueScrollTopButtonVisibilitySync() {
  if (state.scrollTopButtonSyncFrameId) return;
  state.scrollTopButtonSyncFrameId = window.requestAnimationFrame(syncScrollTopButtonVisibility);
}

function nextRenderFrame() {
  return new Promise((resolve) => {
    requestAnimationFrame(() => {
      requestAnimationFrame(resolve);
    });
  });
}

function getPostAnchorGap() {
  return window.matchMedia('(max-width: 860px)').matches
    ? POST_ANCHOR_GAP_MOBILE
    : POST_ANCHOR_GAP_DESKTOP;
}

function queuePostAnchorOffsetSync() {
  if (state.postAnchorOffsetSyncFrameId != null) return;

  state.postAnchorOffsetSyncFrameId = window.requestAnimationFrame(() => {
    state.postAnchorOffsetSyncFrameId = null;
    syncPostAnchorOffset();
  });
}

function wait(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function setChannelContentSwitching(active, { fast = false, mode = null } = {}) {
  if (!elements.siteShell) return;

  const isActive = Boolean(active);
  elements.siteShell.classList.toggle('is-channel-switching', isActive);
  elements.siteShell.classList.toggle('is-channel-switching-fast', isActive && Boolean(fast));
  elements.siteShell.classList.toggle('is-channel-switching-mobile', isActive && mode === 'mobile');
  elements.siteShell.classList.toggle('is-channel-switching-desktop', isActive && mode === 'desktop');
  if (!isActive) {
    elements.siteShell.classList.remove('is-channel-switching-fast');
    elements.siteShell.classList.remove('is-channel-switching-mobile');
    elements.siteShell.classList.remove('is-channel-switching-desktop');
  }
}

function getChannelSwitchTimings({ fast = false, desktopFast = false } = {}) {
  if (fast) {
    return {
      fadeOut: CHANNEL_MOBILE_CONTENT_FADE_OUT_MS,
      fadeInDelay: CHANNEL_MOBILE_CONTENT_FADE_IN_DELAY_MS,
    };
  }

  if (desktopFast) {
    return {
      fadeOut: CHANNEL_DESKTOP_CONTENT_FADE_OUT_MS,
      fadeInDelay: CHANNEL_DESKTOP_CONTENT_FADE_IN_DELAY_MS,
    };
  }

  return {
    fadeOut: CHANNEL_CONTENT_FADE_OUT_MS,
    fadeInDelay: CHANNEL_CONTENT_FADE_IN_DELAY_MS,
  };
}

function getChannelByKey(channelKey) {
  return getCatalogChannels().find((channel) => channel.key === channelKey) || null;
}

function getChannelIndex(channelKey) {
  return getCatalogChannels().findIndex((channel) => channel.key === channelKey);
}

function getRelativeChannelKey(offset) {
  const channels = getCatalogChannels();
  if (!channels.length) return null;

  const currentIndex = getChannelIndex(state.activeChannelKey);
  const safeIndex = currentIndex >= 0 ? currentIndex : 0;
  const nextIndex = (safeIndex + offset + channels.length) % channels.length;
  return channels[nextIndex]?.key || null;
}

function getChannelMenuLabels(channel) {
  const rawLabel = channel?.label || channel?.channel_title || channel?.channel_username || 'Channel';
  const parts = rawLabel.split('|').map((part) => part.trim()).filter(Boolean);
  return {
    rawLabel,
    title: parts[1] || channel?.menu_title || channel?.channel_title || rawLabel,
    subtitle: channel?.menu_subtitle || parts[0] || `@${channel?.channel_username || 'channel'}`,
  };
}

function resolveChannelKey(requestedKey) {
  const channels = getCatalogChannels();
  if (!channels.length) return null;
  if (requestedKey && getChannelByKey(requestedKey)) return requestedKey;
  return state.catalog?.default_channel_key || channels[0].key;
}

function updateChannelUrl(channelKey, { replace = false, clearHash = false } = {}) {
  const url = new URL(window.location.href);
  url.searchParams.set('channel', channelKey);
  if (clearHash) url.hash = '';

  if (replace) {
    window.history.replaceState({}, '', url);
  } else {
    window.history.pushState({}, '', url);
  }
}

function buildChannelRoot(channelKey) {
  return `data/channels/${channelKey}`;
}

function appendJsonVersionParams(path, { buildId = '', manual = false } = {}) {
  const searchParams = new URLSearchParams();
  if (buildId) {
    searchParams.set('v', buildId);
  }
  if (manual) {
    searchParams.set('t', String(Date.now()));
  }
  const query = searchParams.toString();
  return query ? `${path}?${query}` : path;
}

function buildFeedUrl(channelKey, { manual = false } = {}) {
  return appendJsonVersionParams(`${buildChannelRoot(channelKey)}/posts.json`, { manual });
}

function buildPageUrl(channelKey, pageNumber, { manual = false, buildId = '' } = {}) {
  return appendJsonVersionParams(`${buildChannelRoot(channelKey)}/pages/${pageNumber}.json`, { manual, buildId });
}

function buildCommentsUrl(channelKey, postId, { manual = false, buildId = '' } = {}) {
  return appendJsonVersionParams(`${buildChannelRoot(channelKey)}/comments/${postId}.json`, { manual, buildId });
}

function buildCatalogUrl({ manual = false } = {}) {
  return `${CHANNELS_INDEX_URL}${manual ? `?t=${Date.now()}` : ''}`;
}

function getJsonFetchOptions({ manual = false, prefetch = false } = {}) {
  if (manual) {
    return {
      cache: 'no-store',
      headers: {
        'cache-control': 'no-cache',
        pragma: 'no-cache',
      },
    };
  }

  if (prefetch) {
    return { cache: 'force-cache' };
  }

  return { cache: 'default' };
}

function cloneJsonValue(value) {
  if (typeof structuredClone === 'function') {
    return structuredClone(value);
  }
  return JSON.parse(JSON.stringify(value));
}

function normalizeTelegramPostUrl(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';

  try {
    const parsed = new URL(raw, window.location.origin);
    const host = parsed.hostname.replace(/^www\./i, '').toLowerCase();
    if (host !== 't.me') {
      parsed.hash = '';
      return parsed.toString().replace(/\/+$/, '');
    }

    const segments = parsed.pathname.split('/').filter(Boolean);
    const normalizedSegments = segments[0] === 's' ? segments.slice(1) : segments;
    if (normalizedSegments.length >= 2 && /^\d+$/.test(normalizedSegments[1])) {
      return `https://t.me/${normalizedSegments[0]}/${normalizedSegments[1]}`;
    }

    parsed.search = '';
    parsed.hash = '';
    return parsed.toString().replace(/\/+$/, '');
  } catch {
    return raw.replace(/\/+$/, '');
  }
}

function normalizePostTextForFingerprint(value) {
  return String(value || '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeForwardedSourceUrl(post) {
  const forwarded = post?.forwarded_from || {};
  return normalizeTelegramPostUrl(forwarded.source_url || forwarded.channel_url || '');
}

function normalizeMediaFingerprintUrl(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';

  try {
    const parsed = new URL(raw, window.location.href);
    if (/^https?:$/i.test(parsed.protocol)) {
      parsed.search = '';
      parsed.hash = '';
      return parsed.toString().replace(/\/+$/, '');
    }
  } catch {
    return raw.replace(/\/+$/, '');
  }

  return raw.replace(/\/+$/, '');
}

function getPostMediaFingerprint(post) {
  const photos = Array.isArray(post?.photos) ? post.photos.map(normalizePhoto).filter(Boolean) : [];
  const photoFingerprint = photos.map((photo) => (
    normalizeMediaFingerprintUrl(photo.source_url || photo.full_url || photo.feed_url || photo.thumb_url)
    || `${Number.parseInt(photo.source_width, 10) || Number.parseInt(photo.full_width, 10) || 0}x${Number.parseInt(photo.source_height, 10) || Number.parseInt(photo.full_height, 10) || 0}`
  )).join(',');
  const videoUrl = normalizeMediaFingerprintUrl(post?.video_url || '');
  const attachedVideos = getAttachedVideos(post);
  const linkPreviewUrl = normalizeTelegramPostUrl(post?.link_preview?.href || '');
  return [
    photos.length,
    photoFingerprint,
    videoUrl,
    attachedVideos.length,
    attachedVideos.map((video) => (
      normalizeMediaFingerprintUrl(video.source_url || video.url || '')
      || `${Number.parseInt(video.width, 10) || 0}x${Number.parseInt(video.height, 10) || 0}:${video.poster ? 1 : 0}`
    )).join(','),
    Boolean(post?.video_note) ? 'note' : '',
    linkPreviewUrl,
  ].join('|');
}

function getPostMediaShapeFingerprint(post) {
  const photos = Array.isArray(post?.photos) ? post.photos.map(normalizePhoto).filter(Boolean) : [];
  const attachedVideos = getAttachedVideos(post);
  return [
    photos.length,
    photos.map((photo) => (
      `${Number.parseInt(photo.source_width, 10) || Number.parseInt(photo.full_width, 10) || 0}x${Number.parseInt(photo.source_height, 10) || Number.parseInt(photo.full_height, 10) || 0}`
    )).join(','),
    `${Number.parseInt(post?.video_width, 10) || 0}x${Number.parseInt(post?.video_height, 10) || 0}`,
    attachedVideos.length,
    attachedVideos.map((video) => (
      `${Number.parseInt(video.width, 10) || 0}x${Number.parseInt(video.height, 10) || 0}:${video.poster ? 1 : 0}`
    )).join(','),
    Boolean(post?.video_note) ? 'note' : '',
    post?.link_preview ? '1' : '0',
  ].join('|');
}

function getPostCanonicalKey(post) {
  const telegramUrl = normalizeTelegramPostUrl(post?.tg_url);
  if (telegramUrl) {
    return `tg:${telegramUrl}`;
  }

  const postId = String(post?.id ?? '').trim();
  if (postId) {
    return `id:${postId}`;
  }

  return '';
}

function getPostDuplicateFingerprint(post) {
  const forwardedSourceUrl = normalizeForwardedSourceUrl(post);
  const textFingerprint = normalizePostTextForFingerprint(post?.text_html || post?.text || '');
  const mediaFingerprint = getPostMediaFingerprint(post);
  const dateValue = String(post?.date || '').trim();
  const hasPhysicalMedia = (
    (Array.isArray(post?.photos) && post.photos.some(Boolean))
    || Boolean(post?.video_url)
    || getAttachedVideos(post).length > 0
    || Boolean(post?.video_note)
  );

  if (forwardedSourceUrl) {
    return `fwd:${forwardedSourceUrl}|${dateValue}|${textFingerprint}|${mediaFingerprint}`;
  }
  if (hasPhysicalMedia) {
    return `media:${dateValue}|${textFingerprint}|${mediaFingerprint}`;
  }

  return '';
}

function getPostLooseDuplicateFingerprint(post) {
  const forwardedSourceUrl = normalizeForwardedSourceUrl(post);
  if (forwardedSourceUrl) return '';

  const textFingerprint = normalizePostTextForFingerprint(post?.text_html || post?.text || '');
  const dateValue = String(post?.date || '').trim();
  const hasPhysicalMedia = (
    (Array.isArray(post?.photos) && post.photos.some(Boolean))
    || Boolean(post?.video_url)
    || getAttachedVideos(post).length > 0
    || Boolean(post?.video_note)
  );

  if (!dateValue || !textFingerprint || !hasPhysicalMedia) return '';
  return `media-shape:${dateValue}|${textFingerprint}|${getPostMediaShapeFingerprint(post)}`;
}

function dedupePostsById(posts = []) {
  const uniquePosts = [];
  const seenIds = new Set();
  const seenCanonicalKeys = new Set();
  const seenDuplicateFingerprints = new Set();
  const seenLooseDuplicateFingerprints = new Set();

  posts.forEach((post) => {
    const normalizedId = String(post?.id ?? '');
    const canonicalKey = getPostCanonicalKey(post);
    const duplicateFingerprint = getPostDuplicateFingerprint(post);
    const looseDuplicateFingerprint = getPostLooseDuplicateFingerprint(post);
    if (normalizedId && seenIds.has(normalizedId)) return;
    if (canonicalKey && seenCanonicalKeys.has(canonicalKey)) return;
    if (duplicateFingerprint && seenDuplicateFingerprints.has(duplicateFingerprint)) return;
    if (looseDuplicateFingerprint && seenLooseDuplicateFingerprints.has(looseDuplicateFingerprint)) return;
    if (normalizedId) {
      seenIds.add(normalizedId);
    }
    if (canonicalKey) {
      seenCanonicalKeys.add(canonicalKey);
    }
    if (duplicateFingerprint) {
      seenDuplicateFingerprints.add(duplicateFingerprint);
    }
    if (looseDuplicateFingerprint) {
      seenLooseDuplicateFingerprints.add(looseDuplicateFingerprint);
    }
    uniquePosts.push(post);
  });

  return uniquePosts;
}

function deriveFeedBuildId(feedPayload) {
  if (!feedPayload) return '';
  if (feedPayload.build_id) {
    return String(feedPayload.build_id);
  }

  const pagination = feedPayload.pagination || {};
  const source = feedPayload.source || {};
  const postIds = (feedPayload.posts || [])
    .map((post) => String(post?.id ?? ''))
    .filter(Boolean)
    .join(',');

  return [
    source.channel_key || state.activeChannelKey || '',
    pagination.total_posts || 0,
    pagination.total_pages || 0,
    pagination.page_size || DEFAULT_PAGE_SIZE,
    postIds,
  ].join('|');
}

function rememberFeedPayload(channelKey, payload) {
  if (!channelKey || !payload) return;
  const normalizedPayload = cloneJsonValue(payload);
  if (Array.isArray(normalizedPayload.posts)) {
    normalizedPayload.posts = dedupePostsById(normalizedPayload.posts);
  }
  if (state.channelFeedCache.has(channelKey)) {
    state.channelFeedCache.delete(channelKey);
  }
  state.channelFeedCache.set(channelKey, normalizedPayload);
  while (state.channelFeedCache.size > FEED_CACHE_MAX_ENTRIES) {
    const firstKey = state.channelFeedCache.keys().next().value;
    if (!firstKey) break;
    state.channelFeedCache.delete(firstKey);
  }
}

function readCachedFeedPayload(channelKey) {
  const payload = state.channelFeedCache.get(channelKey);
  if (!payload) return null;
  const clonedPayload = cloneJsonValue(payload);
  if (Array.isArray(clonedPayload.posts)) {
    clonedPayload.posts = dedupePostsById(clonedPayload.posts);
  }
  return clonedPayload;
}

function invalidateFeedPayloadCache(channelKey = null) {
  if (channelKey) {
    state.channelFeedCache.delete(channelKey);
    state.channelFeedPrefetchPromises.delete(channelKey);
    return;
  }

  state.channelFeedCache.clear();
  state.channelFeedPrefetchPromises.clear();
}

function getNeighborChannelKeys(channelKey) {
  const channels = getCatalogChannels();
  const index = channels.findIndex((channel) => channel.key === channelKey);
  if (index === -1 || channels.length < 2) return [];

  const neighbors = [];
  const previous = channels[index - 1]?.key;
  const next = channels[index + 1]?.key;
  if (previous) neighbors.push(previous);
  if (next) neighbors.push(next);
  return neighbors;
}

function cancelChannelFeedPrefetch() {
  if (state.channelFeedPrefetchHandle == null) return;

  if (typeof window.cancelIdleCallback === 'function') {
    window.cancelIdleCallback(state.channelFeedPrefetchHandle);
  } else {
    window.clearTimeout(state.channelFeedPrefetchHandle);
  }

  state.channelFeedPrefetchHandle = null;
}

async function prefetchChannelFeed(channelKey) {
  if (!channelKey || channelKey === state.activeChannelKey) return null;

  const cached = readCachedFeedPayload(channelKey);
  if (cached) {
    return cached;
  }

  if (state.channelFeedPrefetchPromises.has(channelKey)) {
    return state.channelFeedPrefetchPromises.get(channelKey);
  }

  const promise = (async () => {
    try {
      const response = await fetch(buildFeedUrl(channelKey), getJsonFetchOptions({ prefetch: true }));
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      rememberFeedPayload(channelKey, payload);
      return cloneJsonValue(payload);
    } finally {
      state.channelFeedPrefetchPromises.delete(channelKey);
    }
  })();

  state.channelFeedPrefetchPromises.set(channelKey, promise);
  return promise;
}

function scheduleNeighborChannelPrefetch() {
  cancelChannelFeedPrefetch();
  if (!state.catalog || !state.activeChannelKey) return;

  const callback = () => {
    state.channelFeedPrefetchHandle = null;
    getNeighborChannelKeys(state.activeChannelKey).forEach((channelKey) => {
      void prefetchChannelFeed(channelKey).catch(() => {});
    });
  };

  if (typeof window.requestIdleCallback === 'function') {
    state.channelFeedPrefetchHandle = window.requestIdleCallback(callback, { timeout: 1400 });
    return;
  }

  state.channelFeedPrefetchHandle = window.setTimeout(callback, 260);
}

function getActiveChannelMeta() {
  return getChannelByKey(state.activeChannelKey);
}

function resolveHeroAvatar(site) {
  const catalogSite = getCatalogSite();
  const activeChannel = getActiveChannelMeta();
  const fallbackAvatar = catalogSite.avatar_path || 'assets/channel-avatar.jpg';
  const siteAvatar = site?.avatar_path || '';
  const channelAvatar = activeChannel?.avatar_path || '';
  const usesStaticFallback = !siteAvatar || siteAvatar === fallbackAvatar || /assets\/channel-avatar\.jpg$/i.test(siteAvatar);

  if (channelAvatar && usesStaticFallback) {
    return channelAvatar;
  }

  return siteAvatar || channelAvatar || fallbackAvatar;
}

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('theme', theme);
  if (elements.themeToggle) {
    elements.themeToggle.checked = theme === 'dark';
  }
}

function initTheme() {
  const saved = localStorage.getItem('theme');
  const preferredDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  applyTheme(saved || (preferredDark ? 'dark' : 'light'));
}

function setStatus(target, message) {
  if (!message) {
    target.classList.add('hidden');
    target.textContent = '';
    return;
  }

  target.classList.remove('hidden');
  target.textContent = message;
}

function showCopyToast(message = 'Скопировано') {
  if (!elements.copyToast) return;

  if (state.copyToastTimeoutId) {
    window.clearTimeout(state.copyToastTimeoutId);
  }

  elements.copyToast.textContent = message;
  elements.copyToast.classList.remove('hidden');
  elements.copyToast.classList.add('is-visible');

  state.copyToastTimeoutId = window.setTimeout(() => {
    elements.copyToast.classList.remove('is-visible');
    state.copyToastTimeoutId = window.setTimeout(() => {
      elements.copyToast.classList.add('hidden');
    }, 180);
  }, 1800);
}

async function copyTextToClipboard(text) {
  const value = String(text || '').trim();
  if (!value) return false;

  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch (_) {
    // Fallback below.
  }

  const helper = document.createElement('textarea');
  helper.value = value;
  helper.setAttribute('readonly', '');
  helper.style.position = 'fixed';
  helper.style.opacity = '0';
  helper.style.pointerEvents = 'none';
  document.body.appendChild(helper);
  helper.select();

  let copied = false;
  try {
    copied = document.execCommand('copy');
  } catch (_) {
    copied = false;
  }

  helper.remove();
  return copied;
}

function attachCopyInteractions() {
  document.querySelectorAll('[data-copy-text]').forEach((target) => {
    let longPressTimerId = null;

    const clearLongPress = () => {
      if (longPressTimerId) {
        window.clearTimeout(longPressTimerId);
        longPressTimerId = null;
      }
    };

    const copyTargetValue = async () => {
      const copied = await copyTextToClipboard(target.dataset.copyText || target.textContent || '');
      showCopyToast(copied ? 'Скопировано' : 'Не удалось скопировать');
    };

    target.addEventListener('contextmenu', (event) => {
      event.preventDefault();
      void copyTargetValue();
    });

    target.addEventListener('touchstart', () => {
      target.dataset.copySuppressClick = '0';
      clearLongPress();
      longPressTimerId = window.setTimeout(() => {
        target.dataset.copySuppressClick = '1';
        void copyTargetValue();
      }, LONG_PRESS_COPY_DELAY_MS);
    }, { passive: true });

    target.addEventListener('touchend', clearLongPress, { passive: true });
    target.addEventListener('touchcancel', clearLongPress, { passive: true });
    target.addEventListener('touchmove', clearLongPress, { passive: true });

    target.addEventListener('click', (event) => {
      if (target.dataset.copySuppressClick === '1') {
        event.preventDefault();
        target.dataset.copySuppressClick = '0';
      }
    });
  });
}

function clearSyncStatusPolling() {
  if (state.syncStatusPollId) {
    window.clearInterval(state.syncStatusPollId);
    state.syncStatusPollId = null;
  }
}

function isStandaloneMode() {
  return window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;
}

function isIosDevice() {
  const userAgent = window.navigator.userAgent || '';
  return /iphone|ipad|ipod/i.test(userAgent) || (window.navigator.platform === 'MacIntel' && window.navigator.maxTouchPoints > 1);
}

function isChromiumLikeBrowser() {
  const userAgent = window.navigator.userAgent || '';
  return /chrome|chromium|edg/i.test(userAgent) && !/opr|opera|yaBrowser/i.test(userAgent);
}

function updateInstallButtonState() {
  if (!elements.installAppButton) return;

  const installed = isStandaloneMode();
  elements.installAppButton.classList.toggle('is-installed', installed);
  elements.installAppButton.setAttribute('aria-label', installed ? 'Приложение уже установлено' : 'Установить приложение');
  elements.installAppButton.setAttribute('title', installed ? 'Приложение уже установлено' : 'Установить приложение');
}

function prefersReducedMotion() {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

function isMobileCarouselViewport() {
  return window.matchMedia('(max-width: 860px)').matches;
}

function getChannelCarouselTransitionDuration() {
  const url = new URL(window.location.href);
  return url.searchParams.get('autotest') === 'carousel' ? 980 : CHANNEL_CAROUSEL_TRANSITION_MS;
}

function finishChannelCarouselTransition() {
  if (state.channelCarouselTransition?.timeoutId) {
    window.clearTimeout(state.channelCarouselTransition.timeoutId);
  }
  state.channelCarouselTransition = null;
  state.channelCarouselAnimating = false;
}

function getChannelCarouselStage() {
  return elements.channelCarousel?.querySelector('.channel-carousel__stage') || null;
}

function getChannelCarouselTrack() {
  return elements.channelCarousel?.querySelector('[data-channel-carousel-track]') || null;
}

function getChannelCarouselListPanel() {
  return elements.channelCarousel?.querySelector('[data-channel-carousel-panel]') || null;
}

function suppressChannelCarouselClick(duration = 420) {
  state.channelCarouselClickSuppressUntil = Date.now() + duration;
}

function isChannelCarouselClickSuppressed() {
  return state.channelCarouselClickSuppressUntil > Date.now();
}

function syncChannelCarouselListState() {
  if (!elements.channelCarousel) return;

  const isOpen = state.channelCarouselListOpen && isMobileCarouselViewport();
  elements.channelCarousel.classList.toggle('is-list-open', isOpen);

  const panel = getChannelCarouselListPanel();
  if (panel) {
    panel.setAttribute('aria-hidden', isOpen ? 'false' : 'true');
  }

  elements.channelCarousel.querySelectorAll('[data-channel-carousel-toggle]').forEach((toggle) => {
    toggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
  });
}

function setChannelCarouselListOpen(open) {
  const nextOpen = Boolean(open);
  if (state.channelCarouselListOpen === nextOpen) {
    syncChannelCarouselListState();
    queuePostAnchorOffsetSync();
    return;
  }

  state.channelCarouselListOpen = nextOpen;
  syncChannelCarouselListState();
  queuePostAnchorOffsetSync();
}

function getChannelCarouselWidth(stage = getChannelCarouselStage()) {
  return Math.max(
    stage?.getBoundingClientRect?.().width || 0,
    stage?.clientWidth || 0,
    window.matchMedia('(max-width: 480px)').matches ? 280 : 340,
  );
}

function setChannelCarouselShift(track, shift, stageOrWidth = getChannelCarouselStage()) {
  if (!track) return;
  const width = typeof stageOrWidth === 'number'
    ? stageOrWidth
    : getChannelCarouselWidth(stageOrWidth);
  const baseOffset = -width;
  track.style.transform = `translate3d(${Math.round(baseOffset + shift)}px, 0, 0)`;
}

function getRelativeChannel(offset) {
  const key = getRelativeChannelKey(offset);
  return key ? getChannelByKey(key) : null;
}

function buildMobileChannelCarouselSlide(channel, index, total, options = {}) {
  return `
    <div class="channel-carousel__slide" data-channel-carousel-slide="true">
      ${buildMobileChannelCarouselSurface(channel, index, total, options)}
    </div>
  `;
}

function scheduleChannelCarouselAutotest() {
  const url = new URL(window.location.href);
  if (url.searchParams.get('autotest') !== 'carousel' || state.channelCarouselAutotest?.running) {
    return;
  }

  const channels = getCatalogChannels();
  if (channels.length < 2 || !isMobileCarouselViewport()) {
    document.body.dataset.carouselTestResult = 'skipped';
    return;
  }

  state.channelCarouselAutotest = {
    running: true,
    observedMotion: false,
  };

  window.setTimeout(() => {
    void moveChannelCarousel(1);
  }, 250);

  window.setTimeout(() => {
    const track = getChannelCarouselTrack();
    const currentSurface = elements.channelCarousel?.querySelector('.channel-carousel__surface--current');
    const passed =
      Boolean(track) &&
      Boolean(currentSurface) &&
      Boolean(state.channelCarouselAutotest?.observedMotion) &&
      !state.channelCarouselAnimating;

    document.body.dataset.carouselTestResult = passed ? 'pass' : 'fail';
    document.body.dataset.carouselTestDetails = [
      `motion:${Boolean(state.channelCarouselAutotest?.observedMotion)}`,
      `current:${Boolean(currentSurface)}`,
      `animating:${Boolean(state.channelCarouselAnimating)}`,
    ].join('|');
    state.channelCarouselAutotest.running = false;
  }, 2600);
}

function animateChannelCarouselShift(track, targetShift, { duration = getChannelCarouselTransitionDuration() } = {}) {
  if (!track || prefersReducedMotion()) {
    setChannelCarouselShift(track, targetShift);
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    const easing = 'cubic-bezier(0.16, 1, 0.3, 1)';
    const transitionState = { timeoutId: null };
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      track.style.removeProperty('transition');
      if (state.channelCarouselTransition === transitionState) {
        state.channelCarouselTransition = null;
      }
      resolve();
    };

    transitionState.timeoutId = window.setTimeout(finish, duration + 120);
    state.channelCarouselTransition = transitionState;

    track.style.transition = `transform ${duration}ms ${easing}`;
    requestAnimationFrame(() => {
      setChannelCarouselShift(track, targetShift);
      if (state.channelCarouselAutotest) {
        state.channelCarouselAutotest.observedMotion = true;
      }
    });

    track.addEventListener('transitionend', (event) => {
      if (event.propertyName === 'transform') {
        finish();
      }
    }, { once: true });
  });
}

async function moveChannelCarousel(offset) {
  const nextChannelKey = getRelativeChannelKey(offset);
  if (!nextChannelKey || nextChannelKey === state.activeChannelKey || state.channelCarouselAnimating) return;

  setChannelCarouselListOpen(false);
  const stage = getChannelCarouselStage();
  const track = getChannelCarouselTrack();
  const shouldAnimate = isMobileCarouselViewport() && !prefersReducedMotion() && stage && track;
  const fastTransition = isMobileCarouselViewport();
  const prefetchedFeedPromise = fetchFeedPayload(nextChannelKey).catch((error) => {
    if (state.channelCarouselAutotest) {
      state.channelCarouselAutotest.prefetchError = error.message;
    }
    throw error;
  });

  try {
    if (shouldAnimate) {
      const width = getChannelCarouselWidth(stage);
      const targetShift = offset > 0 ? -width : width;
      state.channelCarouselAnimating = true;
      stage.classList.add('channel-carousel__stage--animating');
      await animateChannelCarouselShift(track, targetShift);
    }

    await switchChannel(nextChannelKey, {
      scrollToTop: true,
      prefetchedFeedPromise,
      fastTransition,
    });
  } finally {
    getChannelCarouselStage()?.classList.remove('channel-carousel__stage--animating');
    finishChannelCarouselTransition();
    queueScrollTopButtonVisibilitySync();
    window.setTimeout(queueScrollTopButtonVisibilitySync, 48);
  }
}

function isTelegramWebUrl(value) {
  try {
    const url = new URL(value, window.location.href);
    return /^(?:www\.)?(?:t|telegram)\.me$/i.test(url.hostname);
  } catch (_) {
    return false;
  }
}

function buildTelegramAppHref(value) {
  try {
    const url = new URL(value, window.location.href);
    if (!isTelegramWebUrl(url.href)) return null;

    const parts = url.pathname.split('/').filter(Boolean);
    if (!parts.length) return null;

    if (parts[0] === 's') {
      parts.shift();
    }

    const domain = parts[0];
    const post = parts[1];
    if (!domain) return null;

    return post
      ? `tg://resolve?domain=${encodeURIComponent(domain)}&post=${encodeURIComponent(post)}`
      : `tg://resolve?domain=${encodeURIComponent(domain)}`;
  } catch (_) {
    return null;
  }
}

function parseTelegramPostLink(value) {
  try {
    const url = new URL(value, window.location.href);
    if (!isTelegramWebUrl(url.href)) return null;

    const parts = url.pathname.split('/').filter(Boolean);
    if (!parts.length) return null;

    if (parts[0].toLowerCase() === 's') {
      parts.shift();
    }

    if (parts.length < 2) return null;

    const username = String(parts[0] || '').trim();
    const postId = Number.parseInt(parts[1], 10);
    if (!username || !Number.isFinite(postId) || postId <= 0) return null;

    return {
      username,
      postId,
      webHref: url.toString(),
    };
  } catch (_) {
    return null;
  }
}

function findMirroredChannelByUsername(username) {
  const normalizedUsername = String(username || '').trim().toLowerCase();
  if (!normalizedUsername) return null;

  return getCatalogChannels().find((channel) =>
    String(channel?.channel_username || '').trim().toLowerCase() === normalizedUsername
  ) || null;
}

function buildChannelPostUrl(channelKey, postId, { absolute = false } = {}) {
  const resolvedPostId = Number.parseInt(postId, 10);
  const resolvedChannelKey = String(channelKey || state.activeChannelKey || '').trim();
  if (!resolvedChannelKey || !Number.isFinite(resolvedPostId) || resolvedPostId <= 0) return '';

  const url = new URL(window.location.href);
  url.hash = '';
  url.search = '';
  url.searchParams.set('channel', resolvedChannelKey);
  url.hash = `post-${resolvedPostId}`;

  return absolute ? url.toString() : `${url.pathname}${url.search}${url.hash}`;
}

function resolveMirroredTelegramPostLink(value) {
  const parsed = parseTelegramPostLink(value);
  if (!parsed) return null;

  const channel = findMirroredChannelByUsername(parsed.username);
  if (!channel?.key) return null;

  const href = buildChannelPostUrl(channel.key, parsed.postId);
  if (!href) return null;

  return {
    ...parsed,
    channel,
    href,
  };
}

async function openMirroredTelegramPost(anchor) {
  const mirrorHref = anchor.dataset.telegramMirrorHref || anchor.getAttribute('href') || anchor.href;
  const channelKey = String(anchor.dataset.telegramMirrorChannelKey || '').trim();
  const postId = Number.parseInt(anchor.dataset.telegramMirrorPostId || '', 10);
  const fallbackUrl = anchor.dataset.telegramWebHref || '';

  if (!mirrorHref || !channelKey || !Number.isFinite(postId) || postId <= 0) {
    openTelegramAnchor(anchor);
    return;
  }

  try {
    window.history.pushState({}, '', mirrorHref);

    if (channelKey === state.activeChannelKey) {
      let focused = await focusPost(postId);
      if (!focused) {
        await loadFeed(channelKey, true);
        window.history.replaceState({}, '', mirrorHref);
        focused = await focusPost(postId);
      }
      if (!focused) {
        openTelegramAnchor(anchor);
      }
      return;
    }

    await switchChannel(channelKey, { replace: false, scrollToTop: true });
    if (state.activeChannelKey !== channelKey) {
      openTelegramAnchor(anchor);
      return;
    }
    window.history.replaceState({}, '', mirrorHref);
    let focused = await focusPost(postId);
    if (!focused) {
      await loadFeed(channelKey, true);
      window.history.replaceState({}, '', mirrorHref);
      focused = await focusPost(postId);
    }
    if (!focused) {
      openTelegramAnchor(anchor);
    }
  } catch (_) {
    openTelegramAnchor(anchor);
  }
}

function openTelegramAnchor(anchor) {
  const webHref = anchor.dataset.telegramWebHref || anchor.href;
  const appHref = anchor.dataset.telegramAppHref || buildTelegramAppHref(webHref);

  if (!appHref) {
    window.location.href = webHref;
    return;
  }

  let fallbackTimerId = null;
  const cleanup = () => {
    if (fallbackTimerId) {
      window.clearTimeout(fallbackTimerId);
      fallbackTimerId = null;
    }
    window.removeEventListener('blur', cleanup);
    document.removeEventListener('visibilitychange', handleVisibilityChange);
    window.removeEventListener('pagehide', cleanup);
  };

  const handleVisibilityChange = () => {
    if (document.visibilityState === 'hidden') {
      cleanup();
    }
  };

  fallbackTimerId = window.setTimeout(() => {
    const pageStillVisible = document.visibilityState === 'visible' && document.hasFocus();
    cleanup();
    if (!pageStillVisible) return;
    window.location.href = webHref;
  }, 900);

  window.addEventListener('blur', cleanup, { once: true });
  document.addEventListener('visibilitychange', handleVisibilityChange);
  window.addEventListener('pagehide', cleanup, { once: true });
  window.location.href = appHref;
}

function bindTelegramDeepLinks(root) {
  if (!root) return;

  root.querySelectorAll('a[href]').forEach((anchor) => {
    if (anchor.dataset.telegramBound === 'true') return;
    if (!isTelegramWebUrl(anchor.getAttribute('href') || anchor.href)) return;

    const webHref = anchor.href;
    const mirrorTarget = resolveMirroredTelegramPostLink(webHref);
    if (mirrorTarget) {
      anchor.dataset.telegramBound = 'true';
      anchor.dataset.telegramWebHref = webHref;
      anchor.dataset.telegramMirrorHref = mirrorTarget.href;
      anchor.dataset.telegramMirrorChannelKey = mirrorTarget.channel.key;
      anchor.dataset.telegramMirrorPostId = String(mirrorTarget.postId);
      anchor.href = mirrorTarget.href;
      anchor.removeAttribute('target');
      anchor.removeAttribute('rel');

      anchor.addEventListener('click', (event) => {
        if (event.defaultPrevented) return;
        if (event.button !== 0) return;
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;

        event.preventDefault();
        void openMirroredTelegramPost(anchor);
      });
      return;
    }

    const appHref = buildTelegramAppHref(webHref);
    if (!appHref) return;

    anchor.dataset.telegramBound = 'true';
    anchor.dataset.telegramWebHref = webHref;
    anchor.dataset.telegramAppHref = appHref;

    anchor.addEventListener('click', (event) => {
      if (event.defaultPrevented) return;
      if (event.button !== 0) return;
      if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;

      event.preventDefault();
      openTelegramAnchor(anchor);
    });
  });
}

function setupChannelMenuWheelScroll() {
  if (!elements.channelMenu || elements.channelMenu.dataset.wheelScrollBound === 'true') return;

  elements.channelMenu.dataset.wheelScrollBound = 'true';
  elements.channelMenu.addEventListener('wheel', (event) => {
    const isDesktopViewport = window.matchMedia('(min-width: 861px)').matches;
    const hasHorizontalOverflow = elements.channelMenu.scrollWidth > elements.channelMenu.clientWidth + 2;
    const hasVerticalIntent = Math.abs(event.deltaY) > Math.abs(event.deltaX);

    if (!isDesktopViewport || !hasHorizontalOverflow || !hasVerticalIntent) {
      return;
    }

    event.preventDefault();
    elements.channelMenu.scrollLeft += event.deltaY;
  }, { passive: false });
}

function setupChannelCarouselInteractions() {
  if (!elements.channelCarousel || elements.channelCarousel.dataset.carouselBound === 'true') return;

  elements.channelCarousel.dataset.carouselBound = 'true';

  elements.channelCarousel.addEventListener('click', (event) => {
    if (isChannelCarouselClickSuppressed()) return;

    const button = event.target.closest('[data-channel-shift]');
    if (button) {
      const shift = Number(button.dataset.channelShift || '0');
      if (!shift) return;
      setChannelCarouselListOpen(false);
      void moveChannelCarousel(shift);
      return;
    }

    const pickerItem = event.target.closest('[data-channel-carousel-select]');
    if (pickerItem) {
      const nextChannelKey = pickerItem.dataset.channelKey;
      setChannelCarouselListOpen(false);
      if (!nextChannelKey || nextChannelKey === state.activeChannelKey) return;
      void switchChannel(nextChannelKey, { scrollToTop: true, fastTransition: true });
      return;
    }

    const toggle = event.target.closest('[data-channel-carousel-toggle]');
    if (!toggle || state.channelCarouselAnimating) return;
    if (!toggle.closest('.channel-carousel__surface--current')) return;
    setChannelCarouselListOpen(!state.channelCarouselListOpen);
  });

  elements.channelCarousel.addEventListener('touchstart', (event) => {
    const stage = getChannelCarouselStage();
    const track = getChannelCarouselTrack();
    const surface = event.target.closest('.channel-carousel__surface--current');
    if (!surface || !stage || !track || event.touches.length !== 1 || state.channelCarouselAnimating || state.channelCarouselListOpen) return;

    const touch = event.touches[0];
    state.channelCarouselTouch = {
      x: touch.clientX,
      y: touch.clientY,
      deltaX: 0,
      deltaY: 0,
      dragging: false,
      width: getChannelCarouselWidth(stage),
      track,
    };
  }, { passive: true });

  elements.channelCarousel.addEventListener('touchmove', (event) => {
    const touchState = state.channelCarouselTouch;
    if (!touchState || !event.touches.length) return;

    const touch = event.touches[0];
    const deltaX = touch.clientX - touchState.x;
    const deltaY = touch.clientY - touchState.y;
    touchState.deltaX = deltaX;
    touchState.deltaY = deltaY;

    if (!touchState.dragging) {
      if (Math.abs(deltaX) < 10 || Math.abs(deltaX) <= Math.abs(deltaY)) {
        return;
      }
      touchState.dragging = true;
    }

    event.preventDefault();
    const maxOffset = touchState.width + 24;
    const offsetX = clamp(deltaX, -maxOffset, maxOffset);
    touchState.track.style.removeProperty('transition');
    setChannelCarouselShift(touchState.track, offsetX, touchState.width);
  }, { passive: false });

  elements.channelCarousel.addEventListener('touchend', (event) => {
    const touchState = state.channelCarouselTouch;
    state.channelCarouselTouch = null;
    if (!touchState || !event.changedTouches.length) return;

    const deltaX = touchState.deltaX;
    const deltaY = touchState.deltaY;
    const threshold = touchState.width * 0.18;
    const moved = Math.abs(deltaX) > 8 || Math.abs(deltaY) > 8;

    if (moved) {
      suppressChannelCarouselClick();
    }

    if (!touchState.dragging || Math.abs(deltaX) < threshold || Math.abs(deltaX) <= Math.abs(deltaY)) {
      void animateChannelCarouselShift(touchState.track, 0, { duration: 170 });
      queueScrollTopButtonVisibilitySync();
      return;
    }

    void moveChannelCarousel(deltaX < 0 ? 1 : -1);
  }, { passive: true });

  elements.channelCarousel.addEventListener('touchcancel', () => {
    if (state.channelCarouselTouch?.track) {
      void animateChannelCarouselShift(state.channelCarouselTouch.track, 0, { duration: 160 });
    }
    state.channelCarouselTouch = null;
    suppressChannelCarouselClick();
    queueScrollTopButtonVisibilitySync();
  }, { passive: true });
}

function getInstallFallbackMessage() {
  if (isStandaloneMode()) {
    return 'Приложение уже установлено';
  }

  if (isIosDevice()) {
    return 'Safari: Поделиться -> На экран «Домой»';
  }

  if (isChromiumLikeBrowser()) {
    return 'Если prompt не появился: меню браузера -> Установить приложение';
  }

  return 'Установка доступна в Chrome или Edge';
}

async function handleInstallButtonClick() {
  if (isStandaloneMode()) {
    showCopyToast('Приложение уже установлено');
    return;
  }

  if (!state.deferredInstallPrompt) {
    showCopyToast(getInstallFallbackMessage());
    return;
  }

  const promptEvent = state.deferredInstallPrompt;
  state.deferredInstallPrompt = null;

  await promptEvent.prompt();
  const choice = await promptEvent.userChoice;

  if (choice.outcome === 'accepted') {
    showCopyToast('Установка запущена');
  } else {
    showCopyToast('Установка отменена');
  }

  updateInstallButtonState();
}

function getLastScheduledSyncTime(reference = new Date()) {
  const syncTime = new Date(reference);
  syncTime.setSeconds(0, 0);
  const alignedMinutes = Math.floor(syncTime.getMinutes() / AUTO_REFRESH_INTERVAL_MINUTES) * AUTO_REFRESH_INTERVAL_MINUTES;
  syncTime.setMinutes(alignedMinutes);
  return syncTime;
}

function updateSyncTimestamp() {
  elements.updatedText.textContent = formatDate(getLastScheduledSyncTime());
}

function startSyncStatusPolling() {
  clearSyncStatusPolling();
  updateSyncTimestamp();
  state.syncStatusPollId = window.setInterval(updateSyncTimestamp, SYNC_STATUS_POLL_INTERVAL_MS);
}

function renderChannelMenu() {
  const channels = getCatalogChannels();
  elements.channelMenu.style.setProperty('--channel-count', String(channels.length || 1));
  elements.channelMenu.innerHTML = channels.map((channel) => {
    const isActive = channel.key === state.activeChannelKey;
    const { rawLabel, title, subtitle } = getChannelMenuLabels(channel);
    return `
        <button
          class="channel-tab${isActive ? ' is-active' : ''}"
          type="button"
          data-channel-key="${channel.key}"
          ${isActive ? `style="${escapeHtml(buildChannelAccentStyle(channel))}"` : ''}
          aria-pressed="${isActive ? 'true' : 'false'}"
          aria-label="${escapeHtml(rawLabel)}"
          title="${escapeHtml(rawLabel)}"
        >
        <span class="channel-tab__meta">${isActive ? 'Открыт' : 'Канал'}</span>
        <span class="channel-tab__title">${formatTextWithSoftBreaks(title)}</span>
        <span class="channel-tab__subtitle">${formatTextWithSoftBreaks(subtitle)}</span>
      </button>
    `;
  }).join('');

  renderMobileChannelCarousel();
  queuePostAnchorOffsetSync();
}

function buildMobileChannelCarouselSurface(channel, index, total, { current = false } = {}) {
  const safeChannel = channel || getActiveChannelMeta() || {};
  const { title, subtitle, rawLabel } = getChannelMenuLabels(safeChannel);
  const hasMultiple = total > 1;

  return `
    <article
      class="channel-carousel__surface${current ? ' channel-carousel__surface--current' : ''}"
      style="${escapeHtml(buildChannelAccentStyle(safeChannel))}"
      data-channel-carousel-surface="true"
      data-channel-key="${escapeHtml(safeChannel.key || '')}"
      aria-label="${escapeHtml(rawLabel)}"
      ${current ? 'aria-current="true"' : 'aria-hidden="true"'}
    >
      <button
        class="channel-carousel__nav channel-carousel__nav--prev"
        type="button"
        data-channel-shift="-1"
        aria-label="Предыдущий канал"
        ${hasMultiple ? '' : 'disabled'}
      >
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="m14.5 6.5-5.5 5.5 5.5 5.5" />
        </svg>
      </button>
      <button
        class="channel-carousel__content channel-carousel__toggle"
        type="button"
        data-channel-carousel-toggle="true"
        aria-controls="channelCarouselList"
        aria-expanded="${state.channelCarouselListOpen ? 'true' : 'false'}"
        aria-label="${escapeHtml(`${rawLabel}. ${state.channelCarouselListOpen ? 'Свернуть список каналов' : 'Показать все каналы'}`)}"
      >
        <span class="channel-carousel__meta">${escapeHtml(`Канал ${index + 1} из ${total}`)}</span>
        <span class="channel-carousel__title">${formatTextWithSoftBreaks(title)}</span>
        <span class="channel-carousel__subtitle">${formatTextWithSoftBreaks(subtitle)}</span>
        <span class="channel-carousel__disclosure" aria-hidden="true">
          <svg viewBox="0 0 20 20" aria-hidden="true">
            <path d="m5 7 5 5 5-5" />
          </svg>
        </span>
      </button>
      <button
        class="channel-carousel__nav channel-carousel__nav--next"
        type="button"
        data-channel-shift="1"
        aria-label="Следующий канал"
        ${hasMultiple ? '' : 'disabled'}
      >
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="m9.5 6.5 5.5 5.5-5.5 5.5" />
        </svg>
      </button>
    </article>
  `;
}

function buildMobileChannelCarouselPickerItem(channel, index) {
  const { title, subtitle, rawLabel } = getChannelMenuLabels(channel);
  const isActive = channel?.key === state.activeChannelKey;

  return `
    <button
      class="channel-carousel__picker-item${isActive ? ' is-active' : ''}"
      type="button"
      data-channel-carousel-select="true"
      data-channel-key="${escapeHtml(channel?.key || '')}"
      style="${escapeHtml(buildChannelAccentStyle(channel))}"
      aria-label="${escapeHtml(rawLabel)}"
      aria-pressed="${isActive ? 'true' : 'false'}"
    >
      <span class="channel-carousel__picker-meta">${isActive ? 'Открыт' : `Канал ${index + 1}`}</span>
      <span class="channel-carousel__picker-title">${formatTextWithSoftBreaks(title)}</span>
      <span class="channel-carousel__picker-subtitle">${formatTextWithSoftBreaks(subtitle)}</span>
    </button>
  `;
}

function renderMobileChannelCarousel() {
  if (!elements.channelCarousel) return;

  const channels = getCatalogChannels();
  if (!channels.length) {
    state.channelCarouselListOpen = false;
    elements.channelCarousel.innerHTML = '';
    return;
  }

  const activeIndex = Math.max(0, getChannelIndex(state.activeChannelKey));
  const activeChannel = channels[activeIndex] || channels[0];
  const previousChannel = getRelativeChannel(-1) || activeChannel;
  const nextChannel = getRelativeChannel(1) || activeChannel;

  let carouselStage = elements.channelCarousel.querySelector('.channel-carousel__stage');
  let carouselList = elements.channelCarousel.querySelector('[data-channel-carousel-panel]');
  if (!carouselStage || !carouselList) {
    elements.channelCarousel.innerHTML = `
      <div class="channel-carousel__stage"></div>
      <div id="channelCarouselList" class="channel-carousel__dropdown" data-channel-carousel-panel aria-hidden="true"></div>
    `;
    carouselStage = elements.channelCarousel.querySelector('.channel-carousel__stage');
    carouselList = elements.channelCarousel.querySelector('[data-channel-carousel-panel]');
  }

  carouselStage.classList.remove('channel-carousel__stage--animating');
  carouselStage.innerHTML = `
    <div class="channel-carousel__track" data-channel-carousel-track>
      ${buildMobileChannelCarouselSlide(previousChannel, (activeIndex - 1 + channels.length) % channels.length, channels.length)}
      ${buildMobileChannelCarouselSlide(activeChannel, activeIndex, channels.length, { current: true })}
      ${buildMobileChannelCarouselSlide(nextChannel, (activeIndex + 1) % channels.length, channels.length)}
    </div>
  `;

  const track = carouselStage.querySelector('[data-channel-carousel-track]');
  if (track) {
    track.style.removeProperty('transition');
    setChannelCarouselShift(track, 0);
  }

  if (carouselList) {
    carouselList.innerHTML = `
      <div class="channel-carousel__picker" role="listbox" aria-label="Все каналы">
        ${channels.map((channel, index) => buildMobileChannelCarouselPickerItem(channel, index)).join('')}
      </div>
    `;
  }

  finishChannelCarouselTransition();
  syncChannelCarouselListState();
  scheduleChannelCarouselAutotest();
}

function formatTextWithSoftBreaks(value) {
  return escapeHtml(String(value || '').trim()).replace(/([a-zа-яё])([A-ZА-ЯЁ])/g, '$1<wbr>$2');
}

function getOrderedTitleParts(title) {
  const rawTitle = String(title || '').trim();
  if (!rawTitle) return [];

  const parts = rawTitle.split('|').map((part) => part.trim()).filter(Boolean);
  if (parts.length < 2) {
    return rawTitle ? [rawTitle] : [];
  }

  const firstPart = parts[0] || '';
  const secondPart = parts[1] || '';
  const latinPattern = /[A-Za-z]/;
  const cyrillicPattern = /[А-Яа-яЁё]/;
  return (
    latinPattern.test(firstPart) && cyrillicPattern.test(secondPart)
      ? [secondPart, firstPart]
      : parts
  );
}

function renderHeroTitle(title) {
  const orderedParts = getOrderedTitleParts(title);
  if (!orderedParts.length) return '';

  return orderedParts.map((part, index) => `
      <span class="hero__title-line${index === 0 ? ' hero__title-line--lead' : ''}">${formatTextWithSoftBreaks(part)}</span>
    `).join('');
}

function renderHeader(site, generatedAt) {
  const catalogSite = getCatalogSite();
  const activeChannel = getActiveChannelMeta();
  const title = activeChannel?.channel_title || site.channel_title || site.site_name || catalogSite.site_name || 'Telegram Channels';
  const description = site.site_description || activeChannel?.site_description || catalogSite.site_description || '';
  const handle = site.channel_username ? `@${site.channel_username}` : '@channel';
  const avatarSrc = resolveHeroAvatar(site);
  const fallbackAvatar = catalogSite.avatar_path || 'assets/channel-avatar.jpg';

  elements.siteTitle.innerHTML = renderHeroTitle(title);
  elements.siteDescription.innerHTML = linkifyTelegramAwareText(description);
  elements.channelLink.textContent = handle;
  elements.channelLink.href = site.channel_username ? `https://t.me/${site.channel_username}` : 'https://t.me';
  startSyncStatusPolling();
  document.title = getOrderedTitleParts(title).join(' | ') || title;

  if (avatarSrc) {
    elements.channelAvatar.dataset.fallbackSrc = fallbackAvatar;
    elements.channelAvatar.dataset.fallbackApplied = 'false';
    elements.channelAvatar.src = avatarSrc;
    elements.channelAvatar.alt = title;
    elements.channelAvatarWrap.classList.remove('hidden');
  } else {
    elements.channelAvatarWrap.classList.add('hidden');
  }

  const accentColor = activeChannel?.accent_color || site.accent_color || catalogSite.accent_color;
  if (accentColor) {
    document.documentElement.style.setProperty('--accent', accentColor);
    const themeColorMeta = document.querySelector('meta[name="theme-color"]');
    if (themeColorMeta) themeColorMeta.setAttribute('content', accentColor);
  }

  if (site.background_color) {
    document.documentElement.style.setProperty('--bg', site.background_color);
  }

  const descriptionMeta = document.querySelector('meta[name="description"]');
  const ogTitleMeta = document.querySelector('meta[property="og:title"]');
  const ogDescriptionMeta = document.querySelector('meta[property="og:description"]');
  const ogImageMeta = document.querySelector('meta[property="og:image"]');

  if (descriptionMeta) descriptionMeta.setAttribute('content', description);
  if (ogTitleMeta) ogTitleMeta.setAttribute('content', title);
  if (ogDescriptionMeta) ogDescriptionMeta.setAttribute('content', description);
  if (ogImageMeta && avatarSrc) ogImageMeta.setAttribute('content', avatarSrc);
}

function buildResponsiveImageTag(item, index, isGallery) {
  const fallbackSrc = isGallery
    ? (item.thumb_url || item.full_url)
    : (item.feed_url || item.full_url || item.thumb_url);
  if (!fallbackSrc) return '';

  const candidates = isGallery
    ? [
      [item.thumb_url, item.thumb_width],
      [item.full_url, item.full_width],
    ]
    : [
      [item.thumb_url, item.thumb_width],
      [item.feed_url, item.feed_width],
      [item.full_url, item.full_width],
    ];
  const seen = new Set();
  const srcSet = candidates
    .filter(([url, width]) => url && width)
    .filter(([url]) => {
      if (seen.has(url)) return false;
      seen.add(url);
      return true;
    })
    .map(([url, width]) => `${url} ${width}w`)
    .join(', ');
  const intrinsicWidth = isGallery
    ? (item.thumb_width || item.full_width || item.source_width || null)
    : (item.feed_width || item.full_width || item.thumb_width || item.source_width || null);
  const intrinsicHeight = isGallery
    ? (item.thumb_height || item.full_height || item.source_height || null)
    : (item.feed_height || item.full_height || item.thumb_height || item.source_height || null);
  const renderMaxWidth = isGallery
    ? null
    : (item.full_width || item.feed_width || item.thumb_width || item.source_width || null);
  const sizes = isGallery
    ? '(max-width: 480px) calc(100vw - 44px), (max-width: 860px) calc(50vw - 28px), 520px'
    : '(max-width: 860px) calc(100vw - 44px), 980px';

  return `
    <img
      src="${fallbackSrc}"
      ${srcSet ? `srcset="${srcSet}" sizes="${sizes}"` : ''}
      ${intrinsicWidth && intrinsicHeight ? `width="${intrinsicWidth}" height="${intrinsicHeight}"` : ''}
      ${renderMaxWidth ? `style="--media-max-inline-size:${renderMaxWidth}px"` : ''}
      ${renderMaxWidth ? `data-render-max-width="${renderMaxWidth}"` : ''}
      data-media-index="${index}"
      alt="Media ${index + 1}"
      loading="lazy"
      decoding="async"
    >
  `;
}

function buildRoundVideoPreviewMarkup(item, index) {
  return `
    <span class="media-video-note" aria-hidden="true">
      ${item.poster
        ? buildResponsiveImageTag(item.poster, index, false)
        : '<span class="media-video-note__placeholder">Видео</span>'}
      <span class="media-video-note__play" aria-hidden="true">
        <svg viewBox="0 0 24 24">
          <path d="M9 7.5v9l7-4.5z"></path>
        </svg>
      </span>
    </span>
  `;
}

function buildLinkPreviewMarkup(preview) {
  const normalizedPreview = normalizeLinkPreview(preview);
  if (!normalizedPreview) return '';

  const caption = normalizedPreview.site_name || normalizedPreview.host || '';
  const descriptionMarkup = normalizedPreview.description
    ? `<div class="post-card__link-preview-description">${escapeHtml(normalizedPreview.description)}</div>`
    : '';
  const imageMarkup = normalizedPreview.image
    ? `
      <div class="post-card__link-preview-media">
        ${buildResponsiveImageTag(normalizedPreview.image, 0, false)}
        ${normalizedPreview.is_video ? '<span class="post-card__link-preview-badge">Видео</span>' : ''}
      </div>
    `
    : '';

  return `
    <a class="post-card__link-preview" href="${escapeHtml(normalizedPreview.href)}" target="_blank" rel="noopener noreferrer">
      ${imageMarkup}
      <div class="post-card__link-preview-copy">
        ${caption ? `<div class="post-card__link-preview-caption">${escapeHtml(caption)}</div>` : ''}
        <div class="post-card__link-preview-title">${escapeHtml(normalizedPreview.title || normalizedPreview.href)}</div>
        ${descriptionMarkup}
      </div>
    </a>
  `;
}

function buildMedia(post) {
  const media = [];
  const roundVideoPost = isRoundVideoPost(post);
  const seenVideoUrls = new Set();

  (post.photos || []).forEach((photo) => {
    const entry = normalizePhoto(photo);
    if (entry) {
      media.push({
        type: 'image',
        ...entry,
      });
    }
  });

  getAttachedVideos(post).forEach((video) => {
    const normalizedUrl = normalizeMediaUrl(video.url);
    if (normalizedUrl && seenVideoUrls.has(normalizedUrl)) return;
    if (normalizedUrl) seenVideoUrls.add(normalizedUrl);
    media.push({
      type: 'video',
      ...video,
    });
  });

  if (post.video_url) {
    const normalizedUrl = normalizeMediaUrl(post.video_url);
    if (!normalizedUrl || !seenVideoUrls.has(normalizedUrl)) {
      if (normalizedUrl) {
        seenVideoUrls.add(normalizedUrl);
      }
      media.push({
        type: roundVideoPost ? 'round-video' : 'video',
        url: post.video_url,
        width: post.video_width || null,
        height: post.video_height || null,
        poster: post.video_poster ? normalizePhoto(post.video_poster) : null,
      });
    }
  }

  if (!media.length) return '';

  const galleryClass = media.length > 1 ? 'post-card__media post-card__media--gallery' : 'post-card__media';
  const mediaId = `${state.activeChannelKey}-media-${post.id}`;
  state.mediaRegistry[mediaId] = media;
  const isGallery = media.length > 1;
  const isSingleRoundVideo = media.length === 1 && media[0]?.type === 'round-video';

  const items = media.map((item, index) => {
    const content = item.type === 'image'
      ? buildResponsiveImageTag(item, index, isGallery)
      : (item.type === 'round-video' && isSingleRoundVideo
          ? buildRoundVideoPreviewMarkup(item, index)
          : `<video src="${item.url}" preload="${item.type === 'round-video' ? 'auto' : 'metadata'}" muted playsinline webkit-playsinline controls${item.type === 'round-video' ? ' data-round-video="true"' : ''}${item.poster?.full_url || item.poster?.feed_url || item.poster?.thumb_url ? ` poster="${item.poster.full_url || item.poster.feed_url || item.poster.thumb_url}"` : ''}></video>`);
    return `<button class="media-trigger${item.type === 'round-video' && isSingleRoundVideo ? ' media-trigger--round-video' : ''}" type="button" data-index="${index}" aria-label="${item.type === 'round-video' ? 'Открыть видеосообщение' : 'Открыть медиа'}">${content}</button>`;
  }).join('');

  return `<div class="${galleryClass}${isSingleRoundVideo ? ' post-card__media--round-video' : ''}" data-media-id="${mediaId}">${items}</div>`;
}

function normalizeMediaUrl(url) {
  if (!url) return '';

  try {
    return new URL(String(url), window.location.href).href;
  } catch {
    return String(url);
  }
}

function getOrderedImageCandidates(item, { isGallery = false, preferFull = false } = {}) {
  const ordered = preferFull
    ? [item.full_url, item.feed_url, item.thumb_url, item.source_url]
    : (isGallery
        ? [item.thumb_url, item.full_url, item.source_url]
        : [item.feed_url, item.full_url, item.thumb_url, item.source_url]);

  const seen = new Set();
  return ordered.filter((candidate) => {
    const normalized = normalizeMediaUrl(candidate);
    if (!normalized || seen.has(normalized)) return false;
    seen.add(normalized);
    return true;
  });
}

function markMediaUnavailable(target, { isGallery = false } = {}) {
  if (!target) return;

  if (target.classList.contains('viewer__slide')) {
    target.innerHTML = '<div class="viewer__fallback">Изображение временно недоступно</div>';
    return;
  }

  const trigger = target.closest('.media-trigger') || target;
  const mediaRoot = trigger.closest('.post-card__media');
  const image = trigger.querySelector('img');

  trigger.classList.add('media-trigger--unavailable');
  trigger.disabled = true;

  if (image) {
    image.alt = '';
    image.classList.add('hidden');
    image.setAttribute('aria-hidden', 'true');
  }

  if (isGallery) {
    trigger.classList.add('hidden');
    const visibleTriggers = mediaRoot
      ? [...mediaRoot.querySelectorAll('.media-trigger:not(.hidden)')]
      : [];
    if (mediaRoot && !visibleTriggers.length) {
      mediaRoot.classList.add('hidden');
    }
    return;
  }

  if (!trigger.querySelector('.media-trigger__fallback')) {
    trigger.insertAdjacentHTML('beforeend', '<span class="media-trigger__fallback">Изображение временно недоступно</span>');
  }
}

function bindImageFallback(image, item, { isGallery = false, preferFull = false } = {}) {
  if (!image || !item || image.dataset.mediaFallbackBound === 'true') return;

  const candidates = getOrderedImageCandidates(item, { isGallery, preferFull });
  if (!candidates.length) {
    markMediaUnavailable(image.closest('.viewer__slide') || image.closest('.media-trigger'), { isGallery });
    return;
  }

  image.dataset.mediaFallbackBound = 'true';
  image.dataset.activeCandidateUrl = image.getAttribute('src') || candidates[0];

  const handleResolvedLoad = () => {
    if (!image.naturalWidth || !image.naturalHeight) {
      handleError();
      return;
    }
    applyIntrinsicMediaLimit(image);
    applyMediaFill(image);
    logImageDiagnostics(image);
  };

  const handleError = () => {
    const activeCandidate = normalizeMediaUrl(image.dataset.activeCandidateUrl || image.getAttribute('src'));
    const activeIndex = candidates.findIndex((candidate) => normalizeMediaUrl(candidate) === activeCandidate);
    const nextCandidate = candidates.slice(activeIndex + 1).find((candidate) => normalizeMediaUrl(candidate) !== activeCandidate);

    if (nextCandidate) {
      image.dataset.activeCandidateUrl = nextCandidate;
      image.removeAttribute('srcset');
      image.removeAttribute('sizes');
      image.src = nextCandidate;
      return;
    }

    markMediaUnavailable(image.closest('.viewer__slide') || image.closest('.media-trigger'), { isGallery });
  };

  image.addEventListener('load', handleResolvedLoad);
  image.addEventListener('error', handleError);

  if (image.complete) {
    if (image.naturalWidth > 0 && image.naturalHeight > 0) {
      handleResolvedLoad();
    } else {
      handleError();
    }
  }
}

function getAverageEdgeColor(image) {
  const width = image.naturalWidth || image.width;
  const height = image.naturalHeight || image.height;
  if (!width || !height) return null;

  const canvas = document.createElement('canvas');
  const context = canvas.getContext('2d', { willReadFrequently: true });
  if (!context) return null;

  canvas.width = 12;
  canvas.height = 12;

  try {
    context.imageSmoothingEnabled = true;
    if ('imageSmoothingQuality' in context) {
      context.imageSmoothingQuality = 'high';
    }
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    const { data } = context.getImageData(0, 0, canvas.width, canvas.height);
    let red = 0;
    let green = 0;
    let blue = 0;
    let alpha = 0;
    let samples = 0;

    for (let y = 0; y < canvas.height; y += 1) {
      for (let x = 0; x < canvas.width; x += 1) {
        const isEdge = x === 0 || y === 0 || x === canvas.width - 1 || y === canvas.height - 1;
        if (!isEdge) continue;

        const offset = (y * canvas.width + x) * 4;
        const currentAlpha = data[offset + 3] / 255;
        if (currentAlpha <= 0.02) continue;

        red += data[offset];
        green += data[offset + 1];
        blue += data[offset + 2];
        alpha += currentAlpha;
        samples += 1;
      }
    }

    if (!samples) return null;

    const soften = (value) => Math.round(value / samples * 0.9 + 255 * 0.1);
    const normalizedAlpha = Math.min(0.92, Math.max(0.42, alpha / samples));

    return `rgba(${soften(red)}, ${soften(green)}, ${soften(blue)}, ${normalizedAlpha.toFixed(3)})`;
  } catch {
    return null;
  }
}

function applyIntrinsicMediaLimit(image) {
  if (image.closest('.post-card__media--gallery')) return;

  const declaredMaxWidth = Number.parseInt(image.dataset.renderMaxWidth || '', 10);
  const naturalWidth = image.naturalWidth || 0;
  const resolvedMaxWidth = naturalWidth || declaredMaxWidth;
  if (!resolvedMaxWidth) return;

  image.style.setProperty('--media-max-inline-size', `${resolvedMaxWidth}px`);
}

function logImageDiagnostics(image) {
  if (!IMAGE_DEBUG_ENABLED) return;

  const rect = image.getBoundingClientRect();
  const postCard = image.closest('.post-card');
  const postId = postCard?.dataset?.postId || 'unknown';
  console.info('[image-debug]', {
    postId,
    selectedUrl: image.currentSrc || image.getAttribute('src'),
    naturalWidth: image.naturalWidth || 0,
    naturalHeight: image.naturalHeight || 0,
    renderedWidth: Math.round(rect.width),
    renderedHeight: Math.round(rect.height),
    declaredRenderMaxWidth: Number.parseInt(image.dataset.renderMaxWidth || '', 10) || null,
  });
}

function applyMediaFill(image) {
  const trigger = image.closest('.media-trigger');
  if (!trigger || trigger.dataset.fillReady === 'true') return;

  const fillColor = getAverageEdgeColor(image);
  if (!fillColor) return;

  trigger.style.setProperty('--media-fill', fillColor);
  trigger.dataset.fillReady = 'true';
}

function bindMediaFill(root) {
  const items = state.mediaRegistry[root.dataset.mediaId] || [];
  const isGallery = root.classList.contains('post-card__media--gallery');

  root.querySelectorAll('.media-trigger img').forEach((image) => {
    const trigger = image.closest('.media-trigger');
    const index = Number.parseInt(trigger?.dataset.index || image.dataset.mediaIndex || '', 10);
    const item = Number.isFinite(index) ? items[index] : null;
    const resolvedItem = item?.type === 'round-video' && item.poster ? item.poster : item;

    if (!resolvedItem) {
      if (image.complete) {
        applyIntrinsicMediaLimit(image);
        applyMediaFill(image);
        logImageDiagnostics(image);
        return;
      }

      image.addEventListener('load', () => {
        applyIntrinsicMediaLimit(image);
        applyMediaFill(image);
        logImageDiagnostics(image);
      }, { once: true });
      return;
    }

    bindImageFallback(image, resolvedItem, { isGallery, preferFull: false });
  });
}

function bindLinkPreviewImage(card, preview) {
  const normalizedPreview = normalizeLinkPreview(preview);
  const image = card?.querySelector('.post-card__link-preview img');
  const media = card?.querySelector('.post-card__link-preview-media');
  if (!image || !media || !normalizedPreview?.image) return;

  const candidates = getOrderedImageCandidates(normalizedPreview.image, { isGallery: false, preferFull: false });
  if (!candidates.length) {
    media.classList.add('post-card__link-preview-media--unavailable');
    media.innerHTML = '<span class="post-card__link-preview-media-fallback">Ссылка</span>';
    return;
  }

  image.dataset.linkPreviewFallbackBound = 'true';
  image.dataset.activeCandidateUrl = image.getAttribute('src') || candidates[0];

  const showFallback = () => {
    media.classList.add('post-card__link-preview-media--unavailable');
    if (!media.querySelector('.post-card__link-preview-media-fallback')) {
      media.insertAdjacentHTML('beforeend', '<span class="post-card__link-preview-media-fallback">Ссылка</span>');
    }
    image.classList.add('hidden');
    image.setAttribute('aria-hidden', 'true');
  };

  const handleResolvedLoad = () => {
    applyIntrinsicMediaLimit(image);
    applyMediaFill(image);
    logImageDiagnostics(image);
  };

  const handleError = () => {
    const activeCandidate = normalizeMediaUrl(image.dataset.activeCandidateUrl || image.getAttribute('src'));
    const activeIndex = candidates.findIndex((candidate) => normalizeMediaUrl(candidate) === activeCandidate);
    const nextCandidate = candidates.slice(activeIndex + 1).find((candidate) => normalizeMediaUrl(candidate) !== activeCandidate);

    if (nextCandidate) {
      image.dataset.activeCandidateUrl = nextCandidate;
      image.removeAttribute('srcset');
      image.removeAttribute('sizes');
      image.src = nextCandidate;
      return;
    }

    showFallback();
  };

  image.addEventListener('load', handleResolvedLoad);
  image.addEventListener('error', handleError);

  if (image.complete) {
    if (image.naturalWidth > 0 && image.naturalHeight > 0) {
      handleResolvedLoad();
    } else {
      handleError();
    }
  }
}

function buildGeneratedPoster(url, size = 640) {
  return {
    thumb_url: url,
    feed_url: url,
    full_url: url,
    thumb_width: size,
    thumb_height: size,
    feed_width: size,
    feed_height: size,
    full_width: size,
    full_height: size,
    source_width: size,
    source_height: size,
  };
}

function generatePosterFromVideo(url) {
  return new Promise((resolve) => {
    if (!url) {
      resolve(null);
      return;
    }

    const video = document.createElement('video');
    video.preload = 'auto';
    video.muted = true;
    video.defaultMuted = true;
    video.playsInline = true;
    video.setAttribute('muted', '');
    video.setAttribute('playsinline', '');
    video.setAttribute('webkit-playsinline', '');
    video.crossOrigin = 'anonymous';

    let settled = false;
    let seekRequested = false;
    const cleanup = () => {
      video.pause();
      video.removeAttribute('src');
      video.load();
    };
    const finish = (poster) => {
      if (settled) return;
      settled = true;
      window.clearTimeout(timeoutId);
      cleanup();
      resolve(poster);
    };

    const capture = () => {
      try {
        const width = video.videoWidth || 0;
        const height = video.videoHeight || 0;
        if (!width || !height) {
          finish(null);
          return;
        }

        const side = Math.min(width, height);
        const offsetX = Math.max(0, Math.round((width - side) / 2));
        const offsetY = Math.max(0, Math.round((height - side) / 2));
        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        if (!context) {
          finish(null);
          return;
        }

        canvas.width = 640;
        canvas.height = 640;
        context.imageSmoothingEnabled = true;
        if ('imageSmoothingQuality' in context) {
          context.imageSmoothingQuality = 'high';
        }
        context.drawImage(video, offsetX, offsetY, side, side, 0, 0, canvas.width, canvas.height);
        finish(buildGeneratedPoster(canvas.toDataURL('image/jpeg', 0.84), canvas.width));
      } catch {
        finish(null);
      }
    };

    const nudgeToFirstFrame = () => {
      if (seekRequested || settled) return;
      seekRequested = true;
      try {
        const duration = Number.isFinite(video.duration) ? video.duration : 0;
        const nextTime = duration > 0 ? Math.min(0.08, Math.max(0.01, duration * 0.02)) : 0.04;
        if (video.currentTime !== nextTime) {
          video.currentTime = nextTime;
        }
      } catch {
        // Safari can throw while metadata is still stabilizing.
      }
    };

    const timeoutId = window.setTimeout(() => finish(null), 6000);
    video.addEventListener('loadedmetadata', () => {
      if (video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
        capture();
        return;
      }
      nudgeToFirstFrame();
    }, { once: true });
    video.addEventListener('loadeddata', capture, { once: true });
    video.addEventListener('canplay', capture, { once: true });
    video.addEventListener('seeked', capture, { once: true });
    video.addEventListener('error', () => finish(null), { once: true });
    video.src = url;
    try {
      video.load();
    } catch {
      finish(null);
    }
  });
}

function upgradeRoundVideoTrigger(trigger, item) {
  if (!trigger || !item || trigger.dataset.roundVideoUpgraded === 'true') return;
  trigger.dataset.roundVideoUpgraded = 'true';

  const index = Number.parseInt(trigger.dataset.index || '0', 10) || 0;
  const renderPreview = (posterOverride = item.poster || null) => {
    const previewItem = posterOverride ? { ...item, poster: posterOverride } : item;
    trigger.innerHTML = buildRoundVideoPreviewMarkup(previewItem, index);
    const image = trigger.querySelector('img');
    if (image) {
      bindImageFallback(image, previewItem.poster, { isGallery: false, preferFull: false });
      if (image.complete) {
        applyIntrinsicMediaLimit(image);
        applyMediaFill(image);
        logImageDiagnostics(image);
      }
    }
  };

  if (item.poster) {
    renderPreview(item.poster);
    return;
  }

  renderPreview(null);
  generatePosterFromVideo(item.url).then((generatedPoster) => {
    if (!generatedPoster) return;
    item.poster = generatedPoster;
    renderPreview(generatedPoster);
  });
}

function normalizeRoundVideoOnlyCard(article, post, mediaRoot) {
  if (!article || !mediaRoot || !isRoundVideoPost(post) || Array.isArray(post.photos) && post.photos.filter(Boolean).length > 0) {
    return;
  }

  const photoCount = Array.isArray(post.photos) ? post.photos.filter(Boolean).length : 0;
  const attachedVideoCount = getAttachedVideos(post).length;
  const isRoundVideoOnly = Boolean(
    post.video_url &&
    photoCount === 0 &&
    attachedVideoCount === 0 &&
    !post.reply_to &&
    !post.forwarded_from &&
    !hasVisiblePostBody(post)
  );
  if (isRoundVideoOnly) {
    article.classList.add('post-card--round-video-only');
  }

  const trigger = mediaRoot.querySelector('.media-trigger');
  const triggerCount = mediaRoot.querySelectorAll('.media-trigger').length;
  const mediaItems = state.mediaRegistry[mediaRoot.dataset.mediaId] || [];
  const item = mediaItems[0] || null;
  if (trigger && triggerCount === 1 && item) {
    const normalizedItem = {
      ...item,
      type: 'round-video',
      poster: item.poster || (post.video_poster ? normalizePhoto(post.video_poster) : null),
    };
    state.mediaRegistry[mediaRoot.dataset.mediaId] = [normalizedItem];
    mediaRoot.classList.remove('post-card__media--gallery');
    mediaRoot.classList.add('post-card__media--round-video');
    trigger.classList.add('media-trigger--round-video');
    upgradeRoundVideoTrigger(trigger, normalizedItem);
  }
}

function resolveForwardedSource(post) {
  const forwarded = post.forwarded_from;
  if (!forwarded) return null;

  const username = forwarded.channel_username || '';
  const localChannel = username
    ? getCatalogChannels().find((channel) => String(channel.channel_username || '').toLowerCase() === String(username).toLowerCase())
    : null;
  const label = forwarded.channel_title || localChannel?.channel_title || (username ? `@${username}` : 'источника');

  if (localChannel) {
    return {
      label,
      href: `?channel=${localChannel.key}`,
      external: false,
    };
  }

  return {
    label,
    href: forwarded.channel_url || forwarded.source_url || (username ? `https://t.me/s/${username}` : '#'),
    external: true,
  };
}

function formatReplyLinkLabel(value) {
  const source = String(value || '').replace(/\r/g, '\n').trim();
  if (!source) return '';

  const paragraphs = source
    .split(/\n\s*\n+/)
    .map((part) => part.replace(/\s+/g, ' ').trim())
    .filter(Boolean);
  const candidate = (paragraphs[0] || source).replace(/\s+/g, ' ').trim();
  if (candidate.length <= 120) return candidate;

  const clipped = candidate.slice(0, 120).replace(/\s+\S*$/, '').trim() || candidate.slice(0, 120).trim();
  return `${clipped}…`;
}

function buildPostAnchorUrl(postId) {
  return buildChannelPostUrl(state.activeChannelKey, postId, { absolute: true });
}

function updatePostHash(postId, { replace = false } = {}) {
  const nextUrl = new URL(window.location.href);
  nextUrl.hash = `post-${postId}`;
  window.history[replace ? 'replaceState' : 'pushState']({}, '', nextUrl);
}

function resolveReplyTarget(post) {
  const reply = post.reply_to;
  if (!reply) return null;

  const postId = Number.parseInt(reply.post_id, 10);
  if (!Number.isFinite(postId) || postId <= 0) return null;

  return {
    postId,
    label: String(reply.title || `пост #${postId}`).trim(),
    tgUrl: String(reply.tg_url || '').trim(),
  };
}

function renderPostCard(post) {
  const article = document.createElement('article');
  article.className = 'post-card';
  article.id = `post-${post.id}`;
  article.dataset.postId = String(post.id);
  const canonicalPostKey = getPostCanonicalKey(post);
  const duplicateFingerprint = getPostDuplicateFingerprint(post);
  const looseDuplicateFingerprint = getPostLooseDuplicateFingerprint(post);
  if (canonicalPostKey) {
    article.dataset.postCanonicalKey = canonicalPostKey;
  }
  if (duplicateFingerprint) {
    article.dataset.postDuplicateFingerprint = duplicateFingerprint;
  }
  if (looseDuplicateFingerprint) {
    article.dataset.postLooseDuplicateFingerprint = looseDuplicateFingerprint;
  }

  const text = normalizePostHtmlSpacing(normalizePostHtml(post.text_html)) || escapeHtml(post.text || '').replace(/\n/g, '<br>');
  const photoCount = Array.isArray(post.photos) ? post.photos.filter(Boolean).length : 0;
  const attachedVideoCount = getAttachedVideos(post).length;
  const isRoundVideoOnly = Boolean(isRoundVideoPost(post) && post.video_url && photoCount === 0 && attachedVideoCount === 0);
  if (isRoundVideoOnly) {
    article.classList.add('post-card--round-video-only');
  }
  const forwarded = resolveForwardedSource(post);
  const replyTarget = resolveReplyTarget(post);
  const postAnchorUrl = buildPostAnchorUrl(post.id);
  const commentsLabel = post.comments_count ? `Комментарии (${compactNumber(post.comments_count)})` : 'Комментарии';
  const shouldShowComments =
    Boolean(state.feed?.source?.comments_enabled) &&
    (post.comments_count > 0 || post.comments_url || post.comments_available);
  const showVideoPostTitle = isRoundVideoOnly;
  const copyPostButtonMarkup = buildCopyPostButtonMarkup(postAnchorUrl);
  const linkPreviewMarkup = hasPhysicalPostMedia(post) ? '' : buildLinkPreviewMarkup(post.link_preview);
  const metaMarkup = `
    ${showVideoPostTitle ? '<div class="post-card__title">Видео-пост</div>' : ''}
    ${replyTarget ? `<div class="post-card__reply">Опубликовано в ответ на <a href="#post-${replyTarget.postId}" data-reply-post-id="${replyTarget.postId}"${replyTarget.tgUrl ? ` data-reply-tg-url="${escapeHtml(replyTarget.tgUrl)}"` : ''}>${escapeHtml(formatReplyLinkLabel(replyTarget.label))}</a></div>` : ''}
    ${forwarded ? `<div class="post-card__forwarded">Переслано из канала <a href="${forwarded.href}"${forwarded.external ? ' target="_blank" rel="noopener"' : ''}>${escapeHtml(forwarded.label)}</a></div>` : ''}
    ${text ? `<div class="post-card__text">${text}</div>` : ''}
    ${linkPreviewMarkup}
  `;
  const roundVideoHeadMarkup = isRoundVideoOnly
    ? `
      <div class="post-card__head">
        <div class="post-card__meta${showVideoPostTitle || replyTarget || forwarded ? '' : ' post-card__meta--empty'}">
          ${showVideoPostTitle ? '<div class="post-card__title">Видео-пост</div>' : ''}
          ${replyTarget ? `<div class="post-card__reply">Опубликовано в ответ на <a href="#post-${replyTarget.postId}" data-reply-post-id="${replyTarget.postId}"${replyTarget.tgUrl ? ` data-reply-tg-url="${escapeHtml(replyTarget.tgUrl)}"` : ''}>${escapeHtml(formatReplyLinkLabel(replyTarget.label))}</a></div>` : ''}
          ${forwarded ? `<div class="post-card__forwarded">Переслано из канала <a href="${forwarded.href}"${forwarded.external ? ' target="_blank" rel="noopener"' : ''}>${escapeHtml(forwarded.label)}</a></div>` : ''}
        </div>
        ${copyPostButtonMarkup}
      </div>
    `
    : '';

  article.innerHTML = `
    ${roundVideoHeadMarkup}
    ${buildMedia(post)}
    <div class="post-card__body">
      <div class="post-card__content">
        ${isRoundVideoOnly ? `${text ? `<div class="post-card__text">${text}</div>` : ''}${linkPreviewMarkup}` : metaMarkup}
      </div>
      ${isRoundVideoOnly ? '' : copyPostButtonMarkup}
    </div>
    <div class="post-card__footer">
      <div class="post-card__stats">
        <span class="chip">${formatDate(post.date)}</span>
      </div>
      <div class="post-card__links">
        ${shouldShowComments ? `<button class="button button--ghost comments-trigger" type="button" data-post-id="${post.id}">${commentsLabel}</button>` : ''}
        <a class="post-card__link" href="${post.tg_url}" target="_blank" rel="noopener">Открыть в Telegram</a>
      </div>
    </div>
  `;

  const mediaRoot = article.querySelector('[data-media-id]');
  if (mediaRoot) {
    const items = state.mediaRegistry[mediaRoot.dataset.mediaId] || [];
    bindMediaFill(mediaRoot);
    normalizeRoundVideoOnlyCard(article, post, mediaRoot);
    mediaRoot.querySelectorAll('.media-trigger').forEach((button) => {
      button.addEventListener('click', () => openViewer(items, Number(button.dataset.index)));
    });
  }

  const commentsButton = article.querySelector('.comments-trigger');
  if (commentsButton) {
    commentsButton.addEventListener('click', () => {
      window.location.hash = `comments-${post.id}`;
    });
  }

  const copyPostButton = article.querySelector('[data-copy-post-url]');
  if (copyPostButton) {
    copyPostButton.addEventListener('click', async () => {
      const copied = await copyTextToClipboard(copyPostButton.dataset.copyPostUrl || '');
      showCopyToast(copied ? 'Ссылка на пост скопирована' : 'Не удалось скопировать ссылку');
    });
  }

  article.querySelectorAll('[data-reply-post-id]').forEach((link) => {
    link.addEventListener('click', (event) => {
      const targetPostId = Number.parseInt(link.dataset.replyPostId || '', 10);
      if (!Number.isFinite(targetPostId)) return;

      event.preventDefault();
      updatePostHash(targetPostId);
      void focusPost(targetPostId, link.dataset.replyTgUrl || '');
    });
  });

  bindLinkPreviewImage(article, post.link_preview);
  bindTelegramDeepLinks(article);

  return article;
}

function pruneDuplicateFeedCards() {
  if (!elements.postFeed) return 0;

  const seenPostIds = new Set();
  const seenCanonicalKeys = new Set();
  const seenDuplicateFingerprints = new Set();
  const seenLooseDuplicateFingerprints = new Set();
  let removed = 0;
  elements.postFeed.querySelectorAll('.post-card[data-post-id]').forEach((card) => {
    const postId = String(card.dataset.postId || '').trim();
    const canonicalKey = String(card.dataset.postCanonicalKey || '').trim();
    const duplicateFingerprint = String(card.dataset.postDuplicateFingerprint || '').trim();
    const looseDuplicateFingerprint = String(card.dataset.postLooseDuplicateFingerprint || '').trim();
    if (
      (postId && seenPostIds.has(postId)) ||
      (canonicalKey && seenCanonicalKeys.has(canonicalKey)) ||
      (duplicateFingerprint && seenDuplicateFingerprints.has(duplicateFingerprint)) ||
      (looseDuplicateFingerprint && seenLooseDuplicateFingerprints.has(looseDuplicateFingerprint))
    ) {
      card.remove();
      removed += 1;
      return;
    }
    if (postId) {
      seenPostIds.add(postId);
    }
    if (canonicalKey) {
      seenCanonicalKeys.add(canonicalKey);
    }
    if (duplicateFingerprint) {
      seenDuplicateFingerprints.add(duplicateFingerprint);
    }
    if (looseDuplicateFingerprint) {
      seenLooseDuplicateFingerprints.add(looseDuplicateFingerprint);
    }
  });

  return removed;
}

function updateFeedMeta() {
  return;
}

function cancelNextPagePrefetch() {
  if (state.nextPagePrefetchHandle == null) return;

  if (typeof window.cancelIdleCallback === 'function') {
    window.cancelIdleCallback(state.nextPagePrefetchHandle);
  } else {
    window.clearTimeout(state.nextPagePrefetchHandle);
  }

  state.nextPagePrefetchHandle = null;
}

function scheduleNextPagePrefetch() {
  cancelNextPagePrefetch();

  if (state.rendered < state.posts.length || state.loadedPages.size >= state.totalPages) {
    return;
  }

  const nextPageNumber = state.loadedPages.size + 1;
  if (state.pageLoadPromises.has(nextPageNumber)) {
    return;
  }

  const callback = () => {
    state.nextPagePrefetchHandle = null;
    void loadPage(nextPageNumber).catch(() => {});
  };

  if (typeof window.requestIdleCallback === 'function') {
    state.nextPagePrefetchHandle = window.requestIdleCallback(callback, { timeout: 1200 });
    return;
  }

  state.nextPagePrefetchHandle = window.setTimeout(callback, 220);
}

function resetFeed() {
  cancelNextPagePrefetch();
  state.rendered = 0;
  state.appendNextPagePromise = null;
  elements.postFeed.innerHTML = '';
  void appendNextPage();
}

function updateLoadMoreVisibility() {
  const hasMoreLoadedPosts = state.rendered < state.posts.length;
  const hasMoreRemotePages = state.loadedPages.size < state.totalPages;
  elements.loadMoreWrap.classList.toggle('hidden', !(hasMoreLoadedPosts || hasMoreRemotePages));
}

async function loadPage(pageNumber) {
  if (state.loadedPages.has(pageNumber) || pageNumber < 2 || pageNumber > state.totalPages) {
    return;
  }

  if (state.pageLoadPromises.has(pageNumber)) {
    return state.pageLoadPromises.get(pageNumber);
  }

  const pageLoadPromise = (async () => {
    const requestChannelKey = state.activeChannelKey;
    const requestRenderVersion = state.feedRenderVersion;
    const requestBuildId = state.activeFeedBuildId;
    const defaultManual = state.activeChannelKey === requestChannelKey && state.activeFeedManual;
    const fetchPagePayload = async (manualRequest = defaultManual) => {
      const response = await fetch(
        buildPageUrl(requestChannelKey, pageNumber, {
          manual: manualRequest,
          buildId: requestBuildId,
        }),
        getJsonFetchOptions(manualRequest ? { manual: true } : { prefetch: true }),
      );
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.json();
    };

    let payload = await fetchPagePayload();
    if (requestBuildId && payload.build_id && payload.build_id !== requestBuildId && !defaultManual) {
      payload = await fetchPagePayload(true);
    }
    if (requestBuildId && payload.build_id && payload.build_id !== requestBuildId) {
      throw new Error('STALE_PAGE_BUILD');
    }
    if (
      state.activeChannelKey === requestChannelKey &&
      state.feedRenderVersion === requestRenderVersion &&
      !state.loadedPages.has(pageNumber)
    ) {
      state.posts = dedupePostsById([...state.posts, ...(payload.posts || [])]);
      state.loadedPages.add(pageNumber);
    }
  })();

  state.pageLoadPromises.set(pageNumber, pageLoadPromise);

  try {
    await pageLoadPromise;
  } finally {
    state.pageLoadPromises.delete(pageNumber);
  }
}

async function appendNextPage() {
  if (state.appendNextPagePromise) {
    return state.appendNextPagePromise;
  }

  state.appendNextPagePromise = (async () => {
    const requestRenderVersion = state.feedRenderVersion;
    try {
      if (state.rendered >= state.posts.length && state.loadedPages.size < state.totalPages) {
        elements.loadMoreButton.disabled = true;
        await loadPage(state.loadedPages.size + 1);
      }
    } catch (error) {
      elements.loadMoreButton.disabled = false;
      return;
    }

    if (requestRenderVersion !== state.feedRenderVersion) {
      elements.loadMoreButton.disabled = false;
      return;
    }

    const renderedIds = new Set();
    const renderedCanonicalKeys = new Set();
    const renderedDuplicateFingerprints = new Set();
    const renderedLooseDuplicateFingerprints = new Set();
    Array.from(elements.postFeed.querySelectorAll('.post-card[data-post-id]')).forEach((node) => {
      const postId = String(node.dataset.postId || '').trim();
      const canonicalKey = String(node.dataset.postCanonicalKey || '').trim();
      const duplicateFingerprint = String(node.dataset.postDuplicateFingerprint || '').trim();
      const looseDuplicateFingerprint = String(node.dataset.postLooseDuplicateFingerprint || '').trim();
      if (postId) {
        renderedIds.add(postId);
      }
      if (canonicalKey) {
        renderedCanonicalKeys.add(canonicalKey);
      }
      if (duplicateFingerprint) {
        renderedDuplicateFingerprints.add(duplicateFingerprint);
      }
      if (looseDuplicateFingerprint) {
        renderedLooseDuplicateFingerprints.add(looseDuplicateFingerprint);
      }
    });
    const nextSlice = state.posts.slice(state.rendered, state.rendered + state.pageSize);
    const nextPosts = nextSlice.filter((post) => {
      const postId = String(post?.id ?? '').trim();
      const canonicalKey = getPostCanonicalKey(post);
      const duplicateFingerprint = getPostDuplicateFingerprint(post);
      const looseDuplicateFingerprint = getPostLooseDuplicateFingerprint(post);
      if (postId && renderedIds.has(postId)) {
        return false;
      }
      if (canonicalKey && renderedCanonicalKeys.has(canonicalKey)) {
        return false;
      }
      if (duplicateFingerprint && renderedDuplicateFingerprints.has(duplicateFingerprint)) {
        return false;
      }
      if (looseDuplicateFingerprint && renderedLooseDuplicateFingerprints.has(looseDuplicateFingerprint)) {
        return false;
      }
      return true;
    });
    const fragment = document.createDocumentFragment();
    nextPosts.forEach((post) => fragment.appendChild(renderPostCard(post)));
    elements.postFeed.appendChild(fragment);
    pruneDuplicateFeedCards();
    state.rendered += nextSlice.length;
    elements.loadMoreButton.disabled = false;
    updateLoadMoreVisibility();
    updateFeedMeta();
    scheduleNextPagePrefetch();
    queueScrollTopButtonVisibilitySync();
  })();

  try {
    await state.appendNextPagePromise;
  } finally {
    state.appendNextPagePromise = null;
  }
}

function openViewer(items, index) {
  state.viewerItems = items;
  state.viewerIndex = index;
  elements.viewer.classList.remove('hidden');
  elements.viewer.setAttribute('aria-hidden', 'false');
  document.body.classList.add('viewer-open');
  renderViewer();
}

function closeViewer() {
  elements.viewer.classList.add('hidden');
  elements.viewer.setAttribute('aria-hidden', 'true');
  elements.viewerContent.innerHTML = '';
  document.body.classList.remove('viewer-open');
  if (state.viewerTransitionTimerId) {
    window.clearTimeout(state.viewerTransitionTimerId);
    state.viewerTransitionTimerId = null;
  }
  state.viewerPointer = null;
  state.viewerAnimating = false;
}

function getViewerViewport() {
  return elements.viewerContent.querySelector('.viewer__viewport');
}

function getViewerTrack() {
  return elements.viewerContent.querySelector('.viewer__track');
}

function buildViewerSlide(item, index) {
  const content = item.type === 'video' || item.type === 'round-video'
    ? `<video src="${item.url}" controls preload="auto" playsinline webkit-playsinline${item.type === 'round-video' ? ' data-round-video="true"' : ''}${item.poster?.full_url || item.poster?.feed_url || item.poster?.thumb_url ? ` poster="${item.poster.full_url || item.poster.feed_url || item.poster.thumb_url}"` : ''}></video>`
    : `<img src="${item.full_url || item.feed_url || item.thumb_url}" alt="Media preview ${index + 1}" loading="eager" decoding="async" draggable="false">`;

  return `<div class="viewer__slide" data-viewer-index="${index}">${content}</div>`;
}

function updateViewerNavigation() {
  const hasMultiple = state.viewerItems.length > 1;
  elements.viewerPrev.classList.toggle('hidden', !hasMultiple);
  elements.viewerNext.classList.toggle('hidden', !hasMultiple);
  elements.viewerPrev.disabled = !hasMultiple || state.viewerIndex <= 0;
  elements.viewerNext.disabled = !hasMultiple || state.viewerIndex >= state.viewerItems.length - 1;
}

function syncViewerActiveSlide(viewport) {
  viewport?.querySelectorAll('video').forEach((video) => {
    const slide = video.closest('.viewer__slide');
    const slideIndex = Number(slide?.dataset.viewerIndex || -1);
    if (slideIndex === state.viewerIndex) {
      video.play().catch(() => {});
      return;
    }

    video.pause();
  });
}

function bindViewerVideoFallbacks() {
  elements.viewerContent.querySelectorAll('.viewer__slide video').forEach((video) => {
    if (video.dataset.viewerFallbackBound === 'true') return;
    video.dataset.viewerFallbackBound = 'true';

    let settled = false;
    const settle = () => {
      if (settled) return;
      settled = true;
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
    };
    const fail = () => {
      if (settled) return;
      settle();
      const slide = video.closest('.viewer__slide');
      if (!slide) return;
      markMediaUnavailable(slide);
    };
    const timeoutId = window.setTimeout(() => {
      if (video.readyState < HTMLMediaElement.HAVE_CURRENT_DATA) {
        fail();
      } else {
        settle();
      }
    }, 9000);

    ['loadedmetadata', 'loadeddata', 'canplay', 'playing'].forEach((eventName) => {
      video.addEventListener(eventName, settle, { once: true });
    });
    video.addEventListener('error', fail, { once: true });
    video.addEventListener('stalled', () => {
      if (video.networkState === HTMLMediaElement.NETWORK_NO_SOURCE || video.readyState < HTMLMediaElement.HAVE_METADATA) {
        fail();
      }
    });
  });
}

function getViewerViewportWidth(viewport = getViewerViewport()) {
  return Math.max(viewport?.clientWidth || 0, elements.viewerContent.clientWidth || 0, 1);
}

function setViewerTrackPosition(track, index = state.viewerIndex, dragOffset = 0) {
  if (!track) return;
  const width = getViewerViewportWidth();
  const shift = -(width * index) + dragOffset;
  track.style.transform = `translate3d(${Math.round(shift)}px, 0, 0)`;
}

function finishViewerTransition(track = getViewerTrack()) {
  if (state.viewerTransitionTimerId) {
    window.clearTimeout(state.viewerTransitionTimerId);
    state.viewerTransitionTimerId = null;
  }

  if (track) {
    track.style.removeProperty('transition');
  }

  state.viewerAnimating = false;
  state.viewerPointer = null;
}

function animateViewerToIndex(targetIndex, { immediate = false } = {}) {
  const viewport = getViewerViewport();
  const track = getViewerTrack();
  if (!viewport || !track) return Promise.resolve();

  state.viewerIndex = clamp(targetIndex, 0, Math.max(state.viewerItems.length - 1, 0));
  updateViewerNavigation();

  if (immediate || prefersReducedMotion()) {
    finishViewerTransition(track);
    setViewerTrackPosition(track);
    syncViewerActiveSlide(viewport);
    return Promise.resolve();
  }

  state.viewerAnimating = true;

  return new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      finishViewerTransition(track);
      setViewerTrackPosition(track);
      syncViewerActiveSlide(viewport);
      resolve();
    };

    state.viewerTransitionTimerId = window.setTimeout(finish, VIEWER_TRANSITION_MS + 120);
    track.style.transition = `transform ${VIEWER_TRANSITION_MS}ms cubic-bezier(0.22, 1, 0.36, 1)`;
    requestAnimationFrame(() => {
      setViewerTrackPosition(track);
    });

    track.addEventListener('transitionend', (event) => {
      if (event.propertyName === 'transform') {
        finish();
      }
    }, { once: true });
  });
}

function finalizeViewerPointerInteraction(cancelled = false) {
  const pointerState = state.viewerPointer;
  const viewport = getViewerViewport();
  const track = getViewerTrack();
  if (!pointerState || !viewport || !track) {
    state.viewerPointer = null;
    return;
  }

  if (!pointerState.dragging || cancelled) {
    state.viewerPointer = null;
    void animateViewerToIndex(state.viewerIndex);
    return;
  }

  const width = pointerState.width || getViewerViewportWidth(viewport);
  const threshold = width * 0.18;
  let nextIndex = state.viewerIndex;

  if (Math.abs(pointerState.deltaX) >= threshold && Math.abs(pointerState.deltaX) > Math.abs(pointerState.deltaY)) {
    nextIndex = clamp(
      state.viewerIndex + (pointerState.deltaX < 0 ? 1 : -1),
      0,
      Math.max(state.viewerItems.length - 1, 0),
    );
  }

  state.viewerPointer = null;
  void animateViewerToIndex(nextIndex);
}

function bindViewerImageFallbacks() {
  elements.viewerContent.querySelectorAll('.viewer__slide img').forEach((image) => {
    const slide = image.closest('.viewer__slide');
    const index = Number.parseInt(slide?.dataset.viewerIndex || '', 10);
    const item = Number.isFinite(index) ? state.viewerItems[index] : null;
    if (!item) return;

    bindImageFallback(image, item, { isGallery: false, preferFull: true });
  });
}

function bindViewerViewport(viewport) {
  if (!viewport || viewport.dataset.bound === 'true') return;
  viewport.dataset.bound = 'true';

  viewport.addEventListener('pointerdown', (event) => {
    if (state.viewerItems.length < 2 || state.viewerAnimating) return;
    if (event.pointerType === 'mouse' && event.button !== 0) return;

    state.viewerPointer = {
      id: event.pointerId,
      x: event.clientX,
      y: event.clientY,
      deltaX: 0,
      deltaY: 0,
      dragging: false,
      width: getViewerViewportWidth(viewport),
    };

    const track = getViewerTrack();
    if (track) {
      track.style.removeProperty('transition');
    }

    if (viewport.setPointerCapture) {
      viewport.setPointerCapture(event.pointerId);
    }
  });

  viewport.addEventListener('pointermove', (event) => {
    const pointerState = state.viewerPointer;
    const track = getViewerTrack();
    if (!pointerState || pointerState.id !== event.pointerId || !track) return;

    const deltaX = event.clientX - pointerState.x;
    const deltaY = event.clientY - pointerState.y;
    pointerState.deltaX = deltaX;
    pointerState.deltaY = deltaY;

    if (!pointerState.dragging) {
      if (Math.abs(deltaX) < 10 || Math.abs(deltaX) <= Math.abs(deltaY)) {
        return;
      }
      pointerState.dragging = true;
    }

    event.preventDefault();
    const isAtFirst = state.viewerIndex === 0;
    const isAtLast = state.viewerIndex === state.viewerItems.length - 1;
    const resistance = (isAtFirst && deltaX > 0) || (isAtLast && deltaX < 0) ? 0.32 : 1;
    setViewerTrackPosition(track, state.viewerIndex, deltaX * resistance);
  });

  const handlePointerEnd = (event, cancelled = false) => {
    const pointerState = state.viewerPointer;
    if (!pointerState || pointerState.id !== event.pointerId) return;

    if (viewport.releasePointerCapture && viewport.hasPointerCapture?.(event.pointerId)) {
      viewport.releasePointerCapture(event.pointerId);
    }

    finalizeViewerPointerInteraction(cancelled);
  };

  viewport.addEventListener('pointerup', (event) => handlePointerEnd(event));
  viewport.addEventListener('pointercancel', (event) => handlePointerEnd(event, true));
  viewport.addEventListener('lostpointercapture', (event) => handlePointerEnd(event, true));
}

function renderViewer() {
  const item = state.viewerItems[state.viewerIndex];
  if (!item) return;

  elements.viewerContent.innerHTML = `
    <div class="viewer__viewport" aria-label="Просмотр медиа">
      <div class="viewer__track">
        ${state.viewerItems.map((entry, index) => buildViewerSlide(entry, index)).join('')}
      </div>
    </div>
  `;

  const viewport = getViewerViewport();
  bindViewerImageFallbacks();
  bindViewerVideoFallbacks();
  bindViewerViewport(viewport);
  updateViewerNavigation();
  requestAnimationFrame(() => {
    void animateViewerToIndex(state.viewerIndex, { immediate: true });
  });
}

function showFeedView() {
  elements.commentsView.classList.add('hidden');
  elements.feedView.classList.remove('hidden');
}

function sanitizeCommentText(text) {
  return linkifyTelegramAwareText(text || '');
}

function renderComment(comment) {
  const node = document.createElement('article');
  node.className = 'comment-card';
  node.innerHTML = `
    <div class="comment-card__header">
      <strong class="comment-card__author">${escapeHtml(comment.author || 'Telegram user')}</strong>
      <span class="comment-card__date">${formatDate(comment.date)}</span>
    </div>
    <div class="comment-card__text">${sanitizeCommentText(comment.text)}</div>
  `;
  bindTelegramDeepLinks(node);
  return node;
}

async function showComments(postId) {
  const post = state.posts.find((entry) => String(entry.id) === String(postId));
  elements.commentsTitle.textContent = post ? 'Комментарии к посту' : 'Комментарии';
  elements.commentsList.innerHTML = '';
  elements.commentsEmpty.classList.add('hidden');
  elements.commentsLoading.classList.remove('hidden');
  setStatus(elements.commentsStatus, null);
  elements.feedView.classList.add('hidden');
  elements.commentsView.classList.remove('hidden');

  try {
    const commentsBuildId = state.activeFeedBuildId;
    const response = await fetch(
      buildCommentsUrl(state.activeChannelKey, postId, {
        manual: state.activeFeedManual,
        buildId: commentsBuildId,
      }),
      getJsonFetchOptions(state.activeFeedManual ? { manual: true } : {}),
    );
    if (response.status === 404) {
      elements.commentsLoading.classList.add('hidden');
      elements.commentsEmpty.classList.remove('hidden');
      setStatus(elements.commentsStatus, null);
      return;
    }
    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    const payload = await response.json();
    if (commentsBuildId && payload.build_id && payload.build_id !== commentsBuildId) {
      throw new Error('STALE_COMMENTS_BUILD');
    }
    const comments = payload.comments || [];
    elements.commentsLoading.classList.add('hidden');

    if (!comments.length) {
      elements.commentsEmpty.classList.remove('hidden');
      setStatus(elements.commentsStatus, `Обновлено ${timeAgo(payload.generated_at)}`);
      return;
    }

    const fragment = document.createDocumentFragment();
    comments.forEach((comment) => fragment.appendChild(renderComment(comment)));
    elements.commentsList.appendChild(fragment);
    setStatus(
      elements.commentsStatus,
      `Комментариев: ${comments.length}. Последний sync: ${timeAgo(payload.generated_at)}`
    );
  } catch (error) {
    elements.commentsLoading.classList.add('hidden');
    elements.commentsEmpty.classList.remove('hidden');
    setStatus(elements.commentsStatus, `Ошибка загрузки комментариев: ${error.message}`);
  }
}

function getPostHashMatch(hash = window.location.hash) {
  return String(hash || '').match(/^#post-(\d+)$/);
}

function getPostElement(postId) {
  return document.getElementById(`post-${postId}`);
}

function getPostScrollOffset() {
  const stickyNav = elements.channelNav || elements.channelMenu?.closest('.channel-nav');
  if (!stickyNav) {
    return getPostAnchorGap();
  }

  const rect = stickyNav.getBoundingClientRect();
  const stickyBottom = Number.isFinite(rect.bottom) ? Math.max(0, rect.bottom) : 0;
  return stickyBottom + getPostAnchorGap();
}

function syncPostAnchorOffset() {
  const offset = Math.max(0, Math.ceil(getPostScrollOffset()));
  document.documentElement.style.setProperty('--post-anchor-offset', `${offset}px`);
  return offset;
}

function getPostScrollTop(target) {
  if (!target) return 0;
  return Math.max(0, target.getBoundingClientRect().top + window.scrollY - getPostScrollOffset());
}

function getPostAnchorDelta(target) {
  if (!target) return 0;
  return target.getBoundingClientRect().top - getPostScrollOffset();
}

async function alignPostToViewport(target) {
  if (!target) return;

  syncPostAnchorOffset();
  const prefersReduced = prefersReducedMotion();
  target.scrollIntoView({
    block: 'start',
    inline: 'nearest',
    behavior: prefersReduced ? 'auto' : 'smooth',
  });

  if (prefersReduced) {
    await nextRenderFrame();
  } else {
    await wait(340);
  }

  for (let attempt = 0; attempt < 2; attempt += 1) {
    syncPostAnchorOffset();
    const delta = getPostAnchorDelta(target);
    if (Math.abs(delta) <= 2) {
      break;
    }

    window.scrollBy({
      top: delta,
      behavior: 'auto',
    });
    await nextRenderFrame();
  }
}

function highlightPost(element) {
  if (!element) return;

  if (state.postHighlightTimeoutId) {
    window.clearTimeout(state.postHighlightTimeoutId);
  }

  document.querySelectorAll('.post-card--targeted').forEach((node) => node.classList.remove('post-card--targeted'));
  element.classList.add('post-card--targeted');
  state.postHighlightTimeoutId = window.setTimeout(() => {
    element.classList.remove('post-card--targeted');
  }, 2200);
}

async function ensurePostVisible(postId) {
  const normalizedPostId = String(postId);
  let targetIndex = state.posts.findIndex((post) => String(post.id) === normalizedPostId);

  while (targetIndex === -1 && state.loadedPages.size < state.totalPages) {
    await loadPage(state.loadedPages.size + 1);
    targetIndex = state.posts.findIndex((post) => String(post.id) === normalizedPostId);
  }

  if (targetIndex === -1) {
    return null;
  }

  while (state.rendered <= targetIndex) {
    await appendNextPage();
  }

  return getPostElement(postId);
}

async function focusPost(postId, fallbackUrl = '') {
  showFeedView();
  const target = await ensurePostVisible(postId);
  if (!target) {
    if (fallbackUrl) {
      window.open(fallbackUrl, '_blank', 'noopener');
    }
    return false;
  }

  highlightPost(target);
  await nextRenderFrame();
  await alignPostToViewport(target);
  return true;
}

async function handleRoute() {
  const commentsMatch = window.location.hash.match(/^#comments-(\d+)$/);
  if (commentsMatch) {
    await showComments(commentsMatch[1]);
    return;
  }

  const postMatch = getPostHashMatch();
  if (postMatch) {
    await focusPost(postMatch[1]);
    return;
  }

  showFeedView();
}

function showFeedLoadingState(clearPosts = true) {
  elements.loadingState.classList.remove('hidden');
  elements.emptyState.classList.add('hidden');
  elements.errorState.classList.add('hidden');
  if (clearPosts) {
    elements.postFeed.innerHTML = '';
  }
  showFeedView();
}

async function fetchFeedPayload(channelKey, force = false) {
  if (!force) {
    const cachedPayload = readCachedFeedPayload(channelKey);
    if (cachedPayload) {
      return cachedPayload;
    }
  } else {
    invalidateFeedPayloadCache(channelKey);
  }

  const response = await fetch(
    buildFeedUrl(channelKey, { manual: force }),
    getJsonFetchOptions({ manual: force })
  );
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const payload = await response.json();
  if (!force) {
    rememberFeedPayload(channelKey, payload);
  }
  return cloneJsonValue(payload);
}

async function resolveFeedPayloadForSwitch(channelKey, { force = false, prefetchedFeedPromise = null } = {}) {
  if (prefetchedFeedPromise) {
    return prefetchedFeedPromise;
  }

  if (!force && state.channelFeedPrefetchPromises.has(channelKey)) {
    return state.channelFeedPrefetchPromises.get(channelKey);
  }

  return fetchFeedPayload(channelKey, force);
}

function applyFeedPayload(channelKey, feedPayload, { manual = false } = {}) {
  cancelNextPagePrefetch();
  state.pageLoadPromises.clear();
  state.feedRenderVersion += 1;
  state.appendNextPagePromise = null;
  state.activeChannelKey = channelKey;
  state.activeFeedManual = manual;
  state.mediaRegistry = {};
  state.feed = feedPayload;
  state.activeFeedBuildId = deriveFeedBuildId(feedPayload);
  const pagination = state.feed.pagination || {};
  state.posts = dedupePostsById(state.feed.posts || []);
  state.totalPosts = Number(pagination.total_posts) || state.posts.length;
  state.totalPages = Number(pagination.total_pages) || 1;
  state.pageSize = Number(pagination.page_size) || DEFAULT_PAGE_SIZE;
  state.loadedPages = new Set(state.posts.length ? [1] : []);

  renderChannelMenu();
  renderHeader(state.feed.site || getActiveChannelMeta() || getCatalogSite(), state.feed.generated_at);
  void ensureChannelAccent({
    ...getActiveChannelMeta(),
    ...(state.feed.site || {}),
    key: channelKey,
  });
  rememberFeedPayload(channelKey, feedPayload);
  elements.loadingState.classList.add('hidden');
  queueScrollTopButtonVisibilitySync();

  if (!state.posts.length) {
    elements.emptyState.classList.remove('hidden');
    updateFeedMeta();
    updateLoadMoreVisibility();
    void handleRoute();
    scheduleChannelCarouselAutotest();
    return;
  }

  resetFeed();
  scheduleNeighborChannelPrefetch();
  void handleRoute();
  scheduleChannelCarouselAutotest();
}

async function loadFeed(channelKey, force = false) {
  showFeedLoadingState(true);

  try {
    const feedPayload = await fetchFeedPayload(channelKey, force);
    applyFeedPayload(channelKey, feedPayload, { manual: force });
  } catch (error) {
    elements.loadingState.classList.add('hidden');
    elements.errorState.classList.remove('hidden');
    elements.errorMessage.textContent = `РћС€РёР±РєР°: ${error.message}`;
  }
}

async function switchChannel(channelKey, { replace = false, force = false, scrollToTop = false, prefetchedFeedPromise = null, fastTransition = false } = {}) {
  const resolvedChannelKey = resolveChannelKey(channelKey);
  if (!resolvedChannelKey) return;

  const isChannelChange = Boolean(state.activeChannelKey) && resolvedChannelKey !== state.activeChannelKey;
  const shouldClearHash = /^#(?:comments|post)-/.test(window.location.hash);
  const shouldUpdateUrl = getChannelKeyFromLocation() !== resolvedChannelKey || shouldClearHash;
  const desktopFastTransition = !fastTransition && !isMobileCarouselViewport();
  const switchMode = fastTransition ? 'mobile' : 'desktop';
  const switchTimings = getChannelSwitchTimings({
    fast: fastTransition,
    desktopFast: desktopFastTransition,
  });
  const feedPayloadTask = isChannelChange
    ? Promise.resolve(prefetchedFeedPromise || fetchFeedPayload(resolvedChannelKey, force))
        .then((payload) => ({ payload }))
        .catch((error) => ({ error }))
    : null;

  if (isChannelChange && state.channelCarouselListOpen) {
    setChannelCarouselListOpen(false);
  }

  if (scrollToTop) {
    scrollPageToTop();
  }

  if (!isChannelChange) {
    if (shouldUpdateUrl) {
      updateChannelUrl(resolvedChannelKey, { replace, clearHash: shouldClearHash });
    }
    await loadFeed(resolvedChannelKey, force);
    if (scrollToTop) {
      scrollPageToTop();
    }
    return;
  }

  setChannelContentSwitching(true, { fast: fastTransition || desktopFastTransition, mode: switchMode });
  await nextRenderFrame();
  await wait(switchTimings.fadeOut);

  try {
    const feedPayloadResult = feedPayloadTask
      ? await feedPayloadTask
      : { payload: await resolveFeedPayloadForSwitch(resolvedChannelKey, { force, prefetchedFeedPromise }) };

    if (feedPayloadResult.error) {
      throw feedPayloadResult.error;
    }

    const feedPayload = feedPayloadResult.payload;
    applyFeedPayload(resolvedChannelKey, feedPayload, { manual: force });

    if (shouldUpdateUrl) {
      updateChannelUrl(resolvedChannelKey, { replace, clearHash: shouldClearHash });
    }
  } catch (error) {
    elements.errorMessage.textContent = `РћС€РёР±РєР°: ${error.message}`;
    showCopyToast(`Не удалось открыть канал: ${error.message}`);
  } finally {
    await nextRenderFrame();
    if (switchTimings.fadeInDelay > 0) {
      await wait(switchTimings.fadeInDelay);
    }
    setChannelContentSwitching(false);
  }

  if (scrollToTop) {
    scrollPageToTop();
  }
}

async function loadCatalog() {
  elements.loadingState.classList.remove('hidden');
  elements.emptyState.classList.add('hidden');
  elements.errorState.classList.add('hidden');

  try {
    const response = await fetch(buildCatalogUrl(), getJsonFetchOptions());
    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    state.catalog = await response.json();
    const initialChannelKey = resolveChannelKey(getChannelKeyFromLocation());

    if (!initialChannelKey) {
      throw new Error('Каталог каналов пуст.');
    }

    renderChannelMenu();

    if (getChannelKeyFromLocation() !== initialChannelKey) {
      updateChannelUrl(initialChannelKey, { replace: true });
    }

    await loadFeed(initialChannelKey);
    scheduleChannelCarouselAutotest();
  } catch (error) {
    elements.loadingState.classList.add('hidden');
    elements.errorState.classList.remove('hidden');
    elements.errorMessage.textContent = `Ошибка: ${error.message}`;
  }
}

function handleLocationChange() {
  if (!state.catalog) return;

  const nextChannelKey = resolveChannelKey(getChannelKeyFromLocation());
  if (nextChannelKey && nextChannelKey !== state.activeChannelKey) {
    void switchChannel(nextChannelKey, { replace: true });
    return;
  }

  void handleRoute();
}

initTheme();

if (elements.themeToggle) {
  elements.themeToggle.addEventListener('change', (event) => {
    applyTheme(event.target.checked ? 'dark' : 'light');
  });
}

elements.channelMenu.addEventListener('click', (event) => {
  const button = event.target.closest('[data-channel-key]');
  if (!button) return;

  const nextChannelKey = button.dataset.channelKey;
  if (!nextChannelKey || nextChannelKey === state.activeChannelKey) return;

  void switchChannel(nextChannelKey, { scrollToTop: true });
});

elements.refreshButton.addEventListener('click', () => {
  if (state.activeChannelKey) {
    invalidateFeedPayloadCache(state.activeChannelKey);
    void loadFeed(state.activeChannelKey, true);
  }
});

if (elements.scrollTopButton) {
  elements.scrollTopButton.addEventListener('click', () => {
    smoothScrollPageToTop();
  });
}

elements.loadMoreButton.addEventListener('click', appendNextPage);
elements.backButton.addEventListener('click', () => {
  if (window.location.hash.startsWith('#comments-')) {
    window.location.hash = '';
  } else {
    showFeedView();
  }
});

elements.viewerClose.addEventListener('click', closeViewer);
elements.viewer.addEventListener('click', (event) => {
  if (event.target === elements.viewer) closeViewer();
});
elements.viewerPrev.addEventListener('click', () => {
  if (state.viewerAnimating || state.viewerIndex <= 0) return;
  void animateViewerToIndex(state.viewerIndex - 1);
});
elements.viewerNext.addEventListener('click', () => {
  if (state.viewerAnimating || state.viewerIndex >= state.viewerItems.length - 1) return;
  void animateViewerToIndex(state.viewerIndex + 1);
});

elements.channelAvatar.addEventListener('error', () => {
  const fallbackSrc = elements.channelAvatar.dataset.fallbackSrc || 'assets/channel-avatar.jpg';
  if (elements.channelAvatar.dataset.fallbackApplied === 'true') {
    return;
  }

  elements.channelAvatar.dataset.fallbackApplied = 'true';
  elements.channelAvatar.src = fallbackSrc;
});

if (elements.installAppButton) {
  elements.installAppButton.addEventListener('click', () => {
    void handleInstallButtonClick();
  });
}

window.addEventListener('hashchange', () => {
  void handleRoute();
});
window.addEventListener('popstate', handleLocationChange);
window.addEventListener('beforeinstallprompt', (event) => {
  event.preventDefault();
  state.deferredInstallPrompt = event;
  updateInstallButtonState();
});
window.addEventListener('appinstalled', () => {
  state.deferredInstallPrompt = null;
  updateInstallButtonState();
  showCopyToast('Приложение установлено');
});
window.addEventListener('resize', () => {
  if (!isMobileCarouselViewport() && state.channelCarouselListOpen) {
    state.channelCarouselListOpen = false;
    syncChannelCarouselListState();
  }
  queuePostAnchorOffsetSync();
  if (elements.viewer.classList.contains('hidden')) return;
  void animateViewerToIndex(state.viewerIndex, { immediate: true });
});
window.addEventListener('resize', queueScrollTopButtonVisibilitySync);
window.addEventListener('scroll', queueScrollTopButtonVisibilitySync, { passive: true });
window.addEventListener('keydown', (event) => {
  if (elements.viewer.classList.contains('hidden')) return;
  if (event.key === 'Escape') closeViewer();
  if (event.key === 'ArrowLeft' && state.viewerItems.length > 1 && state.viewerIndex > 0) elements.viewerPrev.click();
  if (event.key === 'ArrowRight' && state.viewerItems.length > 1 && state.viewerIndex < state.viewerItems.length - 1) elements.viewerNext.click();
});

if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('./sw.js')
    .then((registration) => registration.update())
    .catch(() => {});
}

setupChannelMenuWheelScroll();
setupChannelCarouselInteractions();
attachCopyInteractions();
syncPostAnchorOffset();
loadCatalog();
updateInstallButtonState();
queueScrollTopButtonVisibilitySync();
