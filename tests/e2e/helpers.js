const fs = require('fs');
const path = require('path');
const { expect } = require('@playwright/test');

function findMirroredRoundVideoPost({ requirePaged = false } = {}) {
  const docsRoot = path.join(process.cwd(), 'docs');
  const channelsRoot = path.join(docsRoot, 'data', 'channels');
  const channelKeys = fs.readdirSync(channelsRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((left, right) => {
      if (left === 'pg-tax') return -1;
      if (right === 'pg-tax') return 1;
      return left.localeCompare(right);
    });

  for (const channelKey of channelKeys) {
    const channelRoot = path.join(channelsRoot, channelKey);
    const pagesRoot = path.join(channelRoot, 'pages');
    const payloadPaths = [];

    if (!requirePaged) {
      payloadPaths.push(path.join(channelRoot, 'posts.json'));
    }
    if (fs.existsSync(pagesRoot)) {
      const pagePaths = fs.readdirSync(pagesRoot)
        .filter((name) => /^\d+\.json$/.test(name))
        .sort((left, right) => Number.parseInt(left, 10) - Number.parseInt(right, 10))
        .map((name) => path.join(pagesRoot, name));
      payloadPaths.push(...pagePaths);
    }

    for (const payloadPath of payloadPaths) {
      if (!fs.existsSync(payloadPath)) continue;
      const payload = JSON.parse(fs.readFileSync(payloadPath, 'utf8'));
      const post = (payload.posts || []).find((entry) => {
        if (!entry?.video_note || !entry.video_url) return false;
        const relativeVideoPath = String(entry.video_url).split(/[?#]/, 1)[0].replace(/^\/+/, '');
        const localVideoPath = path.join(docsRoot, relativeVideoPath);
        return fs.existsSync(localVideoPath) && fs.statSync(localVideoPath).size > 0;
      });

      if (post) {
        return {
          channelKey,
          postId: String(post.id),
        };
      }
    }
  }

  throw new Error('No mirrored round-video post with local media was found in the current archive.');
}

async function waitForFeedReady(page) {
  await expect(page.locator('#errorState')).toHaveClass(/hidden/, { timeout: 20_000 });
  await expect(page.locator('#loadingState')).toHaveClass(/hidden/, { timeout: 20_000 });
  await expect(page.locator('.post-card').first()).toBeVisible({ timeout: 20_000 });
}

async function clickLoadMoreIfVisible(page) {
  const button = page.locator('#loadMoreWrap:not(.hidden) #loadMoreButton');
  if (!await button.count()) return false;
  if (!await button.isVisible()) return false;
  if (await button.isDisabled()) return false;

  await button.click();
  await page.waitForTimeout(350);
  return true;
}

async function ensureMediaInFeed(page, { attempts = 4 } = {}) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const mediaTrigger = page.locator('.post-card .media-trigger').first();
    if (await mediaTrigger.count()) {
      await expect(mediaTrigger).toBeVisible({ timeout: 10_000 });
      return mediaTrigger;
    }

    const loaded = await clickLoadMoreIfVisible(page);
    if (!loaded) break;
  }

  throw new Error('No media trigger found in loaded feed pages.');
}

async function ensureGalleryInFeed(page, { attempts = 4 } = {}) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const galleryTrigger = page.locator('.post-card__media--gallery .media-trigger').first();
    if (await galleryTrigger.count()) {
      await expect(galleryTrigger).toBeVisible({ timeout: 10_000 });
      return galleryTrigger;
    }

    const loaded = await clickLoadMoreIfVisible(page);
    if (!loaded) break;
  }

  throw new Error('No gallery post found in loaded feed pages.');
}

async function openFirstViewerFromFeed(page, { gallery = false } = {}) {
  const trigger = gallery
    ? await ensureGalleryInFeed(page)
    : await ensureMediaInFeed(page);

  await trigger.click();
  await expect(page.locator('#viewer')).toBeVisible({ timeout: 10_000 });
  await expect(page.locator('#viewerContent .viewer__viewport')).toBeVisible({ timeout: 10_000 });
}

module.exports = {
  findMirroredRoundVideoPost,
  waitForFeedReady,
  clickLoadMoreIfVisible,
  ensureMediaInFeed,
  ensureGalleryInFeed,
  openFirstViewerFromFeed,
};
