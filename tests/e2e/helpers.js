const { expect } = require('@playwright/test');

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
  waitForFeedReady,
  clickLoadMoreIfVisible,
  ensureMediaInFeed,
  ensureGalleryInFeed,
  openFirstViewerFromFeed,
};
