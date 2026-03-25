const fs = require('fs');
const path = require('path');
const { test, expect } = require('@playwright/test');
const {
  waitForFeedReady,
  clickLoadMoreIfVisible,
  openFirstViewerFromFeed,
} = require('./helpers');

test.describe('Desktop smoke', () => {
  test('loads desktop shell and active channel feed', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await expect(page.locator('.channel-nav')).toBeVisible();
    await expect(page.locator('#siteTitle')).toContainText(/Пепеляев Групп|Pepeliaev Group/);
    await expect(page.locator('.contact-bar')).toBeVisible();
    expect(await page.locator('.post-card').count()).toBeGreaterThan(0);
  });

  test('switches channel from desktop menu and updates hero + url', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const initialTitle = (await page.locator('#siteTitle').innerText()).trim();
    const targetButton = page.locator('#channelMenu .channel-tab[data-channel-key="pg-tax"]');
    await expect(targetButton).toBeVisible();

    await targetButton.click();
    await waitForFeedReady(page);

    await expect(page).toHaveURL(/channel=pg-tax/);
    await expect(page.locator('#channelLink')).toContainText('@PG_Tax');
    await expect(page.locator('#siteTitle')).not.toHaveText(initialTitle);
  });

  test('keeps narrow desktop channel menu scrollable and clickable', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const menu = page.locator('#channelMenu');
    const hasOverflow = await menu.evaluate((node) => node.scrollWidth > node.clientWidth + 2);
    if (!hasOverflow) {
      return;
    }

    await menu.hover();
    const scrollLeftBefore = await menu.evaluate((node) => node.scrollLeft);
    await page.mouse.wheel(0, 900);
    await expect.poll(async () => menu.evaluate((node) => node.scrollLeft)).toBeGreaterThan(scrollLeftBefore);

    const initialTitle = (await page.locator('#siteTitle').innerText()).trim();
    const targetButton = page.locator('#channelMenu .channel-tab[data-channel-key="pg-employment"]');
    await targetButton.scrollIntoViewIfNeeded();
    await targetButton.click();
    await waitForFeedReady(page);

    await expect(page).toHaveURL(/channel=pg-employment/);
    await expect(page.locator('#siteTitle')).not.toHaveText(initialTitle);
  });

  test('opens and closes viewer for post media', async ({ page }) => {
    await page.goto('/?channel=investment-law');
    await waitForFeedReady(page);
    await openFirstViewerFromFeed(page);

    await expect(page.locator('#viewer')).toBeVisible();
    await page.locator('#viewerClose').click();
    await expect(page.locator('#viewer')).toBeHidden();
  });

  test('loads more posts and deep-link target resolves', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const beforeCount = await page.locator('.post-card').count();
    await clickLoadMoreIfVisible(page);
    const afterCount = await page.locator('.post-card').count();
    expect(afterCount).toBeGreaterThanOrEqual(beforeCount);

    const firstPost = page.locator('.post-card').first();
    const postId = await firstPost.getAttribute('data-post-id');
    expect(postId).toBeTruthy();

    await page.goto(`/?channel=pgp-official#post-${postId}`);
    await waitForFeedReady(page);
    await expect(page.locator(`#post-${postId}`)).toHaveClass(/post-card--targeted/);
  });

  test('does not duplicate feed cards after overlapping load-more and refresh requests', async ({ page }) => {
    await page.route('**/data/channels/pgp-official/pages/2.json', async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 220));
      await route.continue();
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const loadMoreButton = page.locator('#loadMoreWrap:not(.hidden) #loadMoreButton');
    await expect(loadMoreButton).toBeVisible();
    const loadMoreClick = loadMoreButton.click();
    await page.waitForTimeout(40);

    await page.evaluate(async () => {
      await Promise.all([
        window.loadFeed('pgp-official', true),
        window.loadFeed('pgp-official', true),
        window.loadFeed('pgp-official', true),
      ]);
    });

    await loadMoreClick;
    await waitForFeedReady(page);
    await page.waitForTimeout(250);

    const duplicateInfo = await page.evaluate(() => {
      const ids = Array.from(document.querySelectorAll('.post-card[data-post-id]'))
        .map((node) => String(node.dataset.postId || ''))
        .filter(Boolean);
      const counts = ids.reduce((map, id) => {
        map[id] = (map[id] || 0) + 1;
        return map;
      }, {});

      return {
        total: ids.length,
        unique: new Set(ids).size,
        duplicates: Object.entries(counts).filter(([, count]) => count > 1),
      };
    });

    expect(duplicateInfo.duplicates).toEqual([]);
    expect(duplicateInfo.total).toBe(duplicateInfo.unique);
  });

  test('ignores stale cached page payloads from an older feed build', async ({ page }) => {
    const postsPath = path.join(process.cwd(), 'docs', 'data', 'channels', 'investment-law', 'posts.json');
    const page2Path = path.join(process.cwd(), 'docs', 'data', 'channels', 'investment-law', 'pages', '2.json');
    const postsPayload = JSON.parse(fs.readFileSync(postsPath, 'utf8'));
    const page2Payload = JSON.parse(fs.readFileSync(page2Path, 'utf8'));
    const stalePagePayload = {
      ...page2Payload,
      build_id: 'stale-build',
      posts: postsPayload.posts.slice(0, Math.min(3, postsPayload.posts.length)),
    };
    const freshPagePayload = {
      ...page2Payload,
      build_id: 'fresh-build',
    };
    const freshFeedPayload = {
      ...postsPayload,
      build_id: 'fresh-build',
    };

    await page.route('**/data/channels/investment-law/posts.json', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(freshFeedPayload),
      });
    });

    await page.route('**/data/channels/investment-law/pages/2.json**', async (route) => {
      const url = route.request().url();
      const payload = url.includes('t=') ? freshPagePayload : stalePagePayload;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(payload),
      });
    });

    await page.goto('/?channel=investment-law');
    await waitForFeedReady(page);

    const initialIds = await page.locator('.post-card[data-post-id]').evaluateAll((nodes) =>
      nodes.map((node) => String(node.dataset.postId || '')).filter(Boolean)
    );

    await clickLoadMoreIfVisible(page);
    await page.waitForTimeout(250);

    const duplicateInfo = await page.evaluate(() => {
      const ids = Array.from(document.querySelectorAll('.post-card[data-post-id]'))
        .map((node) => String(node.dataset.postId || ''))
        .filter(Boolean);
      const counts = ids.reduce((map, id) => {
        map[id] = (map[id] || 0) + 1;
        return map;
      }, {});

      return {
        ids,
        duplicates: Object.entries(counts).filter(([, count]) => count > 1),
      };
    });

    expect(duplicateInfo.duplicates).toEqual([]);
    expect(new Set(duplicateInfo.ids).size).toBeGreaterThan(initialIds.length);
  });

  test('reveals scroll-to-top control after long scroll and returns to top', async ({ page }) => {
    await page.goto('/?channel=pg-tax');
    await waitForFeedReady(page);

    await page.evaluate(() => window.scrollTo({ top: 1400, behavior: 'auto' }));
    await expect(page.locator('#scrollTopButton')).toHaveClass(/is-visible/);

    await page.locator('#scrollTopButton').click();
    await page.waitForFunction(() => window.scrollY < 24);
    await expect(page.locator('#scrollTopButton')).not.toHaveClass(/is-visible/);
  });

  test('keeps round-video title and copy action in one top row', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'round-video-layout-host-desktop';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999991,
        date: new Date().toISOString(),
        text: '',
        text_html: '',
        photos: [],
        video_note: true,
        video_url: 'data:video/mp4;base64,AAAA',
        tg_url: 'https://t.me/example/999991',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    const title = page.locator('#round-video-layout-host-desktop .post-card__title');
    const copy = page.locator('#round-video-layout-host-desktop .post-card__copy');
    const media = page.locator('#round-video-layout-host-desktop .post-card__media');
    await expect(title).toHaveText('Видео-пост');
    const [titleBox, copyBox, mediaBox] = await Promise.all([title.boundingBox(), copy.boundingBox(), media.boundingBox()]);
    expect(titleBox).toBeTruthy();
    expect(copyBox).toBeTruthy();
    expect(mediaBox).toBeTruthy();
    expect(Math.abs(titleBox.y - copyBox.y)).toBeLessThanOrEqual(10);
    expect(copyBox.x).toBeGreaterThan(titleBox.x);
    expect(titleBox.y).toBeLessThan(mediaBox.y);
  });

  test('shows round-video poster preview and viewer fallback instead of endless loading', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'round-video-fallback-host-desktop';
      document.body.appendChild(host);
      const posterUrl = `${window.location.origin}/assets/channel-avatar.jpg`;
      const card = window.renderPostCard({
        id: 999993,
        date: new Date().toISOString(),
        text: '',
        text_html: '',
        photos: [],
        video_note: true,
        video_url: '/missing-round-video.mp4',
        video_poster: {
          thumb_url: posterUrl,
          feed_url: posterUrl,
          full_url: posterUrl,
        },
        tg_url: 'https://t.me/example/999993',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    await expect(page.locator('#round-video-fallback-host-desktop .media-video-note img')).toBeVisible();
    await page.locator('#round-video-fallback-host-desktop .media-trigger').click();
    await expect(page.locator('#viewer')).toBeVisible();
    await expect(page.locator('#viewer .viewer__fallback')).toContainText(/временно недоступно/i);
    await page.locator('#viewerClose').click();
    await expect(page.locator('#viewer')).toBeHidden();
  });
  test('opens actual bankrotstvo round-video viewer on desktop without fallback', async ({ page }) => {
    await page.goto('/?channel=bankrotstvo-mustknow');
    await waitForFeedReady(page);

    const card = page.locator('.post-card--round-video-only').first();
    await expect(card).toBeVisible();
    await card.locator('.media-trigger').click();
    await expect(page.locator('#viewer')).toBeVisible();
    await page.waitForFunction(() => {
      const video = document.querySelector('#viewer video');
      return Boolean(video && video.readyState >= 1);
    });
    await expect(page.locator('#viewer .viewer__fallback')).toHaveCount(0);
    await expect(page.locator('#viewer video')).toBeVisible();
  });
});
