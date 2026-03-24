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
});
