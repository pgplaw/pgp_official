const { test, expect } = require('@playwright/test');
const {
  waitForFeedReady,
  openFirstViewerFromFeed,
} = require('./helpers');

test.describe('Mobile smoke', () => {
  test('switches channel from mobile carousel', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const initialTitle = (await page.locator('#siteTitle').innerText()).trim();
    const nextButton = page.locator('#channelCarousel .channel-carousel__surface--current .channel-carousel__nav--next');
    await expect(nextButton).toBeVisible();

    await nextButton.click();
    await waitForFeedReady(page);

    await expect(page.locator('#siteTitle')).not.toHaveText(initialTitle);
    expect(page.url()).not.toContain('channel=pgp-official');
  });

  test('toggles mobile channel list and switches channel from it', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const toggle = page.locator('#channelCarousel .channel-carousel__surface--current [data-channel-carousel-toggle]');
    const panel = page.locator('#channelCarousel [data-channel-carousel-panel]');
    await expect(toggle).toBeVisible();

    await toggle.click();
    await expect(page.locator('#channelCarousel')).toHaveClass(/is-list-open/);
    await expect(panel).toBeVisible();

    const panelBox = await panel.boundingBox();
    const viewport = page.viewportSize();
    expect(panelBox).toBeTruthy();
    expect(viewport).toBeTruthy();
    expect(panelBox.y + panelBox.height).toBeLessThanOrEqual(viewport.height + 2);

    await toggle.click();
    await expect(page.locator('#channelCarousel')).not.toHaveClass(/is-list-open/);

    await toggle.click();
    const initialTitle = (await page.locator('#siteTitle').innerText()).trim();
    const target = page.locator('#channelCarousel [data-channel-carousel-select][data-channel-key="pg-antitrust"]');
    await expect(target).toBeVisible();
    await target.click();
    await waitForFeedReady(page);

    await expect(page.locator('#channelCarousel')).not.toHaveClass(/is-list-open/);
    await expect(page.locator('#siteTitle')).not.toHaveText(initialTitle);
    expect(page.url()).toContain('channel=pg-antitrust');
  });

  test('opens gallery viewer and navigates to next slide', async ({ page }) => {
    await page.goto('/?channel=investment-law');
    await waitForFeedReady(page);
    await openFirstViewerFromFeed(page, { gallery: true });

    await expect(page.locator('#viewerNext')).toBeVisible();
    await expect(page.locator('#viewerPrev')).toBeDisabled();

    await page.locator('#viewerNext').click();
    await expect(page.locator('#viewerPrev')).toBeEnabled();
    await page.locator('#viewerClose').click();
    await expect(page.locator('#viewer')).toBeHidden();
  });

  test('shows scroll-to-top control after swipe channel switch and long scroll', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const initialTitle = (await page.locator('#siteTitle').innerText()).trim();
    const nextButton = page.locator('#channelCarousel .channel-carousel__surface--current .channel-carousel__nav--next');
    await expect(nextButton).toBeVisible();
    await nextButton.click();
    await waitForFeedReady(page);
    await expect(page.locator('#siteTitle')).not.toHaveText(initialTitle);

    await page.evaluate(() => {
      window.scrollTo({ top: 1600, behavior: 'auto' });
      window.dispatchEvent(new Event('scroll'));
    });
    await page.waitForFunction(() => document.getElementById('scrollTopButton')?.classList.contains('is-visible'));
    await expect(page.locator('#scrollTopButton')).toHaveClass(/is-visible/);
  });

  test('keeps round-video title and copy action aligned on mobile', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'round-video-layout-host-mobile';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999992,
        date: new Date().toISOString(),
        text: '',
        text_html: '',
        photos: [],
        video_note: true,
        video_url: 'data:video/mp4;base64,AAAA',
        tg_url: 'https://t.me/example/999992',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    const title = page.locator('#round-video-layout-host-mobile .post-card__title');
    const copy = page.locator('#round-video-layout-host-mobile .post-card__copy');
    const media = page.locator('#round-video-layout-host-mobile .post-card__media');
    await expect(title).toHaveText('Видео-пост');
    const [titleBox, copyBox, mediaBox] = await Promise.all([title.boundingBox(), copy.boundingBox(), media.boundingBox()]);
    expect(titleBox).toBeTruthy();
    expect(copyBox).toBeTruthy();
    expect(mediaBox).toBeTruthy();
    expect(Math.abs(titleBox.y - copyBox.y)).toBeLessThanOrEqual(10);
    expect(copyBox.x).toBeGreaterThan(titleBox.x);
    expect(titleBox.y).toBeLessThan(mediaBox.y);
  });

  test('renders actual bankrotstvo round-video post with header above media on mobile', async ({ page }) => {
    await page.goto('/?channel=bankrotstvo-mustknow');
    await waitForFeedReady(page);

    const card = page.locator('.post-card--round-video-only').first();
    await expect(card).toBeVisible();
    await expect(card.locator('.post-card__title')).toHaveText('Видео-пост');
    await expect(card.locator('.post-card__copy')).toBeVisible();
    await expect(card.locator('.post-card__media video')).toHaveCount(0);
    await expect(card.locator('.media-video-note img, .media-video-note__placeholder')).toBeVisible();

    const [titleBox, copyBox, mediaBox] = await Promise.all([
      card.locator('.post-card__title').boundingBox(),
      card.locator('.post-card__copy').boundingBox(),
      card.locator('.post-card__media').boundingBox(),
    ]);

    expect(titleBox).toBeTruthy();
    expect(copyBox).toBeTruthy();
    expect(mediaBox).toBeTruthy();
    expect(Math.abs(titleBox.y - copyBox.y)).toBeLessThanOrEqual(10);
    expect(copyBox.x).toBeGreaterThan(titleBox.x);
    expect(titleBox.y).toBeLessThan(mediaBox.y);
  });

  test('opens actual bankrotstvo round-video viewer without endless pending state on mobile', async ({ page }) => {
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

  test('renders round-video poster preview on mobile and falls back cleanly in viewer', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'round-video-fallback-host-mobile';
      document.body.appendChild(host);
      const posterUrl = `${window.location.origin}/assets/channel-avatar.jpg`;
      const card = window.renderPostCard({
        id: 999994,
        date: new Date().toISOString(),
        text: '',
        text_html: '',
        photos: [],
        video_note: true,
        video_url: '/missing-round-video-mobile.mp4',
        video_poster: {
          thumb_url: posterUrl,
          feed_url: posterUrl,
          full_url: posterUrl,
        },
        tg_url: 'https://t.me/example/999994',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    await page.locator('#round-video-fallback-host-mobile').scrollIntoViewIfNeeded();
    await expect(page.locator('#round-video-fallback-host-mobile .media-video-note img')).toBeVisible();
    await page.locator('#round-video-fallback-host-mobile .media-trigger').click();
    await expect(page.locator('#viewer .viewer__fallback')).toContainText(/временно недоступно/i);
  });
});
