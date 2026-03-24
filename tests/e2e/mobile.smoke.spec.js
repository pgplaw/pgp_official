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

    await page.evaluate(() => window.scrollTo({ top: 1600, behavior: 'auto' }));
    await page.waitForTimeout(120);
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
    await expect(title).toHaveText('Видео-пост');
    const [titleBox, copyBox] = await Promise.all([title.boundingBox(), copy.boundingBox()]);
    expect(titleBox).toBeTruthy();
    expect(copyBox).toBeTruthy();
    expect(Math.abs(titleBox.y - copyBox.y)).toBeLessThanOrEqual(10);
    expect(copyBox.x).toBeGreaterThan(titleBox.x);
  });
});
