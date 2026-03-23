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
});
