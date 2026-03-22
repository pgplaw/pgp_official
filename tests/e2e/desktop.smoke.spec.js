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
});
