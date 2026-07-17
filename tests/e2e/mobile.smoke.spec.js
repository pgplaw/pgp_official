const { test, expect } = require('@playwright/test');
const {
  findMirroredRoundVideoPost,
  waitForFeedReady,
  openFirstViewerFromFeed,
} = require('./helpers');

test.describe('Mobile smoke', () => {
  test('keeps all four contact actions in one row on narrow in-app viewports', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 800 });
    await page.goto('/');

    const contactBar = page.locator('.contact-bar');
    const contactItems = contactBar.locator('.contact-bar__item');
    await expect(contactItems).toHaveCount(4);

    const [barBox, itemBoxes] = await Promise.all([
      contactBar.boundingBox(),
      contactItems.evaluateAll((elements) => elements.map((element) => {
        const rect = element.getBoundingClientRect();
        return { left: rect.left, right: rect.right, top: rect.top, width: rect.width };
      })),
    ]);

    expect(barBox).not.toBeNull();
    expect(itemBoxes.every((box) => Math.abs(box.top - itemBoxes[0].top) <= 1)).toBe(true);
    expect(itemBoxes[0].left).toBeGreaterThanOrEqual(barBox.x - 1);
    expect(itemBoxes[3].right).toBeLessThanOrEqual(barBox.x + barBox.width + 1);
    expect(itemBoxes.every((box) => box.width >= 60)).toBe(true);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  });

  test('renders the ecosystem as a responsive mobile page without overflow', async ({ page }) => {
    await page.goto('/ecosystem.html');

    await expect(page.locator('.ecosystem-card')).toHaveCount(4);
    await expect(page.locator('.ecosystem-card__head').first()).toHaveCSS('justify-content', 'center');
    await expect(page.locator('.ecosystem-card__head').first()).toHaveCSS('text-align', 'center');
    await expect(page.locator('.ecosystem-card--video .ecosystem-card__head')).toHaveCSS('border-bottom-style', 'none');
    await expect(page.locator('.ecosystem-card--social .ecosystem-card__head')).toHaveCSS('border-bottom-style', 'none');
    await expect(page.locator('.video-platforms > span')).toHaveCSS('text-align', 'center');
    await expect(page.locator('.ecosystem-footer')).toHaveCSS('align-items', 'center');
    await expect(page.locator('.ecosystem-footer')).toHaveCSS('text-align', 'center');
    await expect(page.locator('.ecosystem-intro h1')).toHaveCSS('display', 'grid');
    await expect(page.locator('.video-previews > a')).toHaveCount(2);
    await expect(page.locator('.social-links > a')).toHaveCount(2);
    const knowledgeTiles = page.locator('.knowledge-tiles > a');
    await expect(knowledgeTiles).toHaveCount(3);
    await expect(knowledgeTiles.locator('.knowledge-tile__title > strong + svg')).toHaveCount(3);
    const newsletterFeatureIcons = page.locator('.ecosystem-feature-list img');
    await expect(newsletterFeatureIcons).toHaveCount(3);
    expect(await newsletterFeatureIcons.evaluateAll((images) => images.every(
      (image) => image.getAttribute('src').endsWith('.svg') && image.complete && image.naturalWidth > 0
    ))).toBe(true);
    await expect(page.locator('.ecosystem-home-button')).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);

    const [newsletterHeadBox, firstFeatureBox, newsletterCopyBox, newsletterButtonBox] = await Promise.all([
      page.locator('.ecosystem-card--newsletter .ecosystem-card__head').boundingBox(),
      page.locator('.ecosystem-feature-list li').first().boundingBox(),
      page.locator('.newsletter-copy').boundingBox(),
      page.locator('.newsletter-copy .ecosystem-button').boundingBox(),
    ]);
    expect(newsletterHeadBox).not.toBeNull();
    expect(firstFeatureBox).not.toBeNull();
    expect(newsletterCopyBox).not.toBeNull();
    expect(newsletterButtonBox).not.toBeNull();
    expect(firstFeatureBox.y - (newsletterHeadBox.y + newsletterHeadBox.height)).toBeGreaterThanOrEqual(14);
    const newsletterCopyCenter = newsletterCopyBox.x + (newsletterCopyBox.width / 2);
    const newsletterButtonCenter = newsletterButtonBox.x + (newsletterButtonBox.width / 2);
    expect(Math.abs(newsletterCopyCenter - newsletterButtonCenter)).toBeLessThanOrEqual(1);

    const [logoBox, titleBox, brandBox] = await Promise.all([
      page.locator('.ecosystem-intro__logo').boundingBox(),
      page.locator('.ecosystem-intro__title').boundingBox(),
      page.locator('.ecosystem-intro__brand').boundingBox(),
    ]);
    expect(logoBox).not.toBeNull();
    expect(titleBox).not.toBeNull();
    expect(brandBox).not.toBeNull();
    expect(logoBox.x).toBeLessThan(titleBox.x);
    expect(Math.abs(titleBox.x - brandBox.x)).toBeLessThanOrEqual(1);

    const mobileKnowledgeColumns = await page.locator('.knowledge-tiles').evaluate(
      (element) => getComputedStyle(element).gridTemplateColumns.split(' ').length
    );
    expect(mobileKnowledgeColumns).toBe(2);
    const [knowledgeTilesBox, mobileKnowledgeTileBoxes] = await Promise.all([
      page.locator('.knowledge-tiles').boundingBox(),
      knowledgeTiles.evaluateAll((tiles) => tiles.map((tile) => {
        const rect = tile.getBoundingClientRect();
        return { x: rect.x, width: rect.width };
      })),
    ]);
    expect(knowledgeTilesBox).not.toBeNull();
    expect(Math.abs(mobileKnowledgeTileBoxes[0].width - mobileKnowledgeTileBoxes[2].width)).toBeLessThanOrEqual(1);
    const knowledgeTilesCenter = knowledgeTilesBox.x + (knowledgeTilesBox.width / 2);
    const thirdKnowledgeTileCenter = mobileKnowledgeTileBoxes[2].x + (mobileKnowledgeTileBoxes[2].width / 2);
    expect(Math.abs(knowledgeTilesCenter - thirdKnowledgeTileCenter)).toBeLessThanOrEqual(1);
    const touchTargets = page.locator('.video-platforms__links a, .social-links > a, .ecosystem-home-button');
    const touchTargetHeights = await touchTargets.evaluateAll((elements) => elements.map((element) => element.getBoundingClientRect().height));
    expect(touchTargetHeights.every((height) => height >= 44)).toBe(true);

    await page.setViewportSize({ width: 320, height: 800 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    const narrowKnowledgeColumns = await page.locator('.knowledge-tiles').evaluate(
      (element) => getComputedStyle(element).gridTemplateColumns.split(' ').length
    );
    expect(narrowKnowledgeColumns).toBe(1);
    const narrowKnowledgeTileWidths = await knowledgeTiles.evaluateAll((tiles) => tiles.map(
      (tile) => tile.getBoundingClientRect().width
    ));
    expect(narrowKnowledgeTileWidths.every((width) => Math.abs(width - narrowKnowledgeTileWidths[0]) <= 1)).toBe(true);
    const narrowVideoColumns = await page.locator('.video-previews').evaluate(
      (element) => getComputedStyle(element).gridTemplateColumns.split(' ').length
    );
    expect(narrowVideoColumns).toBe(1);
  });

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
    await page.evaluate(() => {
      if (typeof window.closeViewer === 'function') {
        window.closeViewer();
        return;
      }
      document.getElementById('viewerClose')?.click();
    });
    await expect(page.locator('#viewer')).toBeHidden();
  });

  test('aligns deep-linked post flush to the mobile sticky header', async ({ page }) => {
    await page.goto('/?channel=pg-tax');
    await waitForFeedReady(page);

    const postId = await page.locator('.post-card').nth(2).getAttribute('data-post-id');
    expect(postId).toBeTruthy();

    await page.goto(`/?channel=pg-tax#post-${postId}`);
    await waitForFeedReady(page);
    await expect(page.locator(`#post-${postId}`)).toHaveClass(/post-card--targeted/);

    await expect.poll(async () => page.evaluate((id) => {
      const nav = document.querySelector('.channel-nav');
      const target = document.getElementById(`post-${id}`);
      if (!nav || !target) return null;
      return Math.round(target.getBoundingClientRect().top - nav.getBoundingClientRect().bottom);
    }, postId), { timeout: 2200 }).toBeLessThanOrEqual(18);
    await expect.poll(async () => page.evaluate((id) => {
      const nav = document.querySelector('.channel-nav');
      const target = document.getElementById(`post-${id}`);
      if (!nav || !target) return null;
      return Math.round(target.getBoundingClientRect().top - nav.getBoundingClientRect().bottom);
    }, postId), { timeout: 2200 }).toBeGreaterThanOrEqual(0);
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

  test('renders an actual mirrored round-video post with header above media on mobile', async ({ page }) => {
    const { channelKey, postId } = findMirroredRoundVideoPost({ requirePaged: true });
    await page.goto(`/?channel=${encodeURIComponent(channelKey)}#post-${postId}`);
    await waitForFeedReady(page);

    const card = page.locator(`#post-${postId}`);
    await expect(card).toBeVisible();
    await card.scrollIntoViewIfNeeded();
    await expect(card.locator('.post-card__title')).toHaveText('Видео-пост');
    await expect(card.locator('.post-card__copy')).toBeVisible();
    await expect(card.locator('.post-card__media video')).toHaveCount(0);
    await expect(card.locator('.media-video-note img, .media-video-note__placeholder')).toBeVisible();

    const headPrecedesMedia = await card.evaluate((node) => {
      const head = node.querySelector('.post-card__head');
      const media = node.querySelector('.post-card__media');
      if (!head || !media) {
        return false;
      }
      return Boolean(head.compareDocumentPosition(media) & Node.DOCUMENT_POSITION_FOLLOWING);
    });
    expect(headPrecedesMedia).toBe(true);
  });

  test('opens an actual mirrored round-video viewer without endless pending state on mobile', async ({ page }) => {
    const { channelKey, postId } = findMirroredRoundVideoPost({ requirePaged: true });
    await page.goto(`/?channel=${encodeURIComponent(channelKey)}#post-${postId}`);
    await waitForFeedReady(page);

    const card = page.locator(`#post-${postId}`);
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
    await expect(page.locator('#round-video-fallback-host-mobile .media-video-note')).toBeVisible();
    await expect(page.locator('#round-video-fallback-host-mobile .media-video-note img')).toHaveAttribute('src', /channel-avatar\.jpg/);
    await page.locator('#round-video-fallback-host-mobile .media-trigger').click();
    await expect(page.locator('#viewer .viewer__fallback')).toContainText(/временно недоступно/i);
  });
});
