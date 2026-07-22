const fs = require('node:fs');
const path = require('node:path');
const { test, expect } = require('@playwright/test');
const {
  findMirroredRoundVideoPost,
  waitForFeedReady,
  openFirstViewerFromFeed,
  expectLifecycleRefreshPreservesFeedPosition,
  expectManualRefreshShowsContentLoader,
} = require('./helpers');

test.describe('Mobile smoke', () => {
  test('searches the full channel feed on mobile', async ({ page }) => {
    await page.goto('/?channel=pg-tax');
    await waitForFeedReady(page);

    const search = page.locator('#feedSearch');
    const searchInput = page.locator('#feedSearchInput');
    const searchStatus = page.locator('#feedSearchStatus');
    const searchMatches = page.locator('#postFeed .post-card__search-match');

    await expect(search).toBeVisible();
    await expect(searchInput).toHaveCSS('font-size', '16px');
    const searchBox = await search.boundingBox();
    expect(searchBox).not.toBeNull();
    expect(searchBox.x).toBeGreaterThanOrEqual(0);
    expect(searchBox.x + searchBox.width).toBeLessThanOrEqual(page.viewportSize().width);
    expect(searchBox.height).toBeGreaterThanOrEqual(47.5);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);

    await searchInput.fill('нал');
    await expect(searchMatches.first()).toBeVisible();
    await expect(searchStatus).toHaveText('Результат поиска');
    await expect(searchStatus).toHaveCSS('text-align', 'center');
    await expect(searchStatus).toHaveCSS('color', 'rgb(0, 96, 160)');

    await searchInput.fill('алог');
    await expect(searchStatus).toHaveText('Ничего не найдено');
    await expect(page.locator('.site-shell')).toHaveClass(/is-empty-search/);
    await expect(page.locator('#postFeed .post-card[data-post-id]')).toHaveCount(0);

    await page.locator('#feedSearchClear').click();
    await expect(page.locator('.site-shell')).not.toHaveClass(/is-empty-search/);
    await expect(page.locator('#postFeed .post-card[data-post-id]').first()).toBeVisible();
  });

  test('keeps mirrored custom emoji aligned with mobile post text', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'mobile-custom-emoji-host';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999997,
        date: new Date().toISOString(),
        text: 'Mobile custom emoji fixture',
        text_html: [
          'Before ',
          '<img class="post-custom-emoji" data-emoji-id="5321286874256412860" ',
          'src="data/channels/pgp-official/media/custom-emoji/5321286874256412860.webp" ',
          'alt="umbrella" width="24" height="24">',
          ' after',
        ].join(''),
        photos: [],
        tg_url: 'https://t.me/example/999997',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    const customEmoji = page.locator('#mobile-custom-emoji-host img.post-custom-emoji');
    await expect(customEmoji).toHaveCount(1);
    const emojiBox = await customEmoji.boundingBox();
    const textBox = await page.locator('#mobile-custom-emoji-host .post-card__text').boundingBox();
    expect(emojiBox).not.toBeNull();
    expect(textBox).not.toBeNull();
    expect(emojiBox.width).toBeGreaterThanOrEqual(20);
    expect(emojiBox.width).toBeLessThanOrEqual(24);
    expect(emojiBox.y).toBeGreaterThanOrEqual(textBox.y);
    expect(emojiBox.y + emojiBox.height).toBeLessThanOrEqual(textBox.y + textBox.height + 1);
  });

  test('keeps utility controls in the channel card on mobile', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const utilityActions = page.locator('.hero__panel > .hero__actions');
    await expect(page.locator('.hero__row--channel')).toBeHidden();
    await expect(utilityActions).toBeVisible();
    await expect(utilityActions.locator('#refreshButton')).toBeVisible();
    await expect(utilityActions.locator('#installAppButton')).toBeVisible();
    await expect(utilityActions.locator('.theme-toggle')).toBeVisible();
    await expect(page.locator('#channelNavActionsHost .hero__actions')).toHaveCount(0);
    await expect(utilityActions).toHaveCSS('display', 'grid');
    await expect(utilityActions).toHaveCSS('column-gap', '0px');
    await expect(utilityActions.locator('#refreshButton')).toHaveCSS('background-color', 'rgba(0, 0, 0, 0)');
    expect(await utilityActions.locator('#refreshButton').evaluate(
      (element) => getComputedStyle(element, '::after').content
    )).toBe('none');
    expect(await utilityActions.locator('#installAppButton').evaluate(
      (element) => getComputedStyle(element, '::after').content
    )).toBe('none');

    const expectThemeInsideActions = async () => {
      const [actionsBox, themeBox] = await Promise.all([
        utilityActions.boundingBox(),
        utilityActions.locator('.theme-toggle__track').boundingBox(),
      ]);
      expect(actionsBox).toBeTruthy();
      expect(themeBox).toBeTruthy();
      expect(themeBox.x).toBeGreaterThanOrEqual(actionsBox.x);
      expect(themeBox.x + themeBox.width).toBeLessThanOrEqual(actionsBox.x + actionsBox.width + 0.5);
      expect((actionsBox.x + actionsBox.width) - (themeBox.x + themeBox.width)).toBeGreaterThanOrEqual(8);
    };
    await expectThemeInsideActions();
    await utilityActions.locator('.theme-toggle').click();
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
    await expectThemeInsideActions();

    const contactBar = page.locator('.contact-bar');
    await expect(contactBar).toBeVisible();
    await expect(contactBar).toHaveCSS('column-gap', '0px');
    await expect(contactBar.locator('.contact-bar__item')).toHaveCount(4);
    await expect(contactBar.locator('.contact-bar__item').first()).toHaveCSS('border-top-width', '0px');
    expect(await contactBar.locator('.contact-bar__item').first().evaluate(
      (element) => getComputedStyle(element, '::after').content
    )).toBe('none');

    const firstContact = contactBar.locator('.contact-bar__item').first();
    await firstContact.evaluate((element) => {
      element.addEventListener('click', (event) => event.preventDefault(), { once: true });
    });
    await firstContact.tap();
    await expect(page.locator('html')).toHaveClass(/is-touch-input/);
    await expect(firstContact).toHaveCSS('background-color', 'rgba(0, 0, 0, 0)');
    await expect(firstContact).toHaveCSS('background-image', 'none');
    await firstContact.hover();
    await page.evaluate(() => document.documentElement.classList.add('is-touch-input'));
    await expect(firstContact).toHaveCSS('background-color', 'rgba(0, 0, 0, 0)');
    await expect(firstContact).toHaveCSS('background-image', 'none');
  });

  test('keeps the current feed position after the app resumes', async ({ page }) => {
    await expectLifecycleRefreshPreservesFeedPosition(page);
  });

  test('shows a centered content loader during a manual refresh', async ({ page }) => {
    await expectManualRefreshShowsContentLoader(page);
  });

  test('restores the feed position when the mobile OS reloads the suspended app', async ({ page }) => {
    await expectLifecycleRefreshPreservesFeedPosition(page, { reloadOnResume: true });
  });

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
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const ecosystemLink = page.locator('.contact-bar__item--ecosystem');
    await ecosystemLink.click({ noWaitAfter: true });
    await expect(page.locator('html')).toHaveClass(/is-page-leaving/);
    await expect(page).toHaveURL(/\/ecosystem\.html$/);
    await expect(page.locator('.ecosystem-shell')).toHaveCSS('animation-name', 'ecosystem-page-enter');

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
    expect(firstFeatureBox.y - (newsletterHeadBox.y + newsletterHeadBox.height)).toBeGreaterThanOrEqual(13.5);
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
    expect(touchTargetHeights.every((height) => height >= 43.5)).toBe(true);

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
    let releaseTargetFeed;
    const targetFeedGate = new Promise((resolve) => {
      releaseTargetFeed = resolve;
    });
    await page.route('**/data/channels/pg-antitrust/posts.json**', async (route) => {
      await targetFeedGate;
      await route.continue();
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);
    await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'dark'));

    const initialTitle = (await page.locator('#siteTitle').innerText()).trim();
    const nextButton = page.locator('#channelCarousel .channel-carousel__surface--current .channel-carousel__nav--next');
    const siteShell = page.locator('.site-shell');
    const content = page.locator('.content');
    const switchOverlay = page.locator('.channel-switch-overlay');
    await expect(nextButton).toBeVisible();

    await page.evaluate(() => window.scrollTo({ top: 720, behavior: 'auto' }));
    await expect.poll(() => page.evaluate(() => window.scrollY)).toBeGreaterThan(300);

    await nextButton.click();
    await expect(siteShell).toHaveClass(/is-channel-switching-mobile/);
    await expect(switchOverlay).toBeVisible();
    await expect(switchOverlay.locator('.channel-switch-overlay__status')).toHaveCSS('color', 'rgb(224, 96, 32)');
    await expect(switchOverlay.locator('.channel-switch-overlay__spinner')).toHaveCSS('border-top-color', 'rgb(224, 96, 32)');
    const [navBox, overlayBox] = await Promise.all([
      page.locator('.channel-nav').boundingBox(),
      switchOverlay.boundingBox(),
    ]);
    expect(navBox).toBeTruthy();
    expect(overlayBox).toBeTruthy();
    expect(overlayBox.y).toBeGreaterThanOrEqual(navBox.y + navBox.height - 1);
    await expect.poll(() => page.locator('.channel-nav').evaluate(
      (element) => Number.parseFloat(getComputedStyle(element, '::after').opacity)
    )).toBe(0);
    expect(await page.evaluate(() => window.scrollY)).toBeGreaterThan(300);
    await expect(content).toHaveCSS('transition-duration', '0.28s, 0.001s');
    await expect.poll(async () => Number.parseFloat(await content.evaluate((element) => getComputedStyle(element).opacity))).toBeLessThan(0.25);
    expect(Number.parseFloat(await content.evaluate((element) => getComputedStyle(element).opacity))).toBeGreaterThan(0.05);
    await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(0);
    releaseTargetFeed();

    await expect(siteShell).not.toHaveClass(/is-channel-switching/);
    await expect(switchOverlay).toBeHidden();
    await expect(content).toHaveCSS('opacity', '1');
    await expect(content).toHaveCSS('transition-duration', '0.52s, 0.56s');
    await waitForFeedReady(page);

    await expect(page.locator('#siteTitle')).not.toHaveText(initialTitle);
    expect(page.url()).not.toContain('channel=pgp-official');
  });

  test('keeps mobile channel header centered after viewport resize', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 820 });
    await page.goto('/?channel=pg-employment');
    await waitForFeedReady(page);

    const carousel = page.locator('#channelCarousel');
    const stage = carousel.locator('.channel-carousel__stage');
    const currentSurface = carousel.locator('.channel-carousel__surface--current');
    const avatar = currentSurface.locator('.channel-carousel__avatar img');
    const subtitle = currentSurface.locator('.channel-carousel__subtitle');
    const heroAvatar = page.locator('#channelAvatarWrap');

    await expect(currentSurface.locator('.channel-carousel__meta')).toHaveCount(0);
    await expect(avatar).toBeVisible();
    await expect(avatar).toHaveAttribute('src', /data\/channels\/pg-employment\/media\/channel-avatar\.jpg$/);
    await expect(subtitle).toHaveText('@pgEmployment');
    await expect(heroAvatar).toBeHidden();

    await page.setViewportSize({ width: 459, height: 820 });
    await expect.poll(async () => {
      const [stageBox, surfaceBox] = await Promise.all([
        stage.boundingBox(),
        currentSurface.boundingBox(),
      ]);
      if (!stageBox || !surfaceBox) return false;
      return Math.abs(stageBox.x - surfaceBox.x) <= 1
        && Math.abs(stageBox.width - surfaceBox.width) <= 1;
    }).toBe(true);

    const alignment = await currentSurface.evaluate((surface) => {
      const avatarBox = surface.querySelector('.channel-carousel__avatar')?.getBoundingClientRect();
      const copyBox = surface.querySelector('.channel-carousel__copy')?.getBoundingClientRect();
      const titleBox = surface.querySelector('.channel-carousel__title')?.getBoundingClientRect();
      const subtitleBox = surface.querySelector('.channel-carousel__subtitle')?.getBoundingClientRect();
      const disclosureBox = surface.querySelector('.channel-carousel__disclosure')?.getBoundingClientRect();
      if (!avatarBox || !copyBox || !titleBox || !subtitleBox || !disclosureBox) return null;
      const surfaceBox = surface.getBoundingClientRect();
      return {
        surfaceCenter: surfaceBox.left + (surfaceBox.width / 2),
        surfaceVerticalCenter: surfaceBox.top + (surfaceBox.height / 2),
        groupCenter: avatarBox.left + ((copyBox.right - avatarBox.left) / 2),
        avatarCenter: avatarBox.top + (avatarBox.height / 2),
        copyCenter: copyBox.top + (copyBox.height / 2),
        avatarRight: avatarBox.right,
        copyLeft: copyBox.left,
        titleLeft: titleBox.left,
        subtitleLeft: subtitleBox.left,
        contentBottom: Math.max(avatarBox.bottom, copyBox.bottom),
        disclosureTop: disclosureBox.top,
        disclosureCenter: disclosureBox.left + (disclosureBox.width / 2),
        disclosureWidth: disclosureBox.width,
        disclosureBottom: disclosureBox.bottom,
        surfaceBottom: surfaceBox.bottom,
      };
    });
    expect(alignment).toBeTruthy();
    expect(Math.abs(alignment.surfaceCenter - alignment.groupCenter)).toBeLessThanOrEqual(2);
    expect(Math.abs(alignment.surfaceVerticalCenter - alignment.avatarCenter)).toBeLessThanOrEqual(2);
    expect(Math.abs(alignment.avatarCenter - alignment.copyCenter)).toBeLessThanOrEqual(3);
    expect(Math.abs(alignment.surfaceCenter - alignment.disclosureCenter)).toBeLessThanOrEqual(1);
    expect(alignment.disclosureWidth).toBeGreaterThanOrEqual(44);
    expect(alignment.disclosureTop).toBeGreaterThanOrEqual(alignment.contentBottom - 1);
    expect(alignment.disclosureBottom).toBeLessThanOrEqual(alignment.surfaceBottom + 1);
    expect(Math.abs(alignment.titleLeft - alignment.subtitleLeft)).toBeLessThanOrEqual(1);
    expect(alignment.copyLeft).toBeGreaterThan(alignment.avatarRight);
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
    await expect(panel.locator('.channel-carousel__picker-meta')).toHaveCount(0);

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

  test('keeps edge-color side fills around narrow mobile images', async ({ page }) => {
    const postsPath = path.join(process.cwd(), 'docs', 'data', 'channels', 'pgp-official', 'posts.json');
    const postsPayload = JSON.parse(fs.readFileSync(postsPath, 'utf8'));
    const fixturePost = {
      id: 990006,
      date: new Date().toISOString(),
      text: 'Mobile image fill fixture',
      text_html: 'Mobile image fill fixture',
      views: 0,
      comments_count: 0,
      photos: [{
        thumb_url: 'assets/app-icon-192.png',
        feed_url: 'assets/app-icon-192.png',
        full_url: 'assets/app-icon-192.png',
        thumb_width: 192,
        thumb_height: 192,
        feed_width: 192,
        feed_height: 192,
        full_width: 192,
        full_height: 192,
        source_width: 192,
        source_height: 192,
      }],
      videos: [],
      video_url: null,
      tg_url: 'https://t.me/pgp_official/990006',
    };

    await page.route('**/data/channels/pgp-official/posts.json**', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          ...postsPayload,
          posts: [fixturePost, ...(postsPayload.posts || [])],
        }),
      });
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const trigger = page.locator('.post-card[data-post-id="990006"] .media-trigger');
    await expect(trigger).toHaveAttribute('data-fill-ready', 'true');
    const layout = await trigger.evaluate((element) => {
      const image = element.querySelector('img');
      const triggerRect = element.getBoundingClientRect();
      const imageRect = image.getBoundingClientRect();
      return {
        triggerWidth: triggerRect.width,
        triggerHeight: triggerRect.height,
        imageWidth: imageRect.width,
        imageHeight: imageRect.height,
        background: getComputedStyle(element).backgroundColor,
      };
    });

    expect(layout.triggerWidth - layout.imageWidth).toBeGreaterThan(20);
    expect(Math.abs(layout.triggerHeight - layout.imageHeight)).toBeLessThanOrEqual(1);
    expect(layout.background).not.toBe('rgba(0, 0, 0, 0)');
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
