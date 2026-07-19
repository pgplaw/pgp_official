const fs = require('fs');
const path = require('path');
const { test, expect } = require('@playwright/test');
const {
  findMirroredRoundVideoPost,
  waitForFeedReady,
  clickLoadMoreIfVisible,
  openFirstViewerFromFeed,
  expectLifecycleRefreshPreservesFeedPosition,
} = require('./helpers');

test.describe('Desktop smoke', () => {
  test('loads desktop shell and active channel feed', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await expect(page.locator('.channel-nav')).toBeVisible();
    await expect(page.locator('#siteTitle')).toContainText(/Пепеляев Групп|Pepeliaev Group/);
    await expect(page.locator('.contact-bar')).toBeVisible();
    const contactBar = page.locator('.contact-bar');
    const contactItems = contactBar.locator('.contact-bar__item');
    await expect(contactItems).toHaveCount(4);
    await expect(page.locator('#channelNavContactsHost > .contact-bar')).toBeVisible();
    await expect(contactBar).toHaveCSS('gap', '6px');
    await expect(contactBar).toHaveCSS('overflow', 'visible');
    await expect(contactItems.first()).toHaveCSS('transform', 'none');
    const contactIconBox = await contactItems.first().locator('svg').boundingBox();
    expect(contactIconBox).not.toBeNull();
    expect(Math.round(contactIconBox.width)).toBe(22);
    expect(Math.round(contactIconBox.height)).toBe(22);
    const contactBackgroundBeforeHover = await contactItems.first().evaluate(
      (element) => getComputedStyle(element).backgroundColor,
    );
    await contactItems.first().hover();
    await expect.poll(() => contactItems.first().evaluate(
      (element) => getComputedStyle(element).backgroundColor,
    )).not.toBe(contactBackgroundBeforeHover);
    await expect(contactItems.first()).not.toHaveCSS('transform', 'none');
    await expect(page.locator('#feedSearch')).toBeVisible();
    const leadTitle = page.locator('#siteTitle .hero__title-line--lead');
    const secondaryTitle = page.locator('#siteTitle .hero__title-line:not(.hero__title-line--lead)');
    const [leadColor, secondaryColor, leadSize, secondarySize] = await Promise.all([
      leadTitle.evaluate((element) => getComputedStyle(element).color),
      secondaryTitle.evaluate((element) => getComputedStyle(element).color),
      leadTitle.evaluate((element) => Number.parseFloat(getComputedStyle(element).fontSize)),
      secondaryTitle.evaluate((element) => Number.parseFloat(getComputedStyle(element).fontSize)),
    ]);
    expect(secondaryColor).not.toBe(leadColor);
    expect(secondarySize).toBeLessThan(leadSize * 0.65);
    const [avatarBox, titleBox, descriptionBox, panelBox] = await Promise.all([
      page.locator('#channelAvatarWrap').boundingBox(),
      page.locator('#siteTitle').boundingBox(),
      page.locator('#siteDescription').boundingBox(),
      page.locator('.hero__panel').boundingBox(),
    ]);
    expect(avatarBox).not.toBeNull();
    expect(titleBox).not.toBeNull();
    expect(descriptionBox).not.toBeNull();
    expect(panelBox).not.toBeNull();
    const titleCenter = titleBox.y + (titleBox.height / 2);
    const avatarCenter = avatarBox.y + (avatarBox.height / 2);
    expect(Math.abs(titleCenter - avatarCenter)).toBeLessThanOrEqual(8);
    expect(titleBox.x + titleBox.width).toBeLessThanOrEqual(descriptionBox.x - 12);
    expect(descriptionBox.x + descriptionBox.width).toBeLessThanOrEqual(panelBox.x - 12);
    expect(await page.locator('.post-card').count()).toBeGreaterThan(0);
  });

  test('matches only complete words and word prefixes in feed search', async ({ page }) => {
    await page.goto('/?channel=pg-tax');
    await waitForFeedReady(page);

    const searchInput = page.locator('#feedSearchInput');
    const searchStatus = page.locator('#feedSearchStatus');
    const searchMatches = page.locator('#postFeed .post-card__search-match');

    await searchInput.fill('нал');
    await expect(searchMatches.first()).toBeVisible();
    await expect(searchMatches.first()).toHaveText(/нал/i);
    await expect(searchStatus).toHaveText('Результат поиска');
    await expect(searchStatus).toHaveClass(/feed-search__status--result/);
    await expect(searchStatus).toHaveCSS('text-align', 'center');
    await expect(searchStatus).toHaveCSS('font-size', '14.4px');
    await expect(searchStatus).toHaveCSS('color', 'rgb(0, 96, 160)');

    await searchInput.fill('алог');
    await expect(searchStatus).toHaveText('Ничего не найдено');
    await expect(searchStatus).toHaveClass(/feed-search__status--empty/);
    await expect(searchStatus).toHaveCSS('text-align', 'center');
    await expect(page.locator('.site-shell')).toHaveClass(/is-empty-search/);
    await expect(page.locator('#postFeed .post-card[data-post-id]')).toHaveCount(0);
    await expect(searchMatches).toHaveCount(0);
    const footerBox = await page.locator('.site-footer').boundingBox();
    expect(footerBox).not.toBeNull();
    expect(footerBox.y + footerBox.height).toBeGreaterThanOrEqual(page.viewportSize().height - 40);

    await searchInput.fill('налог');
    await expect(searchMatches.first()).toBeVisible();
    await expect(searchMatches.first()).toHaveText(/налог/i);
    await expect(searchStatus).toHaveText('Результат поиска');
    await expect(searchStatus).toHaveClass(/feed-search__status--result/);
    await expect(page.locator('.site-shell')).not.toHaveClass(/is-empty-search/);
  });

  test('shows compact Russian tooltips for channel icons', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const dock = page.locator('#channelMenu');
    const actionsHost = page.locator('#channelNavActionsHost');
    const utilityActions = actionsHost.locator('.hero__actions');
    const tabs = dock.locator('.channel-tab');
    const active = dock.locator('.channel-tab[data-channel-key="pgp-official"]');
    const target = dock.locator('.channel-tab[data-channel-key="pg-tax"]');
    const neighbor = dock.locator('.channel-tab[data-channel-key="pg-antitrust"]');

    await expect(tabs).toHaveCount(8);
    await expect(dock.locator('.channel-tab__avatar')).toHaveCount(8);
    await expect(utilityActions).toBeVisible();
    await expect(utilityActions.locator('#refreshButton')).toBeVisible();
    await expect(utilityActions.locator('#installAppButton')).toBeVisible();
    await expect(utilityActions.locator('.theme-toggle')).toBeVisible();
    await expect(page.locator('.hero__panel .hero__actions')).toHaveCount(0);
    const [dockBox, actionsBox] = await Promise.all([dock.boundingBox(), utilityActions.boundingBox()]);
    expect(dockBox).not.toBeNull();
    expect(actionsBox).not.toBeNull();
    expect(actionsBox.x).toBeGreaterThanOrEqual(dockBox.x + dockBox.width);
    await expect(target.locator('.channel-tab__title')).toHaveText('Налоги');
    await expect(target.locator('.channel-tab__subtitle')).toHaveText('PG Tax');
    await expect(target.locator('.channel-tab__subtitle')).toBeHidden();
    await expect(target.locator('.channel-tab__label')).toHaveCSS('opacity', '0');
    await expect.poll(() => active.evaluate(
      (element) => new DOMMatrix(getComputedStyle(element).transform).a
    )).toBeGreaterThan(1.05);
    await expect(active).toHaveCSS('animation-name', 'active-tab-breathe');
    await expect.poll(() => active.evaluate(
      (element) => Math.abs(new DOMMatrix(getComputedStyle(element).transform).f)
    )).toBeLessThan(0.1);
    await expect.poll(() => active.evaluate((element) => {
      const inactive = element.parentElement?.querySelector('.channel-tab:not(.is-active)');
      if (!inactive) return Number.POSITIVE_INFINITY;
      const activeRect = element.getBoundingClientRect();
      const inactiveRect = inactive.getBoundingClientRect();
      const activeCenter = activeRect.top + (activeRect.height / 2);
      const inactiveCenter = inactiveRect.top + (inactiveRect.height / 2);
      return Math.abs(activeCenter - inactiveCenter);
    })).toBeLessThan(0.5);
    await expect.poll(() => active.evaluate(
      (element) => getComputedStyle(element, '::after').content
    )).toBe('none');

    await target.hover();
    await expect(target.locator('.channel-tab__label')).toHaveCSS('opacity', '1');
    await expect.poll(() => target.locator('.channel-tab__icon').evaluate(
      (element) => Number.parseFloat(getComputedStyle(element).width),
    )).toBeLessThan(60);
    await expect(target.locator('.channel-tab__avatar')).toHaveCSS('opacity', '1');
    await expect(target.locator('.channel-tab__label')).toHaveCSS('pointer-events', 'none');
    await expect.poll(() => target.evaluate((element) => new DOMMatrix(getComputedStyle(element).transform).a)).toBeGreaterThan(1.1);
    await expect.poll(() => neighbor.evaluate((element) => new DOMMatrix(getComputedStyle(element).transform).a)).toBeGreaterThan(1);
    const [targetBox, labelBox] = await Promise.all([
      target.boundingBox(),
      target.locator('.channel-tab__label').boundingBox(),
    ]);
    expect(targetBox).not.toBeNull();
    expect(labelBox).not.toBeNull();
    expect(labelBox.y).toBeGreaterThanOrEqual(targetBox.y + targetBox.height - 3);
    expect(labelBox.width).toBeLessThan(220);
    await neighbor.hover();
    await expect(neighbor.locator('.channel-tab__label')).toHaveCSS('opacity', '1');
    await expect(target.locator('.channel-tab__label')).toHaveCSS('opacity', '0');
  });

  test('uses a compact title hierarchy for the long antitrust channel name', async ({ page }) => {
    await page.goto('/?channel=pg-antitrust');
    await waitForFeedReady(page);

    const title = page.locator('#siteTitle');
    const lead = title.locator('.hero__title-line--lead');
    const subtitle = title.locator('.hero__title-line:not(.hero__title-line--lead)');
    await expect(title).toHaveClass(/hero__title--compact/);
    await expect(lead).toHaveText('Антимонопольное право');
    await expect(subtitle).toHaveText('PG Antitrust');

    const [leadSize, subtitleSize, leadBox, titleBox, avatarBox] = await Promise.all([
      lead.evaluate((element) => Number.parseFloat(getComputedStyle(element).fontSize)),
      subtitle.evaluate((element) => Number.parseFloat(getComputedStyle(element).fontSize)),
      lead.boundingBox(),
      title.boundingBox(),
      page.locator('#channelAvatarWrap').boundingBox(),
    ]);
    expect(subtitleSize).toBeLessThan(leadSize * 0.65);
    expect(leadBox).not.toBeNull();
    expect(leadBox.height).toBeLessThan(52);
    expect(titleBox).not.toBeNull();
    expect(avatarBox).not.toBeNull();
    const titleCenter = titleBox.y + (titleBox.height / 2);
    const avatarCenter = avatarBox.y + (avatarBox.height / 2);
    expect(Math.abs(titleCenter - avatarCenter)).toBeLessThanOrEqual(2);
  });

  test('shows hover feedback on Telegram channel title and post links', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const channelTitle = page.locator('#siteTitle');
    const channelTitleLead = channelTitle.locator('.hero__title-line--lead');
    const channelBefore = await channelTitle.evaluate((element) => ({
      leadColor: getComputedStyle(element.querySelector('.hero__title-line--lead')).color,
      background: getComputedStyle(element).backgroundColor,
      transform: getComputedStyle(element).transform,
    }));
    await channelTitle.hover();
    await expect.poll(() => channelTitleLead.evaluate(
      (element) => getComputedStyle(element).color
    )).not.toBe(channelBefore.leadColor);
    await expect(channelTitle).toHaveCSS('background-color', channelBefore.background);
    await expect(channelTitle).toHaveCSS('transform', channelBefore.transform);

    const postLink = page.locator('.post-card__link').first();
    await expect(postLink).toBeVisible();
    const postBefore = await postLink.evaluate((element) => ({
      color: getComputedStyle(element).color,
      background: getComputedStyle(element).backgroundColor,
      transform: getComputedStyle(element).transform,
    }));
    await postLink.hover();
    await expect.poll(() => postLink.evaluate(
      (element) => getComputedStyle(element).color
    )).not.toBe(postBefore.color);
    await expect(postLink).toHaveCSS('background-color', postBefore.background);
    await expect(postLink).toHaveCSS('transform', postBefore.transform);
    await expect(postLink).toHaveCSS('text-decoration-line', 'none');
    await expect(postLink).toHaveCSS('box-shadow', 'none');
  });

  test('keeps the current feed position after returning to a desktop tab', async ({ page }) => {
    await expectLifecycleRefreshPreservesFeedPosition(page);
  });

  test('publishes an installable manifest with required desktop icon sizes', async ({ page }) => {
    await page.goto('/');

    const manifest = await page.evaluate(() => fetch('./manifest.webmanifest', { cache: 'no-store' }).then(
      (response) => response.json()
    ));
    const requiredIcons = manifest.icons.filter((icon) => ['192x192', '512x512'].includes(icon.sizes));
    expect(requiredIcons).toHaveLength(2);
    expect(requiredIcons.map((icon) => icon.sizes).sort()).toEqual(['192x192', '512x512']);
    expect(requiredIcons.every((icon) => icon.type === 'image/png')).toBe(true);

    const imageSizes = await page.evaluate((icons) => Promise.all(icons.map((icon) => new Promise((resolve, reject) => {
      const image = new Image();
      image.onload = () => resolve(`${image.naturalWidth}x${image.naturalHeight}`);
      image.onerror = reject;
      image.src = icon.src;
    }))), requiredIcons);
    expect(imageSizes.sort()).toEqual(['192x192', '512x512']);

    const cdp = await page.context().newCDPSession(page);
    await expect.poll(async () => {
      const result = await cdp.send('Page.getInstallabilityErrors');
      return result.installabilityErrors;
    }).toEqual([]);
  });

  test('opens the ecosystem page from the fourth desktop contact action', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const contactItems = page.locator('.contact-bar__item');
    await expect(contactItems).toHaveCount(4);
    const ecosystemLink = page.locator('.contact-bar__item--ecosystem');
    await expect(ecosystemLink).toBeVisible();
    await expect(ecosystemLink).toContainText('Экосистема');

    await ecosystemLink.click();
    await expect(page).toHaveURL(/\/ecosystem\.html$/);
    await expect(page.locator('.ecosystem-nav')).toHaveCount(0);
    await expect(page.locator('.ecosystem-intro__title')).toHaveText('Экосистема');
    await expect(page.locator('.ecosystem-intro > p')).toHaveText('Профессиональная информация на одной странице');
    await expect(page.locator('.ecosystem-intro__eyebrow')).toHaveCount(0);
    await expect(page.locator('.ecosystem-nav__brand')).toHaveCount(0);
    const ecosystemLogo = page.locator('.ecosystem-intro__logo');
    await expect(ecosystemLogo).toHaveAttribute('src', 'data/channels/pgp-official/media/channel-avatar.jpg');
    expect(await ecosystemLogo.evaluate((image) => image.complete && image.naturalWidth > 0)).toBe(true);
    const ecosystemBrand = page.locator('.ecosystem-intro__brand');
    await expect(ecosystemBrand).toHaveText('«Пепеляев Групп»');
    await expect(ecosystemBrand).toHaveAttribute('href', 'https://www.pgplaw.ru/');
    await expect(page.locator('.ecosystem-intro__separator')).toHaveCount(1);
    await expect(ecosystemBrand).toHaveCSS('border-left-width', '0px');
    const [ecosystemTitleStyle, ecosystemBrandStyle] = await Promise.all([
      page.locator('.ecosystem-intro__title').evaluate((element) => {
        const style = getComputedStyle(element);
        return { fontFamily: style.fontFamily, fontSize: style.fontSize, fontWeight: style.fontWeight };
      }),
      ecosystemBrand.evaluate((element) => {
        const style = getComputedStyle(element);
        return { fontFamily: style.fontFamily, fontSize: style.fontSize, fontWeight: style.fontWeight };
      }),
    ]);
    expect(ecosystemBrandStyle).toEqual(ecosystemTitleStyle);
    await expect(page.locator('.ecosystem-card')).toHaveCount(4);
    expect(await page.locator('.ecosystem-card').evaluateAll((cards) => cards.every(
      (card) => getComputedStyle(card).borderTopColor === 'rgb(160, 32, 128)'
    ))).toBe(true);
    await expect(page.locator('.ecosystem-card__index')).toHaveCount(0);
    await expect(page.locator('#newsletterTitle')).toHaveText('Рассылки');
    await expect(page.locator('#videoTitle')).toHaveText('Экспертные видео');
    await expect(page.locator('#socialTitle')).toHaveText('Социальные сети');
    await expect(page.locator('#knowledgeTitle')).toHaveText('База знаний');
    await expect(page.locator('.newsletter-layout')).toHaveCSS('align-content', 'center');
    await expect(page.locator('.video-content')).toHaveCSS('align-content', 'center');
    await expect(page.locator('.social-links')).toHaveCSS('align-content', 'center');
    await expect(page.locator('.newsletter-visual')).toBeVisible();
    const [newsletterCopyBox, newsletterVisualBox] = await Promise.all([
      page.locator('.newsletter-copy').boundingBox(),
      page.locator('.newsletter-visual').boundingBox(),
    ]);
    expect(newsletterCopyBox).not.toBeNull();
    expect(newsletterVisualBox).not.toBeNull();
    expect(Math.abs(newsletterCopyBox.y - newsletterVisualBox.y)).toBeLessThanOrEqual(1);
    const newsletterFeatureIcons = page.locator('.ecosystem-feature-list img');
    await expect(newsletterFeatureIcons).toHaveCount(3);
    await expect(page.locator('.ecosystem-feature-list li').first()).toContainText('Анонсы мероприятий');
    expect(await newsletterFeatureIcons.evaluateAll((images) => images.every(
      (image) => image.getAttribute('src').endsWith('.svg') && image.complete && image.naturalWidth > 0
    ))).toBe(true);
    const newsletterButton = page.locator('.ecosystem-card--newsletter .ecosystem-button');
    const newsletterButtonBox = await newsletterButton.boundingBox();
    expect(newsletterButtonBox).not.toBeNull();
    expect(Math.abs(newsletterCopyBox.width - newsletterButtonBox.width)).toBeLessThanOrEqual(1);
    await expect(newsletterButton).toHaveCSS('background-color', 'rgb(145, 39, 141)');
    await newsletterButton.hover();
    await expect(newsletterButton).toHaveCSS('background-color', 'rgb(167, 60, 162)');
    await expect(newsletterButton).toHaveCSS('transform', /matrix/);
    const telegramIcon = page.locator('.social-links a[href^="https://telegram"] > img');
    await expect(telegramIcon).toHaveAttribute('src', 'assets/ecosystem/telegram.svg?v=3');
    expect(await telegramIcon.evaluate((image) => image.complete && image.naturalWidth > 0)).toBe(true);
    const maxIcon = page.locator('.social-links a[href^="https://max.ru"] > img');
    await expect(maxIcon).toHaveAttribute('src', 'assets/ecosystem/max.svg?v=2');
    expect(await maxIcon.evaluate((image) => image.complete && image.naturalWidth > 0)).toBe(true);
    const socialTiles = page.locator('.social-links > a');
    await expect(socialTiles).toHaveCount(2);
    await expect(socialTiles.first()).toHaveCSS('box-shadow', 'none');
    const [telegramTileBox, maxTileBox] = await Promise.all([
      socialTiles.first().boundingBox(),
      socialTiles.last().boundingBox(),
    ]);
    expect(telegramTileBox).not.toBeNull();
    expect(maxTileBox).not.toBeNull();
    expect(maxTileBox.y - (telegramTileBox.y + telegramTileBox.height)).toBeGreaterThanOrEqual(12);
    await socialTiles.first().hover();
    await expect(socialTiles.first()).not.toHaveCSS('box-shadow', 'none');
    const knowledgeImage = page.locator('.knowledge-visual img');
    await expect(knowledgeImage).toHaveAttribute('src', 'assets/ecosystem/knowledge-book-open.svg');
    expect(await knowledgeImage.evaluate((image) => image.complete && image.naturalWidth > 0)).toBe(true);
    await expect(page.locator('.knowledge-copy, .knowledge-mark')).toHaveCount(0);
    const knowledgeTiles = page.locator('.knowledge-tiles > a');
    await expect(knowledgeTiles).toHaveCount(3);
    await expect(knowledgeTiles.locator('.knowledge-tile__title > strong + svg')).toHaveCount(3);
    const knowledgeTileHeights = await knowledgeTiles.evaluateAll((tiles) => tiles.map(
      (tile) => tile.getBoundingClientRect().height
    ));
    expect(knowledgeTileHeights.every((height) => height <= 90)).toBe(true);
    await expect(knowledgeTiles.nth(0)).toHaveAttribute('href', 'https://www.pgplaw.ru/analytics-and-brochures/legislation/');
    await expect(knowledgeTiles.nth(1)).toHaveAttribute('href', 'https://www.pgplaw.ru/analytics-and-brochures/alerts/');
    await expect(knowledgeTiles.nth(2)).toHaveAttribute('href', 'https://www.pgplaw.ru/analytics-and-brochures/books/');
    const knowledgeButton = page.locator('.ecosystem-button--knowledge');
    await expect(knowledgeButton).toHaveAttribute('href', 'https://www.pgplaw.ru/analytics-and-brochures/');
    await expect(knowledgeButton).toHaveCSS('background-color', 'rgb(145, 39, 141)');
    const [knowledgeLayoutBox, knowledgeButtonBox] = await Promise.all([
      page.locator('.knowledge-layout').boundingBox(),
      knowledgeButton.boundingBox(),
    ]);
    expect(knowledgeLayoutBox).not.toBeNull();
    expect(knowledgeButtonBox).not.toBeNull();
    const knowledgeLayoutCenter = knowledgeLayoutBox.x + (knowledgeLayoutBox.width / 2);
    const knowledgeButtonCenter = knowledgeButtonBox.x + (knowledgeButtonBox.width / 2);
    expect(Math.abs(knowledgeLayoutCenter - knowledgeButtonCenter)).toBeLessThanOrEqual(1);
    expect(Math.abs(knowledgeLayoutBox.width - knowledgeButtonBox.width)).toBeLessThanOrEqual(1);
    const homeButton = page.locator('.ecosystem-home-button');
    await expect(homeButton).toHaveText('Вернуться на Главную');
    await expect(homeButton).toHaveAttribute('href', './');
    await homeButton.hover();
    await expect(homeButton).toHaveCSS('box-shadow', 'none');
    const [mainBox, homeButtonBox, footerBox] = await Promise.all([
      page.locator('main').boundingBox(),
      homeButton.boundingBox(),
      page.locator('.ecosystem-footer').boundingBox(),
    ]);
    expect(mainBox).not.toBeNull();
    expect(homeButtonBox).not.toBeNull();
    expect(footerBox).not.toBeNull();
    expect(Math.abs(mainBox.width - homeButtonBox.width)).toBeLessThanOrEqual(1);
    expect(homeButtonBox.y).toBeGreaterThanOrEqual(mainBox.y + mainBox.height);
    expect(footerBox.y).toBeGreaterThanOrEqual(homeButtonBox.y + homeButtonBox.height);

    const platformImages = page.locator('.video-platforms__links img');
    await expect(platformImages.first()).toHaveCSS('filter', 'none');
    await expect(platformImages.first()).toHaveCSS('opacity', '1');
    await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'dark'));
    const platformLinks = page.locator('.video-platforms__links a');
    await expect(platformLinks).toHaveCount(3);
    await expect(platformImages.first()).toHaveCSS('filter', 'none');
    await expect(platformImages.first()).toHaveCSS('opacity', '1');
    await platformLinks.first().hover();
    await expect(platformLinks.first()).toHaveCSS('border-color', 'rgb(255, 0, 51)');
    await expect(platformLinks.first()).toHaveCSS('transform', /matrix/);
  });

  test('switches channel from desktop menu and updates hero + url', async ({ page }) => {
    let releaseTargetFeed;
    const targetFeedGate = new Promise((resolve) => {
      releaseTargetFeed = resolve;
    });
    await page.route('**/data/channels/pg-tax/posts.json**', async (route) => {
      await targetFeedGate;
      await route.continue();
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const initialTitle = (await page.locator('#siteTitle').innerText()).trim();
    const targetButton = page.locator('#channelMenu .channel-tab[data-channel-key="pg-tax"]');
    const siteShell = page.locator('.site-shell');
    const content = page.locator('.content');
    await expect(targetButton).toBeVisible();

    await targetButton.click();
    await expect(siteShell).toHaveClass(/is-channel-switching-desktop/);
    await expect(content).toHaveCSS('transition-duration', '0.28s, 0.3s');
    await expect.poll(async () => Number.parseFloat(await content.evaluate((element) => getComputedStyle(element).opacity))).toBeLessThan(0.02);
    releaseTargetFeed();

    await expect(siteShell).not.toHaveClass(/is-channel-switching/);
    await expect(content).toHaveCSS('opacity', '1');
    await expect(content).toHaveCSS('transition-duration', '0.5s, 0.58s');
    await waitForFeedReady(page);

    await expect(page).toHaveURL(/channel=pg-tax/);
    await expect(page.locator('#channelLink')).toContainText('@PG_Tax');
    await expect(page.locator('#siteTitle')).not.toHaveText(initialTitle);
  });

  test('prefetches the neighboring channel with a fresh request before switching', async ({ page }) => {
    const feedPath = path.join(process.cwd(), 'docs', 'data', 'channels', 'pg-antitrust', 'posts.json');
    const freshFeed = JSON.parse(fs.readFileSync(feedPath, 'utf8'));
    const latestPost = freshFeed.posts?.[0];
    expect(latestPost?.id).toBeTruthy();
    const staleFeed = {
      ...freshFeed,
      generated_at: '2020-01-01T00:00:00Z',
      build_id: 'stale-prefetch',
      posts: (freshFeed.posts || []).slice(1),
    };
    const feedRequests = [];

    await page.route('**/data/channels/pg-antitrust/posts.json**', async (route) => {
      const requestUrl = route.request().url();
      feedRequests.push(requestUrl);
      const payload = new URL(requestUrl).searchParams.has('t') ? freshFeed : staleFeed;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(payload),
      });
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);
    await expect.poll(() => feedRequests.length).toBeGreaterThanOrEqual(1);
    expect(new URL(feedRequests[0]).searchParams.has('t')).toBe(true);

    await page.locator('#channelMenu .channel-tab[data-channel-key="pg-antitrust"]').click();
    await waitForFeedReady(page);

    await expect(page.locator(`#post-${latestPost.id}`)).toBeVisible();
    expect(feedRequests.every((url) => new URL(url).searchParams.has('t'))).toBe(true);
  });

  test('expires in-memory channel feeds after one sync interval', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const result = await page.evaluate(() => {
      const originalDateNow = Date.now;
      let now = originalDateNow();
      try {
        Date.now = () => now;
        window.rememberFeedPayload('cache-expiry-test', { posts: [{ id: 101 }] });
        const fresh = window.readCachedFeedPayload('cache-expiry-test');
        now += 6 * 60 * 1000;
        const expired = window.readCachedFeedPayload('cache-expiry-test');
        return {
          freshPostId: fresh?.posts?.[0]?.id || null,
          expired,
        };
      } finally {
        Date.now = originalDateNow;
      }
    });

    expect(result.freshPostId).toBe(101);
    expect(result.expired).toBeNull();
  });

  test('builds versioned feed urls as relative channel data paths', async ({ page }) => {
    await page.goto('/?channel=investment-law');
    await waitForFeedReady(page);

    const urls = await page.evaluate(() => ({
      feed: typeof window.buildFeedUrl === 'function'
        ? window.buildFeedUrl('investment-law')
        : null,
      page: typeof window.buildPageUrl === 'function'
        ? window.buildPageUrl('investment-law', 2, { buildId: 'abc123' })
        : null,
      comments: typeof window.buildCommentsUrl === 'function'
        ? window.buildCommentsUrl('investment-law', 1001, { buildId: 'abc123' })
        : null,
    }));

    expect(urls.feed).toBe('data/channels/investment-law/posts.json');
    expect(urls.page).toBe('data/channels/investment-law/pages/2.json?v=abc123');
    expect(urls.comments).toBe('data/channels/investment-law/comments/1001.json?v=abc123');
  });

  test('keeps narrow desktop channel dock contained and clickable', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const menu = page.locator('#channelMenu');
    const iconBounds = await menu.locator('.channel-tab').evaluateAll((tabs) => tabs.map((tab) => {
      const rect = tab.getBoundingClientRect();
      return { left: rect.left, right: rect.right };
    }));
    const viewport = page.viewportSize();
    expect(viewport).toBeTruthy();
    expect(iconBounds).toHaveLength(8);
    expect(iconBounds.every((rect) => rect.left >= 0 && rect.right <= viewport.width)).toBe(true);

    await menu.hover();
    await page.mouse.wheel(0, 900);
    await expect.poll(() => page.evaluate(() => window.scrollY)).toBeGreaterThan(300);

    const initialTitle = (await page.locator('#siteTitle').innerText()).trim();
    const targetButton = page.locator('#channelMenu .channel-tab[data-channel-key="pg-employment"]');
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

  test('renders attached post videos alongside photos and opens them in the viewer', async ({ page }) => {
    const docsRoot = path.join(process.cwd(), 'docs');
    const channelDataRoot = path.join(docsRoot, 'data', 'channels');
    const availableVideoPath = fs.readdirSync(channelDataRoot, { recursive: true })
      .map((entry) => path.join(channelDataRoot, entry.toString()))
      .find((entryPath) => entryPath.endsWith('.mp4'));
    expect(availableVideoPath, 'Expected at least one local mirrored mp4 for the attached-video regression.').toBeTruthy();
    const availablePosterPath = fs.readdirSync(channelDataRoot, { recursive: true })
      .map((entry) => path.join(channelDataRoot, entry.toString()))
      .find((entryPath) => /video-posters[\\/].+\.jpg$/i.test(entryPath));
    const localVideoUrl = path.relative(docsRoot, availableVideoPath).replace(/\\/g, '/');
    const localPosterUrl = availablePosterPath
      ? path.relative(docsRoot, availablePosterPath).replace(/\\/g, '/')
      : null;

    await page.goto('/?channel=pg-antitrust');
    await waitForFeedReady(page);

    await page.evaluate(({ localVideoUrl, localPosterUrl }) => {
      const host = document.createElement('div');
      host.id = 'attached-video-host';
      document.body.appendChild(host);
      const photo = 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360"><rect width="100%" height="100%" fill="%232b3350"/></svg>';
      const card = window.renderPostCard({
        id: 999995,
        date: new Date().toISOString(),
        text: 'mixed media',
        text_html: '<p>Пост с фотографией и прикрепленным видео.</p>',
        photos: [{
          thumb_url: photo,
          feed_url: photo,
          full_url: photo,
          thumb_width: 640,
          thumb_height: 360,
          feed_width: 640,
          feed_height: 360,
          full_width: 640,
          full_height: 360,
        }],
        videos: [{
          url: localVideoUrl,
          source_url: localVideoUrl,
          width: 640,
          height: 360,
          poster: localPosterUrl ? {
            thumb_url: localPosterUrl,
            feed_url: localPosterUrl,
            full_url: localPosterUrl,
            thumb_width: 640,
            thumb_height: 360,
            feed_width: 640,
            feed_height: 360,
            full_width: 640,
            full_height: 360,
          } : null,
        }],
        tg_url: 'https://t.me/example/999995',
        comments_count: 0,
      });
      host.appendChild(card);
    }, { localVideoUrl, localPosterUrl });

    const triggers = page.locator('#attached-video-host .media-trigger');
    await expect(triggers).toHaveCount(2);
    await expect(triggers.nth(1).locator('video')).toHaveCount(1);

    await triggers.nth(1).click();
    await expect(page.locator('#viewer')).toBeVisible();
    await expect(page.locator('#viewer .viewer__slide video').first()).toHaveAttribute('src', new RegExp(localVideoUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
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
    await expect.poll(async () => page.evaluate((id) => {
      const nav = document.querySelector('.channel-nav');
      const target = document.getElementById(`post-${id}`);
      if (!nav || !target) return null;
      return Math.round(target.getBoundingClientRect().top - nav.getBoundingClientRect().bottom);
    }, postId), { timeout: 2000 }).toBeLessThanOrEqual(16);
    await expect.poll(async () => page.evaluate((id) => {
      const nav = document.querySelector('.channel-nav');
      const target = document.getElementById(`post-${id}`);
      if (!nav || !target) return null;
      return Math.round(target.getBoundingClientRect().top - nav.getBoundingClientRect().bottom);
    }, postId), { timeout: 2000 }).toBeGreaterThanOrEqual(0);
  });

  test('routes mirrored telegram post links to the local post page instead of opening telegram', async ({ page }) => {
    const targetFeedPath = path.join(process.cwd(), 'docs', 'data', 'channels', 'pg-tax', 'posts.json');
    const targetFeed = JSON.parse(fs.readFileSync(targetFeedPath, 'utf8'));
    const targetPostId = Number(targetFeed.posts?.[0]?.id || 0);
    expect(targetPostId).toBeGreaterThan(0);

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(({ targetPostId }) => {
      const host = document.createElement('div');
      host.id = 'telegram-mirror-link-host';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999991,
        date: new Date().toISOString(),
        text: 'Ссылка на зеркальный пост',
        text_html: `<p><a href="https://t.me/PG_Tax/${targetPostId}">Открыть налоговый пост</a></p>`,
        photos: [],
        tg_url: 'https://t.me/example/999991',
        comments_count: 0,
      });
      host.appendChild(card);
    }, { targetPostId });

    const link = page.locator('#telegram-mirror-link-host .post-card__text a').first();
    await expect(link).toBeVisible();
    await expect.poll(async () => link.evaluate((node) => node.getAttribute('href') || '')).toContain(`channel=pg-tax#post-${targetPostId}`);

    await link.click();
    await waitForFeedReady(page);

    await expect(page).toHaveURL(new RegExp(`channel=pg-tax.*#post-${targetPostId}`));
    await expect(page.locator(`#post-${targetPostId}`)).toHaveClass(/post-card--targeted/);
  });

  test('keeps the post footer action pointed at the exact Telegram post', async ({ page }) => {
    const feedPath = path.join(process.cwd(), 'docs', 'data', 'channels', 'pgp-official', 'posts.json');
    const feed = JSON.parse(fs.readFileSync(feedPath, 'utf8'));
    const post = (feed.posts || []).find((entry) => entry?.id && entry?.tg_url);
    expect(post).toBeTruthy();
    const telegramPath = new URL(post.tg_url).pathname.split('/').filter(Boolean);
    if (telegramPath[0] === 's') telegramPath.shift();
    const expectedAppHref = `tg://resolve?domain=${encodeURIComponent(telegramPath[0])}&post=${encodeURIComponent(telegramPath[1])}`;

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const link = page.locator(`#post-${post.id} .post-card__link`);
    await expect(link).toHaveAttribute('href', post.tg_url);
    await expect(link).toHaveAttribute('data-telegram-external', 'true');
    await expect(link).toHaveAttribute('data-telegram-web-href', post.tg_url);
    await expect(link).toHaveAttribute('data-telegram-app-href', expectedAppHref);
    await expect(link).not.toHaveAttribute('data-telegram-mirror-href', /.+/);
  });

  test('forces a fresh mirrored channel feed before falling back from a telegram post link', async ({ page }) => {
    const targetFeedPath = path.join(process.cwd(), 'docs', 'data', 'channels', 'pg-tax', 'posts.json');
    const freshFeed = JSON.parse(fs.readFileSync(targetFeedPath, 'utf8'));
    const targetPostId = Number(freshFeed.posts?.[0]?.id || 0);
    expect(targetPostId).toBeGreaterThan(0);

    const staleFeed = {
      ...freshFeed,
      pagination: {
        ...(freshFeed.pagination || {}),
        total_pages: 1,
      },
      posts: (freshFeed.posts || []).filter((post) => Number(post?.id) !== targetPostId).slice(0, 3),
    };

    await page.route('**/data/channels/pg-tax/posts.json**', async (route) => {
      const url = route.request().url();
      const payload = url.includes('t=') ? freshFeed : staleFeed;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(payload),
      });
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(({ targetPostId }) => {
      const host = document.createElement('div');
      host.id = 'telegram-mirror-stale-link-host';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999992,
        date: new Date().toISOString(),
        text: 'Ссылка на зеркальный пост с устаревшим кешем',
        text_html: `<p><a href="https://t.me/PG_Tax/${targetPostId}">Открыть налоговый пост</a></p>`,
        photos: [],
        tg_url: 'https://t.me/example/999992',
        comments_count: 0,
      });
      host.appendChild(card);
    }, { targetPostId });

    const link = page.locator('#telegram-mirror-stale-link-host .post-card__text a').first();
    await expect(link).toBeVisible();
    await link.click();
    await waitForFeedReady(page);

    await expect(page).toHaveURL(new RegExp(`channel=pg-tax.*#post-${targetPostId}`));
    await expect(page.locator(`#post-${targetPostId}`)).toHaveClass(/post-card--targeted/);
  });

  test('merges adjacent identical text links into one anchor', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'split-anchor-host';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999993,
        date: new Date().toISOString(),
        text: 'split anchor',
        text_html: '<p><a href="https://example.com/story" target="_blank" rel="noopener noreferrer">Шохин</a><a href="https://example.com/story" target="_blank" rel="noopener noreferrer">подчеркнул</a>, что речь идет о важном вопросе.</p>',
        photos: [],
        tg_url: 'https://t.me/example/999993',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    const anchors = page.locator('#split-anchor-host .post-card__text a');
    await expect(anchors).toHaveCount(1);
    await expect(anchors.first()).toContainText('Шохин подчеркнул');
    await expect(anchors.first()).toHaveAttribute('href', 'https://example.com/story');
  });

  test('renders telegram-style emoji markup as matching unicode emoji in post text', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'emoji-host';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999994,
        date: new Date().toISOString(),
        text: 'emoji markup',
        text_html: '<p><a href="https://example.com/story"><img class="emoji" alt="🔥" src="/emoji/fire.png">Важная ссылка</a> <tg-emoji emoji-id="1">⚡️</tg-emoji> <span class="tg-emoji" title="📌"></span> новость</p>',
        photos: [],
        tg_url: 'https://t.me/example/999994',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    const text = page.locator('#emoji-host .post-card__text');
    await expect(text).toContainText('🔥Важная ссылка ⚡️ 📌 новость');

    const anchor = page.locator('#emoji-host .post-card__text a').first();
    await expect(anchor).toContainText('🔥Важная ссылка');
    await expect(anchor).toHaveAttribute('href', 'https://example.com/story');
  });

  test('renders Telegram text formatting and removes unsafe markup', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'formatted-post-host';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999995,
        date: new Date().toISOString(),
        text: 'formatted post',
        text_html: [
          'Plain <strong onclick="window.formattingAttack = true">bold</strong> ',
          '<em>italic</em> <u>underlined</u> <s>struck</s> ',
          '<code>inline code</code>',
          '<blockquote>Quoted text</blockquote>',
          '<span class="post-text-spoiler" style="color:red">Spoiler</span>',
          '<script>window.formattingAttack = true</script>',
          '<a href="javascript:window.formattingAttack = true">unsafe link</a>',
        ].join(''),
        photos: [],
        tg_url: 'https://t.me/example/999995',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    const text = page.locator('#formatted-post-host .post-card__text');
    await expect(text.locator('strong')).toHaveText('bold');
    await expect(text.locator('strong')).toHaveCSS('font-weight', /^(700|750|800)$/);
    await expect(text.locator('em')).toHaveCSS('font-style', 'italic');
    await expect(text.locator('blockquote')).toHaveText('Quoted text');
    await expect(text.locator('.post-text-spoiler')).toHaveAttribute('tabindex', '0');
    await expect(text.locator('script, [onclick], [style]')).toHaveCount(0);
    await expect(text.locator('a:has-text("unsafe link")')).toHaveCount(0);
    expect(await page.evaluate(() => window.formattingAttack)).toBeUndefined();
  });

  test('deduplicates repeated forwarded album posts in the antitrust feed', async ({ page }) => {
    const postsPath = path.join(process.cwd(), 'docs', 'data', 'channels', 'pg-antitrust', 'posts.json');
    const postsPayload = JSON.parse(fs.readFileSync(postsPath, 'utf8'));
    const sourcePost = postsPayload.posts[0];
    expect(sourcePost).toBeTruthy();

    const duplicatedPayload = {
      ...postsPayload,
      pagination: {
        ...(postsPayload.pagination || {}),
        page: 1,
        total_pages: 1,
        total_posts: (postsPayload.posts || []).length + 4,
      },
      posts: [
        { ...sourcePost, id: 990001, tg_url: 'https://t.me/PgAntitrust/990001' },
        { ...sourcePost, id: 990002, tg_url: 'https://t.me/PgAntitrust/990002' },
        { ...sourcePost, id: 990003, tg_url: 'https://t.me/PgAntitrust/990003' },
        { ...sourcePost, id: 990004, tg_url: 'https://t.me/PgAntitrust/990004' },
        { ...sourcePost, id: 990005, tg_url: 'https://t.me/PgAntitrust/990005' },
        ...(postsPayload.posts || []).slice(1),
      ],
    };

    await page.route('**/data/channels/pg-antitrust/posts.json**', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(duplicatedPayload),
      });
    });

    await page.goto('/?channel=pg-antitrust');
    await waitForFeedReady(page);

    const duplicateInfo = await page.evaluate(() => {
      const cards = Array.from(document.querySelectorAll('.post-card[data-post-id]'));
      const counts = cards.reduce((map, node) => {
        const key = String(node.dataset.postCanonicalKey || node.dataset.postId || '');
        map[key] = (map[key] || 0) + 1;
        return map;
      }, {});
      return Object.entries(counts).filter(([, count]) => count > 1);
    });

    expect(duplicateInfo).toEqual([]);
  });

  test('deduplicates repeated mirrored media posts even when ids and local asset paths differ', async ({ page }) => {
    const postsPath = path.join(process.cwd(), 'docs', 'data', 'channels', 'pgp-official', 'posts.json');
    const postsPayload = JSON.parse(fs.readFileSync(postsPath, 'utf8'));
    const sourcePost = (postsPayload.posts || []).find((post) => Array.isArray(post.photos) && post.photos.length > 0);
    expect(sourcePost).toBeTruthy();

    const remapPhotos = (photos, token) => photos.map((photo, index) => ({
      ...photo,
      thumb_url: `data/channels/pgp-official/media/posts/thumbs/${token}-${index + 1}.jpg`,
      feed_url: `data/channels/pgp-official/media/posts/feed/${token}-${index + 1}.jpg`,
      full_url: `data/channels/pgp-official/media/posts/${token}-${index + 1}.jpg`,
      source_url: `https://cdn4.telesco.pe/file/${token}-${index + 1}.jpg`,
    }));

    const duplicatedPayload = {
      ...postsPayload,
      pagination: {
        ...(postsPayload.pagination || {}),
        page: 1,
        total_pages: 1,
        total_posts: (postsPayload.posts || []).length + 1,
      },
      posts: [
        { ...sourcePost, id: 980001, tg_url: 'https://t.me/pgp_official/980001', photos: remapPhotos(sourcePost.photos, 'dup-a') },
        { ...sourcePost, id: 980002, tg_url: 'https://t.me/pgp_official/980002', photos: remapPhotos(sourcePost.photos, 'dup-b') },
        ...(postsPayload.posts || []).slice(1),
      ],
    };

    await page.route('**/data/channels/pgp-official/posts.json**', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(duplicatedPayload),
      });
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const renderedIds = await page.locator('.post-card[data-post-id="980001"], .post-card[data-post-id="980002"]').evaluateAll(
      (nodes) => nodes.map((node) => node.getAttribute('data-post-id')),
    );
    expect(renderedIds).toHaveLength(1);
  });

  test('uses natural image height for single images and the tallest item in each gallery row', async ({ page }) => {
    const postsPath = path.join(process.cwd(), 'docs', 'data', 'channels', 'pgp-official', 'posts.json');
    const postsPayload = JSON.parse(fs.readFileSync(postsPath, 'utf8'));
    const buildPhoto = (fileName, width, height) => ({
      thumb_url: `assets/${fileName}`,
      feed_url: `assets/${fileName}`,
      full_url: `assets/${fileName}`,
      thumb_width: width,
      thumb_height: height,
      feed_width: width,
      feed_height: height,
      full_width: width,
      full_height: height,
      source_width: width,
      source_height: height,
    });
    const singleImagePost = {
      id: 990003,
      date: new Date().toISOString(),
      text: 'Single image fixture',
      text_html: 'Single image fixture',
      views: 0,
      comments_count: 0,
      photos: [buildPhoto('app-icon-512.png', 512, 512)],
      videos: [],
      video_url: null,
      tg_url: 'https://t.me/pgp_official/990003',
    };
    const matchedRowsPost = {
      ...singleImagePost,
      id: 990004,
      text: 'Matched gallery rows fixture',
      text_html: 'Matched gallery rows fixture',
      photos: [
        buildPhoto('app-icon-512.png', 512, 512),
        buildPhoto('app-icon-192.png', 192, 192),
        buildPhoto('app-icon-512.png', 512, 512),
        buildPhoto('app-icon-192.png', 192, 192),
        buildPhoto('app-icon-512.png', 512, 512),
      ],
      tg_url: 'https://t.me/pgp_official/990004',
    };
    const mismatchedRowPost = {
      ...singleImagePost,
      id: 990005,
      text: 'Mismatched gallery row fixture',
      text_html: 'Mismatched gallery row fixture',
      photos: [
        buildPhoto('app-icon-512.png', 320, 180),
        buildPhoto('app-icon-192.png', 192, 192),
      ],
      tg_url: 'https://t.me/pgp_official/990005',
    };
    const fixturePayload = {
      ...postsPayload,
      posts: [singleImagePost, matchedRowsPost, mismatchedRowPost, ...(postsPayload.posts || [])],
    };

    await page.route('**/data/channels/pgp-official/posts.json**', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(fixturePayload),
      });
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    const singleMedia = page.locator('.post-card[data-post-id="990003"] .post-card__media--natural-single-image');
    await expect(singleMedia).toBeVisible();
    const singleLayout = await singleMedia.evaluate((root) => {
      const trigger = root.querySelector('.media-trigger');
      const image = trigger.querySelector('img');
      const rootRect = root.getBoundingClientRect();
      const triggerRect = trigger.getBoundingClientRect();
      const imageRect = image.getBoundingClientRect();
      return {
        rootHeight: rootRect.height,
        triggerHeight: triggerRect.height,
        imageHeight: imageRect.height,
        triggerBackground: getComputedStyle(trigger).backgroundColor,
      };
    });
    expect(Math.abs(singleLayout.rootHeight - singleLayout.imageHeight)).toBeLessThanOrEqual(1);
    expect(Math.abs(singleLayout.triggerHeight - singleLayout.imageHeight)).toBeLessThanOrEqual(1);
    expect(singleLayout.triggerBackground).not.toBe('rgba(0, 0, 0, 0)');

    const gallery = page.locator('.post-card[data-post-id="990004"] .post-card__media--gallery');
    await expect(gallery).toBeVisible();
    await expect(gallery.locator('img').first()).toHaveAttribute(
      'sizes',
      '(max-width: 860px) calc(100vw - 44px), 520px',
    );
    const layout = await gallery.evaluate((root) => ({
      triggerSizes: [...root.querySelectorAll('.media-trigger')].map((trigger) => {
        const image = trigger.querySelector('img');
        const triggerRect = trigger.getBoundingClientRect();
        const imageRect = image.getBoundingClientRect();
        return {
          triggerWidth: triggerRect.width,
          triggerHeight: triggerRect.height,
          imageWidth: imageRect.width,
          imageHeight: imageRect.height,
          imageBackground: getComputedStyle(image).backgroundColor,
        };
      }),
    }));

    expect(layout.triggerSizes).toHaveLength(5);
    expect(layout.triggerSizes.every((item) => (
      Math.abs(item.triggerWidth - item.imageWidth) <= 1
      && Math.abs(item.triggerHeight - item.imageHeight) <= 1
      && Math.abs(item.imageWidth - item.imageHeight) <= 1
      && item.imageBackground === 'rgba(0, 0, 0, 0)'
    ))).toBe(true);

    const mismatchedGallery = page.locator('.post-card[data-post-id="990005"] .post-card__media--gallery');
    const mismatchedTriggers = mismatchedGallery.locator(':scope > .media-trigger');
    await expect(mismatchedTriggers.nth(0)).not.toHaveClass(/media-trigger--natural-image/);
    await expect(mismatchedTriggers.nth(1)).toHaveClass(/media-trigger--natural-image/);
    const mismatchedLayout = await mismatchedGallery.evaluate((root) => (
      [...root.querySelectorAll(':scope > .media-trigger')].map((trigger) => {
        const image = trigger.querySelector('img');
        const triggerRect = trigger.getBoundingClientRect();
        const imageRect = image.getBoundingClientRect();
        return {
          topGap: imageRect.top - triggerRect.top,
          bottomGap: triggerRect.bottom - imageRect.bottom,
          triggerBackground: getComputedStyle(trigger).backgroundColor,
        };
      })
    ));
    expect(mismatchedLayout[0].topGap).toBeGreaterThan(0);
    expect(mismatchedLayout[0].bottomGap).toBeGreaterThan(0);
    expect(Math.abs(mismatchedLayout[1].topGap)).toBeLessThanOrEqual(1);
    expect(Math.abs(mismatchedLayout[1].bottomGap)).toBeLessThanOrEqual(1);
    expect(mismatchedLayout[1].triggerBackground).toBe('rgba(0, 0, 0, 0)');
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

    await page.route('**/data/channels/investment-law/posts.json**', async (route) => {
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

  test('renders external link preview card at the end of post content', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'link-preview-host-desktop';
      document.body.appendChild(host);
      const previewUrl = `${window.location.origin}/assets/channel-avatar.jpg`;
      const card = window.renderPostCard({
        id: 999990,
        date: new Date().toISOString(),
        text: 'Ссылка на видео',
        text_html: '<p>Ссылка на видео</p>',
        photos: [],
        tg_url: 'https://t.me/example/999990',
        comments_count: 0,
        link_preview: {
          href: 'https://rutube.ru/video/example',
          title: 'Видео обзор',
          description: 'Краткое описание видео',
          site_name: 'Rutube',
          host: 'rutube.ru',
          is_video: true,
          image: {
            thumb_url: previewUrl,
            feed_url: previewUrl,
            full_url: previewUrl,
          },
        },
      });
      host.appendChild(card);
    });

    await expect(page.locator('#link-preview-host-desktop .post-card__text')).toContainText('Ссылка на видео');
    await expect(page.locator('#link-preview-host-desktop .post-card__link-preview')).toBeVisible();
    await expect(page.locator('#link-preview-host-desktop .post-card__link-preview-title')).toContainText('Видео обзор');
    await expect(page.locator('#link-preview-host-desktop .post-card__link-preview-badge')).toContainText('Видео');

    const [mediaBox, copyBox] = await Promise.all([
      page.locator('#link-preview-host-desktop .post-card__link-preview-media').boundingBox(),
      page.locator('#link-preview-host-desktop .post-card__link-preview-copy').boundingBox(),
    ]);
    expect(mediaBox).toBeTruthy();
    expect(copyBox).toBeTruthy();
    expect(copyBox.x).toBeGreaterThanOrEqual(mediaBox.x + mediaBox.width + 8);
  });

  test('renders telegram poll results as a read-only block', async ({ page }) => {
    await page.goto('/?channel=pg-antitrust');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'poll-preview-host';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999982,
        date: new Date().toISOString(),
        text: 'Тестовый пост с итогами опроса',
        text_html: 'Тестовый пост с итогами опроса',
        photos: [],
        videos: [],
        video_url: null,
        poll: {
          type: 'Anonymous Poll',
          question: 'Какой формат полезнее?',
          total_voters: 128,
          options: [
            { text: 'Короткий обзор', percent: 64 },
            { text: 'Подробный разбор', percent: 36 },
          ],
        },
        polls: [{
          type: 'Anonymous Poll',
          question: 'Признали бы нарушение?',
          total_voters: 95,
          options: [
            { text: 'Да', percent: 71.6 },
            { text: 'Нет', percent: 28.4 },
          ],
        }],
        tg_url: 'https://t.me/PgAntitrust/999982',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    const poll = page.locator('#poll-preview-host .post-card__poll');
    await expect(poll).toHaveCount(2);
    await expect(poll.first().locator('.post-card__poll-question')).toContainText('Какой формат полезнее?');
    await expect(poll.nth(1).locator('.post-card__poll-question')).toContainText('Признали бы нарушение?');
    await expect(poll.first().locator('.post-card__poll-option')).toHaveCount(2);
    await expect(poll.first().locator('.post-card__poll-option-percent').first()).toContainText('64%');
    await expect(poll.locator('button')).toHaveCount(0);
  });

  test('does not render link preview card when post already has physical media', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'link-preview-media-guard-host-desktop';
      document.body.appendChild(host);
      const previewUrl = `${window.location.origin}/assets/channel-avatar.jpg`;
      const card = window.renderPostCard({
        id: 999989,
        date: new Date().toISOString(),
        text: 'Пост с картинкой',
        text_html: '<p>Пост с картинкой</p>',
        photos: [{
          thumb_url: previewUrl,
          feed_url: previewUrl,
          full_url: previewUrl,
        }],
        tg_url: 'https://t.me/example/999989',
        comments_count: 0,
        link_preview: {
          href: 'https://rutube.ru/video/example',
          title: 'Видео обзор',
          description: 'Краткое описание видео',
          site_name: 'Rutube',
          host: 'rutube.ru',
          is_video: true,
          image: {
            thumb_url: previewUrl,
            feed_url: previewUrl,
            full_url: previewUrl,
          },
        },
      });
      host.appendChild(card);
    });

    await expect(page.locator('#link-preview-media-guard-host-desktop .post-card__media')).toBeVisible();
    await expect(page.locator('#link-preview-media-guard-host-desktop .post-card__link-preview')).toHaveCount(0);
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

  test('keeps square single-video posts round even if explicit video-note flag is missing', async ({ page }) => {
    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);

    await page.evaluate(() => {
      const host = document.createElement('div');
      host.id = 'round-video-square-hint-host-desktop';
      document.body.appendChild(host);
      const card = window.renderPostCard({
        id: 999994,
        date: new Date().toISOString(),
        text: 'Короткая подпись к кружку',
        text_html: '<p>Короткая подпись к кружку</p>',
        photos: [],
        video_url: 'data:video/mp4;base64,AAAA',
        video_width: 640,
        video_height: 640,
        tg_url: 'https://t.me/example/999994',
        comments_count: 0,
      });
      host.appendChild(card);
    });

    await expect(page.locator('#round-video-square-hint-host-desktop .media-video-note__placeholder')).toBeVisible();
    await expect(page.locator('#round-video-square-hint-host-desktop .post-card__media video')).toHaveCount(0);
  });

  test('opens an actual mirrored round-video viewer on desktop without fallback', async ({ page }) => {
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

  test('renders a paged mirrored round-video post as a round preview after deep-link loading', async ({ page }) => {
    const { channelKey, postId } = findMirroredRoundVideoPost({ requirePaged: true });
    await page.goto(`/?channel=${encodeURIComponent(channelKey)}#post-${postId}`);
    await waitForFeedReady(page);

    const card = page.locator(`#post-${postId}`);
    await expect(card).toBeVisible();
    await expect(card.locator('.media-video-note img, .media-video-note__placeholder')).toBeVisible();
    await expect(card.locator('.post-card__media video')).toHaveCount(0);
  });
});

test.describe('Desktop PWA smoke', () => {
  test.use({ serviceWorkers: 'allow' });

  test('reloads the app shell when an updated service worker takes control', async ({ page }) => {
    let mainFrameNavigations = 0;
    page.on('framenavigated', (frame) => {
      if (frame === page.mainFrame()) mainFrameNavigations += 1;
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);
    const navigationsBeforeControllerChange = mainFrameNavigations;

    await page.evaluate(() => {
      navigator.serviceWorker.dispatchEvent(new Event('controllerchange'));
    });

    await expect.poll(() => mainFrameNavigations).toBeGreaterThan(navigationsBeforeControllerChange);
    await waitForFeedReady(page);
    await expect(page.locator('.contact-bar__item')).toHaveCount(4);
    await expect(page.locator('.contact-bar__item--ecosystem')).toBeVisible();
  });

  test('refreshes the active feed on startup and when the app returns from the background', async ({ page }) => {
    const feedRequests = [];
    page.on('request', (request) => {
      const url = new URL(request.url());
      if (url.pathname.endsWith('/data/channels/pgp-official/posts.json')) {
        feedRequests.push(url.toString());
      }
    });

    await page.goto('/?channel=pgp-official');
    await waitForFeedReady(page);
    await expect.poll(() => feedRequests.length).toBeGreaterThanOrEqual(1);
    expect(new URL(feedRequests[0]).searchParams.has('t')).toBe(true);

    const requestCountBeforeResume = feedRequests.length;
    await page.evaluate(() => {
      document.dispatchEvent(new Event('freeze'));
      document.dispatchEvent(new Event('resume'));
      window.dispatchEvent(new PageTransitionEvent('pagehide', { persisted: true }));
      window.dispatchEvent(new PageTransitionEvent('pageshow', { persisted: true }));
      window.dispatchEvent(new Event('online'));
    });

    await expect.poll(() => feedRequests.length).toBe(requestCountBeforeResume + 1);
    expect(new URL(feedRequests.at(-1)).searchParams.has('t')).toBe(true);
  });

  test('loads more posts when the installed-app service worker controls the page', async ({ page }) => {
    await page.goto('/?channel=pg-tax');
    await waitForFeedReady(page);

    await page.evaluate(async () => {
      if (!('serviceWorker' in navigator)) return;
      const registration = await navigator.serviceWorker.register('./sw.js');
      await registration.update();
      await navigator.serviceWorker.ready;
    });

    await page.reload();
    await waitForFeedReady(page);
    await expect.poll(() => page.evaluate(() => Boolean(navigator.serviceWorker?.controller))).toBe(true);

    const initialCount = await page.locator('.post-card').count();
    const loaded = await clickLoadMoreIfVisible(page);
    expect(loaded).toBe(true);

    const afterFirstClick = await page.locator('.post-card').count();
    expect(afterFirstClick).toBeGreaterThan(initialCount);

    const loadedAgain = await clickLoadMoreIfVisible(page);
    expect(loadedAgain).toBe(true);

    const afterSecondClick = await page.locator('.post-card').count();
    expect(afterSecondClick).toBeGreaterThan(afterFirstClick);
  });
});
