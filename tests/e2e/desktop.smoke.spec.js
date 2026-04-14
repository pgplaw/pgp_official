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

  test('opens actual bankrotstvo round-video viewer on desktop without fallback', async ({ page }) => {
    await page.goto('/?channel=bankrotstvo-mustknow#post-444');
    await waitForFeedReady(page);

    const card = page.locator('#post-444');
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

  test('renders paged pg-tax round-video posts as round previews after deep-link loading', async ({ page }) => {
    await page.goto('/?channel=pg-tax#post-2558');
    await waitForFeedReady(page);

    const card = page.locator('#post-2558');
    await expect(card).toBeVisible();
    await expect(card.locator('.media-video-note img, .media-video-note__placeholder')).toBeVisible();
    await expect(card.locator('.post-card__media video')).toHaveCount(0);
  });
});
