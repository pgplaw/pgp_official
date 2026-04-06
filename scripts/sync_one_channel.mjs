import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, '..');
const configPath = path.join(rootDir, 'config', 'channels.json');
const channelsDataDir = path.join(rootDir, 'docs', 'data', 'channels');
const channelPagesDir = path.join(rootDir, 'docs', 'channels');
const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
const channelTimeoutMs = Number(process.env.TG_SYNC_CHANNEL_TIMEOUT_MS || 150000);

function readArg(flag) {
  const index = process.argv.indexOf(flag);
  if (index === -1) return null;
  return process.argv[index + 1] || null;
}

function collectTreeStats(baseDir, accumulator) {
  if (!fs.existsSync(baseDir)) {
    return;
  }

  for (const entry of fs.readdirSync(baseDir, { withFileTypes: true })) {
    const entryPath = path.join(baseDir, entry.name);
    if (entry.isDirectory()) {
      collectTreeStats(entryPath, accumulator);
      continue;
    }

    if (!entry.isFile()) {
      continue;
    }

    const stats = fs.statSync(entryPath);
    const relativePath = entryPath.replaceAll('\\', '/');
    const extension = path.extname(entry.name).toLowerCase();

    accumulator.total_bytes += stats.size;
    accumulator.file_count += 1;

    if (relativePath.includes('/media/')) {
      accumulator.media_bytes += stats.size;
      accumulator.media_files += 1;
    }
    if (extension === '.json') {
      accumulator.json_files += 1;
      if (relativePath.includes('/pages/')) {
        accumulator.page_json_files += 1;
      }
      if (relativePath.includes('/comments/')) {
        accumulator.comment_json_files += 1;
      }
    }
  }
}

function collectChannelArchiveStats(channelKey) {
  const summary = {
    total_bytes: 0,
    file_count: 0,
    media_bytes: 0,
    media_files: 0,
    json_files: 0,
    page_json_files: 0,
    comment_json_files: 0,
  };

  collectTreeStats(path.join(channelsDataDir, channelKey), summary);
  collectTreeStats(path.join(channelPagesDir, channelKey), summary);

  return summary;
}

function buildArchiveDelta(before, after) {
  return {
    total_bytes: after.total_bytes - before.total_bytes,
    file_count: after.file_count - before.file_count,
    media_bytes: after.media_bytes - before.media_bytes,
    media_files: after.media_files - before.media_files,
    json_files: after.json_files - before.json_files,
    page_json_files: after.page_json_files - before.page_json_files,
    comment_json_files: after.comment_json_files - before.comment_json_files,
  };
}

function buildChannelEnv(channel) {
  return {
    ...process.env,
    TG_CHANNEL_KEY: channel.key,
    TG_CHANNEL_USERNAME: channel.channel_username,
    TG_CHANNEL_TITLE: channel.channel_title,
    TG_SITE_NAME: channel.channel_title,
    TG_SITE_DESCRIPTION: channel.site_description,
    TG_LANGUAGE: channel.language || config.language,
    TG_ACCENT_COLOR: channel.accent_color || config.accent_color,
    TG_BACKGROUND_COLOR: channel.background_color || config.background_color,
    TG_AVATAR_PATH: channel.avatar_path || config.avatar_path,
    TG_MESSAGES_LIMIT: String(channel.messages_limit || config.messages_limit || 250),
    TG_RECENT_POSTS_MONTHS: String(channel.recent_posts_months || config.recent_posts_months || 3),
    TG_COMMENTS_POSTS_LIMIT: String(channel.comments_posts_limit || config.comments_posts_limit || 40),
    TG_COMMENTS_MAX_AGE_DAYS: String(channel.comments_max_age_days || config.comments_max_age_days || 7),
  };
}

function writeResult(resultPath, payload) {
  if (!resultPath) return;
  fs.mkdirSync(path.dirname(resultPath), { recursive: true });
  fs.writeFileSync(resultPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

const channelKey = readArg('--channel') || process.argv[2] || null;
const resultPath = readArg('--result-path');

if (!channelKey) {
  console.error('Usage: node scripts/sync_one_channel.mjs --channel <key> [--result-path <file>]');
  process.exit(1);
}

const channel = config.channels.find((entry) => entry.key === channelKey);
if (!channel) {
  console.error(`Unknown channel key: ${channelKey}`);
  process.exit(1);
}

console.log(`\n=== Syncing ${channel.channel_title} (@${channel.channel_username}) ===`);
const archiveBefore = collectChannelArchiveStats(channel.key);
const startedAt = Date.now();
const result = spawnSync('python', ['scripts/sync_channel.py'], {
  cwd: rootDir,
  env: buildChannelEnv(channel),
  stdio: 'inherit',
  timeout: channelTimeoutMs,
  killSignal: 'SIGKILL',
});
const durationMs = Date.now() - startedAt;
const archiveAfter = collectChannelArchiveStats(channel.key);
const archiveDelta = buildArchiveDelta(archiveBefore, archiveAfter);

console.log(
  `Archive stats for ${channel.key}: total ${(archiveAfter.total_bytes / (1024 * 1024)).toFixed(1)} MB `
  + `(${archiveDelta.total_bytes >= 0 ? '+' : ''}${(archiveDelta.total_bytes / (1024 * 1024)).toFixed(1)} MB), `
  + `media ${(archiveAfter.media_bytes / (1024 * 1024)).toFixed(1)} MB `
  + `(${archiveDelta.media_bytes >= 0 ? '+' : ''}${(archiveDelta.media_bytes / (1024 * 1024)).toFixed(1)} MB), `
  + `pages ${archiveAfter.page_json_files}`,
);

const payload = {
  key: channel.key,
  channel_username: channel.channel_username,
  channel_title: channel.channel_title,
  status: result.error || result.status !== 0 ? 'failed' : 'success',
  exit_code: typeof result.status === 'number' ? result.status : null,
  signal: result.signal || null,
  duration_ms: durationMs,
  error: result.error ? result.error.message : null,
  archive_before: archiveBefore,
  archive_after: archiveAfter,
  archive_delta: archiveDelta,
};

writeResult(resultPath, payload);

if (payload.status === 'failed') {
  if (result.error) {
    if (result.error.code === 'ETIMEDOUT') {
      console.error(`Channel ${channel.key} timed out after ${channelTimeoutMs} ms.`);
    } else {
      console.error(`Channel ${channel.key} failed to start: ${result.error.message}`);
    }
  } else if (result.signal) {
    console.error(`Channel ${channel.key} was terminated with signal ${result.signal}.`);
  }
  process.exit(1);
}

process.exit(0);
