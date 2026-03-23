import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, '..');
const configPath = path.join(rootDir, 'config', 'channels.json');
const syncReportPath = path.join(rootDir, 'docs', 'data', 'sync-report.json');
const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
const channelTimeoutMs = Number(process.env.TG_SYNC_CHANNEL_TIMEOUT_MS || 150000);

const failures = [];
const channelResults = [];
let successCount = 0;

for (const channel of config.channels) {
  const env = {
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

  console.log(`\n=== Syncing ${channel.channel_title} (@${channel.channel_username}) ===`);
  const startedAt = Date.now();
  const result = spawnSync('python', ['scripts/sync_channel.py'], {
    cwd: rootDir,
    env,
    stdio: 'inherit',
    timeout: channelTimeoutMs,
    killSignal: 'SIGKILL',
  });
  const durationMs = Date.now() - startedAt;

  if (result.error || result.status !== 0) {
    failures.push(channel.key);
    channelResults.push({
      key: channel.key,
      channel_username: channel.channel_username,
      channel_title: channel.channel_title,
      status: 'failed',
      exit_code: typeof result.status === 'number' ? result.status : null,
      signal: result.signal || null,
      duration_ms: durationMs,
      error: result.error ? result.error.message : null,
    });
    if (result.error) {
      if (result.error.code === 'ETIMEDOUT') {
        console.error(`Channel ${channel.key} timed out after ${channelTimeoutMs} ms.`);
      } else {
        console.error(`Channel ${channel.key} failed to start: ${result.error.message}`);
      }
    } else if (result.signal) {
      console.error(`Channel ${channel.key} was terminated with signal ${result.signal}.`);
    }
    continue;
  }

  successCount += 1;
  channelResults.push({
    key: channel.key,
    channel_username: channel.channel_username,
    channel_title: channel.channel_title,
    status: 'success',
    exit_code: 0,
    signal: null,
    duration_ms: durationMs,
    error: null,
  });
}

const report = {
  generated_at: new Date().toISOString(),
  success_count: successCount,
  failure_count: failures.length,
  failure_channels: failures,
  channel_timeout_ms: channelTimeoutMs,
  channels: channelResults,
};

fs.mkdirSync(path.dirname(syncReportPath), { recursive: true });
fs.writeFileSync(syncReportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
console.log(`Wrote sync report to ${path.relative(rootDir, syncReportPath)}.`);

if (failures.length) {
  console.error(`Failed channels: ${failures.join(', ')}`);
  process.exit(1);
}
