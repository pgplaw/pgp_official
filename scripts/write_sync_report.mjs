import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, '..');
const configPath = path.join(rootDir, 'config', 'channels.json');
const syncReportPath = path.join(rootDir, 'docs', 'data', 'sync-report.json');
const resultsDir = path.resolve(process.argv[2] || path.join(rootDir, '.sync-results'));
const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
const channelTimeoutMs = Number(process.env.TG_SYNC_CHANNEL_TIMEOUT_MS || 150000);

function loadResult(channelKey) {
  const resultPath = path.join(resultsDir, `${channelKey}.json`);
  if (!fs.existsSync(resultPath)) {
    return {
      key: channelKey,
      status: 'failed',
      exit_code: null,
      signal: null,
      duration_ms: 0,
      error: 'Missing sync result',
      archive_before: {
        total_bytes: 0,
        media_bytes: 0,
        file_count: 0,
        media_files: 0,
        json_files: 0,
        page_json_files: 0,
        comment_json_files: 0,
      },
      archive_after: {
        total_bytes: 0,
        media_bytes: 0,
        file_count: 0,
        media_files: 0,
        json_files: 0,
        page_json_files: 0,
        comment_json_files: 0,
      },
      archive_delta: {
        total_bytes: 0,
        media_bytes: 0,
        file_count: 0,
        media_files: 0,
        json_files: 0,
        page_json_files: 0,
        comment_json_files: 0,
      },
    };
  }

  return JSON.parse(fs.readFileSync(resultPath, 'utf8'));
}

const channelResults = config.channels.map((channel) => {
  const result = loadResult(channel.key);
  return {
    key: channel.key,
    channel_username: result.channel_username || channel.channel_username,
    channel_title: result.channel_title || channel.channel_title,
    status: result.status || 'failed',
    exit_code: typeof result.exit_code === 'number' ? result.exit_code : null,
    signal: result.signal || null,
    duration_ms: result.duration_ms || 0,
    error: result.error || null,
    archive_before: result.archive_before || {},
    archive_after: result.archive_after || {},
    archive_delta: result.archive_delta || {},
  };
});

const failures = channelResults.filter((result) => result.status !== 'success').map((result) => result.key);
const report = {
  generated_at: new Date().toISOString(),
  success_count: channelResults.filter((result) => result.status === 'success').length,
  failure_count: failures.length,
  failure_channels: failures,
  channel_timeout_ms: channelTimeoutMs,
  channels: channelResults,
  archive_totals: channelResults.reduce((accumulator, result) => {
    const archive = result.archive_after || {};
    accumulator.total_bytes += archive.total_bytes || 0;
    accumulator.media_bytes += archive.media_bytes || 0;
    accumulator.file_count += archive.file_count || 0;
    accumulator.media_files += archive.media_files || 0;
    accumulator.json_files += archive.json_files || 0;
    accumulator.page_json_files += archive.page_json_files || 0;
    accumulator.comment_json_files += archive.comment_json_files || 0;
    return accumulator;
  }, {
    total_bytes: 0,
    media_bytes: 0,
    file_count: 0,
    media_files: 0,
    json_files: 0,
    page_json_files: 0,
    comment_json_files: 0,
  }),
};

fs.mkdirSync(path.dirname(syncReportPath), { recursive: true });
fs.writeFileSync(syncReportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
console.log(`Wrote sync report to ${path.relative(rootDir, syncReportPath)}.`);
