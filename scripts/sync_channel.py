from __future__ import annotations

import calendar
import asyncio
import hashlib
import html as html_lib
import io
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "channel.json"
DOCS_DIR = ROOT / "docs"
DATA_DIR = DOCS_DIR / "data"
CHANNEL_KEY = (os.environ.get("TG_CHANNEL_KEY") or "").strip()
CHANNELS_DATA_DIR = DATA_DIR / "channels"
CHANNEL_DATA_DIR = CHANNELS_DATA_DIR / CHANNEL_KEY if CHANNEL_KEY else DATA_DIR
POSTS_PATH = CHANNEL_DATA_DIR / "posts.json"
COMMENTS_DIR = CHANNEL_DATA_DIR / "comments"
PAGES_DIR = CHANNEL_DATA_DIR / "pages"
POST_DETAILS_DIR = CHANNEL_DATA_DIR / "posts"
POSTS_MEDIA_DIR = CHANNEL_DATA_DIR / "media" / "posts"
POSTS_THUMBS_DIR = POSTS_MEDIA_DIR / "thumbs"
POSTS_FEED_DIR = POSTS_MEDIA_DIR / "feed"
POSTS_VIDEOS_DIR = POSTS_MEDIA_DIR / "videos"
POSTS_VIDEO_POSTERS_DIR = POSTS_MEDIA_DIR / "video-posters"
POSTS_ATTACHED_VIDEOS_DIR = POSTS_MEDIA_DIR / "attached-videos"
POSTS_ATTACHED_VIDEO_POSTERS_DIR = POSTS_MEDIA_DIR / "attached-video-posters"
POSTS_LINK_PREVIEWS_DIR = POSTS_MEDIA_DIR / "link-previews"
CHANNEL_MEDIA_DIR = CHANNEL_DATA_DIR / "media"
CHANNEL_AVATAR_PATH = CHANNEL_MEDIA_DIR / "channel-avatar.jpg"
SUPERRES_MODEL_DIR = ROOT / "ops" / "models"
EDSR_X2_MODEL_PATH = SUPERRES_MODEL_DIR / "EDSR_x2.pb"
FSRCNN_X2_MODEL_PATH = SUPERRES_MODEL_DIR / "FSRCNN_x2.pb"
POST_PAGES_DIR = DOCS_DIR / "channels" / CHANNEL_KEY / "posts" if CHANNEL_KEY else DOCS_DIR / "posts"
MANIFEST_PATH = DOCS_DIR / "manifest.webmanifest"
FEED_PAGE_SIZE = 16
IMAGE_VARIANT_VERSION = "v8"
VIDEO_VARIANT_VERSION = "v1"
STALE_MEDIA_RETENTION_DAYS = 3
STALE_MEDIA_RETENTION_SECONDS = STALE_MEDIA_RETENTION_DAYS * 24 * 60 * 60
LEGACY_VARIANT_RETENTION_DAYS = 1
LEGACY_VARIANT_RETENTION_SECONDS = LEGACY_VARIANT_RETENTION_DAYS * 24 * 60 * 60

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
FETCH_RETRY_DELAYS = (1.0, 2.5, 5.0)
FAST_EXTERNAL_RETRY_DELAYS: tuple[float, ...] = ()
EXTERNAL_PAGE_TIMEOUT_SECONDS = 5
EXTERNAL_IMAGE_TIMEOUT_SECONDS = 6
LINK_PREVIEW_PAGE_TIMEOUT_SECONDS = 5
LINK_PREVIEW_IMAGE_TIMEOUT_SECONDS = 6
MAX_LINK_PREVIEW_CANDIDATES = 2
DIRECT_POST_PROBE_FETCH_RETRY_DELAYS = (1.0, 2.0)
DIRECT_POST_PROBE_TIMEOUT_SECONDS = 12
DIRECT_POST_PROBE_STALE_MAX_IDS = 20
DIRECT_POST_PROBE_FOLLOWUP_MAX_IDS = 4
DIRECT_POST_PROBE_MAX_CONSECUTIVE_MISSES = 6
MIN_EXTERNAL_OVERRIDE_WIDTH = 1000
MIN_EXTERNAL_OVERRIDE_RATIO_GAIN = 1.15
MAX_EXTERNAL_OVERRIDE_RATIO_DELTA = 0.12
MAX_EXTERNAL_PREVIEW_OVERRIDE_POSTS = 10
MAX_EXTERNAL_LINKS_TO_TRY = 2
ENABLE_EXTERNAL_PREVIEW_OVERRIDE = os.environ.get("TG_ENABLE_EXTERNAL_PREVIEW_OVERRIDE", "").strip().lower() in {"1", "true", "yes", "on"}
ENABLE_SUPERRES = os.environ.get("TG_ENABLE_SUPERRES", "").strip().lower() in {"1", "true", "yes", "on"}
LOW_RES_SINGLE_UPSCALE_THRESHOLD = 1200
LOW_RES_SINGLE_FEED_TARGET = 1800
LOW_RES_SINGLE_FULL_TARGET = 2400
LOW_RES_SINGLE_MAX_UPSCALE_FACTOR = 2.35
EDSR_X2_MODEL_URL = "https://raw.githubusercontent.com/opencv/opencv_contrib/4.x/modules/dnn_superres/samples/EDSR_x2.pb"
FSRCNN_X2_MODEL_URL = "https://raw.githubusercontent.com/Saafke/FSRCNN_Tensorflow/master/models/FSRCNN_x2.pb"
FAILED_EXTERNAL_PREVIEW_HOSTS: set[str] = set()
FAILED_LINK_PREVIEW_HOSTS: set[str] = set()
FAILED_SUPERRES_MODELS: dict[str, str] = {}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("telegram-pages-mirror")


@dataclass
class SiteConfig:
    channel_username: str
    channel_title: str
    site_name: str
    site_description: str
    language: str
    accent_color: str
    background_color: str
    avatar_path: str
    messages_limit: int
    recent_posts_months: int
    comments_posts_limit: int
    comments_max_age_days: int

    @property
    def channel_web_url(self) -> str:
        return f"https://t.me/s/{self.channel_username}"


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text("utf-8"))
    except json.JSONDecodeError:
        return default


def load_existing_feed_posts(existing_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []
    payload = existing_payload or {}

    for post in deepcopy(payload.get("posts") or []):
        if isinstance(post, dict):
            posts.append(post)

    page_payloads: list[tuple[int, Path]] = []
    for path in PAGES_DIR.glob("*.json"):
        try:
            page_number = int(path.stem)
        except ValueError:
            continue
        if page_number < 2:
            continue
        page_payloads.append((page_number, path))

    for _, path in sorted(page_payloads, key=lambda item: item[0]):
        page_payload = load_json(path, {})
        for post in deepcopy(page_payload.get("posts") or []):
            if isinstance(post, dict):
                posts.append(post)

    return dedupe_posts(posts)


def json_without_generated_at(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: json_without_generated_at(item)
            for key, item in value.items()
            if key != "generated_at"
        }
    if isinstance(value, list):
        return [json_without_generated_at(item) for item in value]
    return value


def write_json_if_changed(path: Path, payload: dict[str, Any]) -> bool:
    existing = load_json(path, {})
    comparable_existing = json_without_generated_at(existing)
    comparable_next = json_without_generated_at(payload)
    if comparable_existing == comparable_next:
        log.info("No material changes in %s", path.relative_to(ROOT))
        return False

    payload = deepcopy(payload)
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", "utf-8")
    log.info("Updated %s", path.relative_to(ROOT))
    return True


def write_text_if_changed(path: Path, content: str) -> bool:
    if path.exists() and path.read_text("utf-8") == content:
        log.info("No material changes in %s", path.relative_to(ROOT))
        return False
    path.write_text(content, "utf-8")
    log.info("Updated %s", path.relative_to(ROOT))
    return True


def load_config() -> SiteConfig:
    raw = load_json(CONFIG_PATH, {})
    env = os.environ

    config = SiteConfig(
        channel_username=(env.get("TELEGRAM_CHANNEL") or env.get("TG_CHANNEL_USERNAME") or raw.get("channel_username") or "").strip(),
        channel_title=(env.get("TG_CHANNEL_TITLE") or raw.get("channel_title") or "").strip(),
        site_name=(env.get("TG_SITE_NAME") or raw.get("site_name") or "").strip(),
        site_description=(env.get("TG_SITE_DESCRIPTION") or raw.get("site_description") or "").strip(),
        language=(env.get("TG_LANGUAGE") or raw.get("language") or "ru").strip(),
        accent_color=(env.get("TG_ACCENT_COLOR") or raw.get("accent_color") or "#0f766e").strip(),
        background_color=(env.get("TG_BACKGROUND_COLOR") or raw.get("background_color") or "#f7f3ea").strip(),
        avatar_path=(env.get("TG_AVATAR_PATH") or raw.get("avatar_path") or "").strip(),
        messages_limit=int(env.get("MESSAGES_LIMIT") or env.get("TG_MESSAGES_LIMIT") or raw.get("messages_limit") or 200),
        recent_posts_months=int(env.get("RECENT_POSTS_MONTHS") or env.get("TG_RECENT_POSTS_MONTHS") or raw.get("recent_posts_months") or 3),
        comments_posts_limit=int(env.get("COMMENTS_POSTS_LIMIT") or env.get("TG_COMMENTS_POSTS_LIMIT") or raw.get("comments_posts_limit") or 40),
        comments_max_age_days=int(env.get("COMMENTS_MAX_AGE_DAYS") or env.get("TG_COMMENTS_MAX_AGE_DAYS") or raw.get("comments_max_age_days") or 7),
    )

    if not config.channel_username or config.channel_username == "replace-with-channel-username":
        raise SystemExit("Set TG_CHANNEL_USERNAME or channel_username in config/channel.json before running the sync.")

    if not config.channel_title:
        config.channel_title = config.channel_username
    if not config.site_name:
        config.site_name = config.channel_title
    if not config.site_description:
        config.site_description = f"Static browser mirror for the public Telegram channel @{config.channel_username}."

    return config


def fetch_url(
    url: str,
    *,
    binary: bool = False,
    timeout: int = 30,
    retry_delays: tuple[float, ...] = FETCH_RETRY_DELAYS,
    log_failures: bool = True,
    accept: str | None = None,
    extra_headers: dict[str, str] | None = None,
) -> str | bytes:
    headers = dict(BASE_HEADERS)
    if binary:
        headers["Accept"] = accept or "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
    elif accept:
        headers["Accept"] = accept

    if extra_headers:
        headers.update({key: value for key, value in extra_headers.items() if value})

    last_error: Exception | None = None
    for attempt, delay in enumerate((0.0, *retry_delays), start=1):
        if delay:
            time.sleep(delay)

        try:
            request = Request(url, headers=headers)
            with urlopen(request, timeout=timeout) as response:
                payload = response.read()
                return payload if binary else payload.decode("utf-8", errors="replace")
        except Exception as error:  # pragma: no cover - network/runtime path
            last_error = error
            if log_failures:
                log.warning("Fetch attempt %s failed for %s: %s", attempt, url, error)

    if last_error:
        raise last_error

    raise RuntimeError(f"Unable to fetch {url}")


def fetch_page(
    url: str,
    *,
    timeout: int = 30,
    retry_delays: tuple[float, ...] = FETCH_RETRY_DELAYS,
    log_failures: bool = True,
) -> str:
    return str(fetch_url(url, binary=False, timeout=timeout, retry_delays=retry_delays, log_failures=log_failures))


def fetch_binary(
    url: str,
    *,
    timeout: int = 30,
    retry_delays: tuple[float, ...] = FETCH_RETRY_DELAYS,
    log_failures: bool = True,
    accept: str | None = None,
    extra_headers: dict[str, str] | None = None,
) -> bytes:
    return bytes(
        fetch_url(
            url,
            binary=True,
            timeout=timeout,
            retry_delays=retry_delays,
            log_failures=log_failures,
            accept=accept,
            extra_headers=extra_headers,
        )
    )


def build_telegram_avatar_url(channel_username: str) -> str:
    return f"https://t.me/i/userpic/320/{channel_username}.jpg"


def resolve_avatar_path(config: SiteConfig) -> str:
    if CHANNEL_AVATAR_PATH.exists():
        return CHANNEL_AVATAR_PATH.relative_to(DOCS_DIR).as_posix()

    if config.avatar_path and config.avatar_path != "assets/channel-avatar.jpg":
        return config.avatar_path

    return build_telegram_avatar_url(config.channel_username)


def get_telegram_session_credentials() -> tuple[str, str, str] | None:
    api_id = os.environ.get("TELEGRAM_API_ID")
    api_hash = os.environ.get("TELEGRAM_API_HASH")
    session_string = os.environ.get("TELEGRAM_SESSION_STR")
    if not all((api_id, api_hash, session_string)):
        return None
    return api_id, api_hash, session_string


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def subtract_months(reference: datetime, months: int) -> datetime:
    month_index = reference.month - 1 - months
    year = reference.year + month_index // 12
    month = month_index % 12 + 1
    day = min(reference.day, calendar.monthrange(year, month)[1])
    return reference.replace(year=year, month=month, day=day)


def parse_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def normalize_photo_entry(photo: Any) -> dict[str, Any] | None:
    if not photo:
        return None
    if isinstance(photo, str):
        normalized_url = photo.lstrip("./")
        entry = {
            "thumb_url": normalized_url,
            "feed_url": normalized_url,
            "full_url": normalized_url,
        }
        if re.match(r"^https?://", normalized_url):
            entry["source_url"] = normalized_url
        return entry
    if isinstance(photo, dict):
        thumb_url = (photo.get("thumb_url") or photo.get("thumb") or photo.get("url") or "").lstrip("./")
        full_url = (photo.get("full_url") or photo.get("full") or photo.get("url") or thumb_url).lstrip("./")
        feed_url = (photo.get("feed_url") or photo.get("feed") or full_url or thumb_url).lstrip("./")
        source_url = (
            photo.get("source_url")
            or photo.get("original_url")
            or photo.get("remote_url")
            or (full_url if re.match(r"^https?://", full_url) else "")
            or (thumb_url if re.match(r"^https?://", thumb_url) else "")
        )
        source_url = source_url.strip() if isinstance(source_url, str) else ""
        if not thumb_url and full_url:
            thumb_url = full_url
        if not feed_url and full_url:
            feed_url = full_url
        if thumb_url and full_url:
            entry = {
                "thumb_url": thumb_url,
                "feed_url": feed_url or full_url,
                "full_url": full_url,
            }
            for key in (
                "thumb_width",
                "thumb_height",
                "feed_width",
                "feed_height",
                "full_width",
                "full_height",
                "source_width",
                "source_height",
            ):
                parsed = parse_positive_int(photo.get(key))
                if parsed:
                    entry[key] = parsed
            if source_url and re.match(r"^https?://", source_url):
                entry["source_url"] = source_url
            return entry
    return None


def normalize_video_poster_entry(poster: Any) -> dict[str, Any] | None:
    return normalize_photo_entry(poster)


def normalize_video_entry(video: Any) -> dict[str, Any] | None:
    if not video:
        return None

    if isinstance(video, str):
        normalized_url = video.lstrip("./")
        if not normalized_url:
            return None
        entry: dict[str, Any] = {"url": normalized_url}
        if re.match(r"^https?://", normalized_url):
            entry["source_url"] = normalized_url
        return entry

    if not isinstance(video, dict):
        return None

    normalized_url = str(
        video.get("url")
        or video.get("video_url")
        or video.get("src")
        or video.get("full_url")
        or ""
    ).lstrip("./")
    if not normalized_url:
        return None

    entry = {"url": normalized_url}
    source_url = str(video.get("source_url") or video.get("original_url") or "").strip()
    if not source_url and re.match(r"^https?://", normalized_url, re.IGNORECASE):
        source_url = normalized_url
    if source_url and re.match(r"^https?://", source_url):
        entry["source_url"] = source_url

    poster = normalize_video_poster_entry(video.get("poster") or video.get("video_poster"))
    if poster:
        entry["poster"] = poster

    for key, source_key in (("width", "width"), ("height", "height")):
        parsed = parse_positive_int(video.get(source_key))
        if parsed:
            entry[key] = parsed

    return entry


def normalize_video_entries(videos: Any) -> list[dict[str, Any]]:
    normalized_entries: list[dict[str, Any]] = []
    for video in videos or []:
        entry = normalize_video_entry(video)
        if entry:
            normalized_entries.append(entry)
    return normalized_entries


def read_image_dimensions(path: Path) -> tuple[int | None, int | None]:
    try:
        from PIL import Image

        with Image.open(path) as image:
            image.load()
            return image.width, image.height
    except Exception:
        return None, None


def is_valid_local_image(path: Path | None) -> bool:
    if not path or not path.exists() or not path.is_file():
        return False

    width, height = read_image_dimensions(path)
    return bool(width and height)


def is_local_asset_url(url: str | None) -> bool:
    if not isinstance(url, str):
        return False

    normalized = url.strip()
    if not normalized:
        return False

    return not re.match(r"^(?:[a-z]+:)?//", normalized, re.IGNORECASE) and not normalized.startswith("data:")


def resolve_local_asset_path(url: str | None) -> Path | None:
    if not is_local_asset_url(url):
        return None

    normalized = str(url).strip().lstrip("./").replace("/", os.sep)
    if not normalized:
        return None

    return DOCS_DIR / normalized


def is_valid_local_video(path: Path | None) -> bool:
    if not path or not path.exists() or not path.is_file():
        return False

    try:
        return path.stat().st_size > 0
    except OSError:
        return False


def with_local_variant_dimensions(
    photo: dict[str, Any],
    *,
    thumb_path: Path | None = None,
    feed_path: Path | None = None,
    full_path: Path | None = None,
) -> dict[str, Any]:
    enriched = dict(photo)
    for prefix, path in (("thumb", thumb_path), ("feed", feed_path), ("full", full_path)):
        if not path or not path.exists():
            continue

        width, height = read_image_dimensions(path)
        if width and height:
            enriched[f"{prefix}_width"] = width
            enriched[f"{prefix}_height"] = height

    if "source_width" not in enriched and enriched.get("full_width"):
        enriched["source_width"] = enriched["full_width"]
    if "source_height" not in enriched and enriched.get("full_height"):
        enriched["source_height"] = enriched["full_height"]
    return enriched


def infer_video_extension_from_url(video_url: str | None, default: str = "mp4") -> str:
    if not isinstance(video_url, str):
        return default

    parsed = urlparse(video_url)
    suffix = Path(parsed.path or "").suffix.lower().lstrip(".")
    if suffix in {"mp4", "webm", "mov", "m4v"}:
        return suffix

    return default


def iterate_mp4_top_level_boxes(raw_bytes: bytes) -> list[tuple[str, int, int]]:
    boxes: list[tuple[str, int, int]] = []
    offset = 0
    total_size = len(raw_bytes)

    while offset + 8 <= total_size:
        size = int.from_bytes(raw_bytes[offset : offset + 4], "big")
        box_type = raw_bytes[offset + 4 : offset + 8].decode("ascii", errors="ignore")
        header_size = 8

        if size == 1:
            if offset + 16 > total_size:
                break
            size = int.from_bytes(raw_bytes[offset + 8 : offset + 16], "big")
            header_size = 16

        if size < header_size or offset + size > total_size:
            break

        boxes.append((box_type, offset, size))
        offset += size

        if offset >= total_size:
            break

    return boxes


def has_faststart_layout(raw_bytes: bytes, extension: str | None = "mp4") -> bool:
    normalized_extension = (extension or "mp4").strip().lower()
    if normalized_extension not in {"mp4", "mov", "m4v"}:
        return True

    boxes = iterate_mp4_top_level_boxes(raw_bytes)
    moov_offset = None
    mdat_offset = None
    for box_type, offset, _ in boxes:
        if box_type == "moov" and moov_offset is None:
            moov_offset = offset
        if box_type == "mdat" and mdat_offset is None:
            mdat_offset = offset

    if moov_offset is None or mdat_offset is None:
        return True

    return moov_offset < mdat_offset


def optimize_video_for_streaming(raw_bytes: bytes, extension: str = "mp4") -> bytes:
    normalized_extension = (extension or "mp4").strip().lower()
    if has_faststart_layout(raw_bytes, normalized_extension):
        return raw_bytes

    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        log.warning("FFmpeg is unavailable, cannot remux %s video to faststart layout", normalized_extension)
        return raw_bytes

    input_path: Path | None = None
    output_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{normalized_extension or 'mp4'}") as input_handle:
            input_handle.write(raw_bytes)
            input_path = Path(input_handle.name)

        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{normalized_extension or 'mp4'}") as output_handle:
            output_path = Path(output_handle.name)

        completed = subprocess.run(
            [
                ffmpeg_path,
                "-v",
                "error",
                "-y",
                "-i",
                str(input_path),
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                str(output_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
            check=False,
        )
        if completed.returncode != 0:
            stderr = completed.stderr.decode("utf-8", errors="ignore").strip()
            log.warning("FFmpeg faststart remux failed for %s video: %s", normalized_extension, stderr or completed.returncode)
            return raw_bytes

        optimized_bytes = output_path.read_bytes()
        if not optimized_bytes:
            return raw_bytes

        if not has_faststart_layout(optimized_bytes, normalized_extension):
            log.warning("FFmpeg remux did not produce faststart %s video", normalized_extension)
            return raw_bytes

        if optimized_bytes != raw_bytes:
            log.info("Optimized %s video for progressive playback (faststart)", normalized_extension)

        return optimized_bytes
    except Exception as error:  # pragma: no cover - runtime/video path
        log.warning("Video faststart optimization failed for %s video: %s", normalized_extension, error)
        return raw_bytes
    finally:
        for path in (input_path, output_path):
            if path:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass


def extract_video_poster_bytes(raw_bytes: bytes, extension: str = "mp4") -> bytes | None:
    temp_path: Path | None = None
    try:
        import cv2
        from PIL import Image

        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{extension or 'mp4'}") as handle:
            handle.write(raw_bytes)
            temp_path = Path(handle.name)

        capture = cv2.VideoCapture(str(temp_path))
        if not capture.isOpened():
            capture.release()
            return None

        success, frame = capture.read()
        capture.release()
        if not success or frame is None:
            return None

        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        image = Image.fromarray(rgb_frame)
        buffer = io.BytesIO()
        image.save(buffer, format="JPEG", quality=90, optimize=True, progressive=True)
        return buffer.getvalue()
    except Exception as error:  # pragma: no cover - runtime/video libs path
        log.warning("Round video poster extraction fallback used: %s", error)
        return None
    finally:
        if temp_path:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass


def ensure_superres_model(model_path: Path, model_url: str, label: str) -> Path:
    if model_path.exists():
        return model_path

    if label in FAILED_SUPERRES_MODELS:
        raise RuntimeError(FAILED_SUPERRES_MODELS[label])

    SUPERRES_MODEL_DIR.mkdir(parents=True, exist_ok=True)
    try:
        model_bytes = fetch_binary(model_url, timeout=20)
        model_path.write_bytes(model_bytes)
        log.info("Downloaded %s model to %s", label, model_path.relative_to(ROOT))
        return model_path
    except Exception as error:
        FAILED_SUPERRES_MODELS[label] = f"{label} model unavailable: {error}"
        raise RuntimeError(FAILED_SUPERRES_MODELS[label]) from error


def apply_single_image_super_resolution(image: Any, resampling: Any) -> Any | None:
    try:
        import cv2
        import numpy as np
    except Exception as error:  # pragma: no cover - runtime dependency path
        log.warning("OpenCV super-resolution is unavailable: %s", error)
        return None

    rgb_array = np.array(image.convert("RGB"))
    bgr_array = cv2.cvtColor(rgb_array, cv2.COLOR_RGB2BGR)

    for label, algorithm, scale, model_path, model_url in (
        ("EDSR x2", "edsr", 2, EDSR_X2_MODEL_PATH, EDSR_X2_MODEL_URL),
        ("FSRCNN x2", "fsrcnn", 2, FSRCNN_X2_MODEL_PATH, FSRCNN_X2_MODEL_URL),
    ):
        try:
            resolved_model_path = ensure_superres_model(model_path, model_url, label)
            sr = cv2.dnn_superres.DnnSuperResImpl_create()
            sr.readModel(str(resolved_model_path))
            sr.setModel(algorithm, scale)
            upscaled = sr.upsample(bgr_array)
            upscaled_rgb = cv2.cvtColor(upscaled, cv2.COLOR_BGR2RGB)

            from PIL import Image

            return Image.fromarray(upscaled_rgb)
        except Exception as error:  # pragma: no cover - runtime/model path
            log.warning("%s super-resolution fallback used: %s", label, error)

    return None


def optimize_image_variants(
    raw_bytes: bytes,
    full_path: Path,
    feed_path: Path,
    thumb_path: Path,
    *,
    allow_single_image_upscale: bool = False,
) -> bool:
    changes_detected = False
    try:
        from PIL import Image, ImageFilter, ImageOps

        with Image.open(io.BytesIO(raw_bytes)) as image:
            image = ImageOps.exif_transpose(image)
            image.load()
            resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
            original_format = (image.format or "").upper()
            original_size = image.size

            if image.mode not in ("RGB", "L"):
                background = Image.new("RGB", image.size, "white")
                alpha = image.getchannel("A") if "A" in image.getbands() else None
                background.paste(image.convert("RGBA"), mask=alpha)
                image = background
            elif image.mode == "L":
                image = image.convert("RGB")

            superres_image = None
            if ENABLE_SUPERRES and allow_single_image_upscale and max(original_size) < LOW_RES_SINGLE_UPSCALE_THRESHOLD:
                superres_image = apply_single_image_super_resolution(image, resampling)

            def write_bytes_if_changed(path: Path, content: bytes) -> None:
                nonlocal changes_detected
                path.parent.mkdir(parents=True, exist_ok=True)
                if not path.exists() or path.read_bytes() != content:
                    path.write_bytes(content)
                    changes_detected = True

            def save_variant(
                *,
                path: Path,
                max_size: tuple[int, int],
                quality: int,
                sharpen_radius: float,
                sharpen_percent: int,
                preserve_max_dimension: int,
                preserve_max_bytes: int,
                upscale_longest_side: int | None = None,
            ) -> None:
                should_upscale = bool(
                    allow_single_image_upscale
                    and upscale_longest_side
                    and max(original_size) < LOW_RES_SINGLE_UPSCALE_THRESHOLD
                )
                use_superres_source = bool(should_upscale and superres_image is not None)
                keep_original_jpeg = (
                    not should_upscale
                    and
                    original_format in {"JPEG", "JPG"}
                    and max(original_size) <= preserve_max_dimension
                    and len(raw_bytes) <= preserve_max_bytes
                )
                if keep_original_jpeg:
                    write_bytes_if_changed(path, raw_bytes)
                    return

                variant = superres_image.copy() if use_superres_source else image.copy()
                if should_upscale and not use_superres_source:
                    scale_factor = min(
                        upscale_longest_side / max(max(original_size), 1),
                        LOW_RES_SINGLE_MAX_UPSCALE_FACTOR,
                    )
                    if scale_factor > 1.01:
                        target_size = (
                            max(1, round(original_size[0] * scale_factor)),
                            max(1, round(original_size[1] * scale_factor)),
                        )
                        variant = variant.resize(target_size, resample=resampling)

                variant.thumbnail(max_size, resampling)
                variant = variant.filter(
                    ImageFilter.UnsharpMask(radius=sharpen_radius, percent=sharpen_percent, threshold=2)
                )
                buffer = io.BytesIO()
                variant.save(buffer, format="JPEG", quality=quality, optimize=True, progressive=True)
                write_bytes_if_changed(path, buffer.getvalue())

            save_variant(
                path=full_path,
                max_size=(2600, 2600),
                quality=93,
                sharpen_radius=0.78,
                sharpen_percent=122,
                preserve_max_dimension=2600,
                preserve_max_bytes=4_000_000,
                upscale_longest_side=LOW_RES_SINGLE_FULL_TARGET,
            )
            save_variant(
                path=feed_path,
                max_size=(1800, 1800),
                quality=92,
                sharpen_radius=0.74,
                sharpen_percent=136,
                preserve_max_dimension=1800,
                preserve_max_bytes=3_000_000,
                upscale_longest_side=LOW_RES_SINGLE_FEED_TARGET,
            )
            save_variant(
                path=thumb_path,
                max_size=(1280, 1280),
                quality=89,
                sharpen_radius=0.7,
                sharpen_percent=132,
                preserve_max_dimension=1280,
                preserve_max_bytes=1_500_000,
                upscale_longest_side=None,
            )
    except Exception as error:  # pragma: no cover - runtime/image libs path
        log.warning("Image optimization fallback used: %s", error)
        for path in (full_path, feed_path, thumb_path):
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(raw_bytes)
                changes_detected = True

    return changes_detected


def optimize_single_image(raw_bytes: bytes, output_path: Path, max_size: tuple[int, int], quality: int = 86) -> bool:
    changed_detected = False
    try:
        from PIL import Image, ImageOps

        with Image.open(io.BytesIO(raw_bytes)) as image:
            image = ImageOps.exif_transpose(image)
            image.load()

            if image.mode not in ("RGB", "L"):
                background = Image.new("RGB", image.size, "white")
                alpha = image.getchannel("A") if "A" in image.getbands() else None
                background.paste(image.convert("RGBA"), mask=alpha)
                image = background
            elif image.mode == "L":
                image = image.convert("RGB")

            prepared = image.copy()
            prepared.thumbnail(max_size)

            buffer = io.BytesIO()
            prepared.save(buffer, format="JPEG", quality=quality, optimize=True, progressive=True)
            next_bytes = buffer.getvalue()
    except Exception as error:  # pragma: no cover - runtime/image libs path
        log.warning("Single image optimization fallback used: %s", error)
        next_bytes = raw_bytes

    if not output_path.exists() or output_path.read_bytes() != next_bytes:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(next_bytes)
        changed_detected = True

    return changed_detected


def parse_channel_avatar_url(html_text: str, channel_web_url: str) -> str | None:
    patterns = [
        r'tgme_page_photo_image[^>]+style="[^"]*url\([\'"]?([^\'")]+)',
        r'tgme_channel_info_header_photo[^>]+style="[^"]*url\([\'"]?([^\'")]+)',
        r'tgme_page_photo_image[^>]+src="([^"]+)"',
        r'tgme_channel_info_header_photo[^>]+src="([^"]+)"',
        r'<meta\s+property="og:image"\s+content="([^"]+)"',
        r'<link\s+rel="image_src"\s+href="([^"]+)"',
    ]

    for pattern in patterns:
        match = re.search(pattern, html_text, re.IGNORECASE)
        if match:
            return urljoin(channel_web_url, html_lib.unescape(match.group(1)))

    return None


def parse_channel_description(html_text: str) -> str | None:
    patterns = (
        r'tgme_channel_info_description[^>]*>(.*?)</div>',
        r'tgme_page_description[^>]*>(.*?)</div>',
        r'<meta\s+property="og:description"\s+content="([^"]+)"',
    )

    for pattern in patterns:
        match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
        if not match:
            continue

        raw_value = match.group(1)
        normalized = re.sub(r"<br\s*/?>", "\n", raw_value, flags=re.IGNORECASE)
        normalized = re.sub(r"</p>\s*<p[^>]*>", "\n\n", normalized, flags=re.IGNORECASE)
        normalized = strip_tags(normalized)
        normalized = html_lib.unescape(normalized)
        normalized = collapse_whitespace(normalized)
        if normalized:
            return normalized

    return None


def mirror_channel_avatar(config: SiteConfig, html_text: str) -> bool:
    avatar_url = parse_channel_avatar_url(html_text, config.channel_web_url)
    if not avatar_url:
        return False

    try:
        raw_bytes = fetch_binary(avatar_url)
    except Exception as error:  # pragma: no cover - network/runtime path
        log.warning("Failed to fetch channel avatar for %s: %s", config.channel_username, error)
        return False

    changed = optimize_single_image(raw_bytes, CHANNEL_AVATAR_PATH, (512, 512), quality=90)
    relative_avatar_path = CHANNEL_AVATAR_PATH.relative_to(DOCS_DIR).as_posix()
    if config.avatar_path != relative_avatar_path:
        config.avatar_path = relative_avatar_path
        changed = True

    return changed


def extract_media_variant_tag(path: Path) -> str | None:
    match = re.search(r"-(v\d+)\.[^.]+$", path.name)
    return match.group(1) if match else None


def should_delete_inactive_media(path: Path, age_seconds: float, current_variant_tags: set[str] | None = None) -> bool:
    active_variants = current_variant_tags or {IMAGE_VARIANT_VERSION}
    if age_seconds >= STALE_MEDIA_RETENTION_SECONDS:
        return True

    variant_tag = extract_media_variant_tag(path)
    if variant_tag and variant_tag not in active_variants and age_seconds >= LEGACY_VARIANT_RETENTION_SECONDS:
        return True

    return False


def detect_video_media_hint(block: str) -> bool:
    return bool(
        re.search(
            r"(tgme_widget_message_video|video_player|video_note|round_message|message_video)",
            block,
            re.IGNORECASE,
        )
    )


def extract_video_dimensions_from_html(block: str) -> tuple[int | None, int | None]:
    video_tag_match = re.search(r"<video\b[^>]*>", block, re.IGNORECASE)
    if not video_tag_match:
        return None, None

    video_tag = video_tag_match.group(0)
    width_match = re.search(r'\bwidth="(\d+)"', video_tag, re.IGNORECASE)
    height_match = re.search(r'\bheight="(\d+)"', video_tag, re.IGNORECASE)
    if width_match and height_match:
        return int(width_match.group(1)), int(height_match.group(1))

    style_match = re.search(r'\bstyle="([^"]+)"', video_tag, re.IGNORECASE)
    if not style_match:
        return None, None

    style = style_match.group(1)
    width_style = re.search(r"width:\s*(\d+)px", style, re.IGNORECASE)
    height_style = re.search(r"height:\s*(\d+)px", style, re.IGNORECASE)
    if width_style and height_style:
        return int(width_style.group(1)), int(height_style.group(1))

    return None, None


def extract_standard_video_entries_from_html(block: str) -> list[dict[str, Any]]:
    video_chunks = re.findall(r"(<video\b[\s\S]*?</video>)", block, re.IGNORECASE)
    entries: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for chunk in video_chunks:
        video_tag_match = re.search(r"<video\b([^>]*)>", chunk, re.IGNORECASE)
        if not video_tag_match:
            continue

        video_tag = video_tag_match.group(0)
        src_match = re.search(r'\bsrc="([^"]+)"', video_tag, re.IGNORECASE)
        if not src_match:
            src_match = re.search(r'<source[^>]+src="([^"]+)"', chunk, re.IGNORECASE)
        if not src_match:
            continue

        width, height = extract_video_dimensions_from_html(chunk)
        if detect_round_video_hint(chunk, width, height):
            continue

        normalized_url = urljoin("https://t.me", html_lib.unescape(src_match.group(1)).strip())
        if not normalized_url or normalized_url in seen_urls:
            continue
        seen_urls.add(normalized_url)

        entry: dict[str, Any] = {
            "url": normalized_url,
            "source_url": normalized_url,
        }
        if width and height:
            entry["width"] = width
            entry["height"] = height

        poster_match = re.search(r'\bposter="([^"]+)"', video_tag, re.IGNORECASE)
        if poster_match:
            poster_url = urljoin("https://t.me", html_lib.unescape(poster_match.group(1)).strip())
            if poster_url:
                entry["poster"] = {
                    "thumb_url": poster_url,
                    "feed_url": poster_url,
                    "full_url": poster_url,
                    "source_url": poster_url,
                }

        entries.append(entry)

    return entries


def is_square_like_dimensions(width: int | None, height: int | None, tolerance: float = 0.12) -> bool:
    if not width or not height:
        return False
    larger = max(width, height)
    smaller = min(width, height)
    if not larger or not smaller:
        return False
    return abs(1 - (smaller / larger)) <= tolerance


def detect_round_video_hint(block: str, width: int | None = None, height: int | None = None) -> bool:
    if re.search(r"(video_note|round_message|roundvideo|message_video_note)", block, re.IGNORECASE):
        return True
    if is_square_like_dimensions(width, height):
        return max(width or 0, height or 0) <= 640
    return False


def mirror_post_photos(posts: list[dict[str, Any]], photo_overrides: dict[int, list[bytes]] | None = None) -> bool:
    POSTS_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    POSTS_THUMBS_DIR.mkdir(parents=True, exist_ok=True)
    POSTS_FEED_DIR.mkdir(parents=True, exist_ok=True)
    active_relative_paths: set[str] = set()
    changes_detected = False
    photo_overrides = photo_overrides or {}

    for post in posts:
        mirrored_photos: list[dict[str, Any]] = []
        post_photo_overrides = photo_overrides.get(post["id"], [])
        normalized_photos = [normalize_photo_entry(raw_photo) for raw_photo in post.get("photos") or []]
        normalized_photos = [photo for photo in normalized_photos if photo]
        is_single_photo_post = len(normalized_photos) == 1

        for index, photo in enumerate(normalized_photos):
            full_source = photo["full_url"]
            source_fetch_url = photo.get("source_url") or full_source
            override_bytes = post_photo_overrides[index] if index < len(post_photo_overrides) else None
            if not override_bytes and not re.match(r"^https?://", source_fetch_url):
                local_full_path = DOCS_DIR / full_source.lstrip("./")
                local_thumb_path = DOCS_DIR / photo["thumb_url"].lstrip("./")
                local_feed_path = DOCS_DIR / photo.get("feed_url", "").lstrip("./") if photo.get("feed_url") else None
                current_variant_present = (
                    IMAGE_VARIANT_VERSION in Path(full_source).name
                    and IMAGE_VARIANT_VERSION in Path(photo["thumb_url"]).name
                    and (not is_single_photo_post or (photo.get("feed_url") and IMAGE_VARIANT_VERSION in Path(photo["feed_url"]).name))
                )
                current_files_present = (
                    is_valid_local_image(local_full_path)
                    and is_valid_local_image(local_thumb_path)
                    and (not is_single_photo_post or is_valid_local_image(local_feed_path))
                )

                if current_variant_present and current_files_present:
                    current_entry = with_local_variant_dimensions(
                        photo,
                        thumb_path=local_thumb_path,
                        feed_path=local_feed_path if is_single_photo_post else None,
                        full_path=local_full_path,
                    )
                    active_relative_paths.add(full_source)
                    active_relative_paths.add(photo["thumb_url"])
                    if is_single_photo_post and photo.get("feed_url"):
                        active_relative_paths.add(photo["feed_url"])
                    mirrored_photos.append(current_entry)
                    continue

                if local_full_path.exists() and is_valid_local_image(local_full_path):
                    try:
                        override_bytes = local_full_path.read_bytes()
                    except Exception as error:  # pragma: no cover - runtime/filesystem path
                        log.warning("Failed to read existing mirrored image for post %s: %s", post["id"], error)
                        active_relative_paths.add(full_source)
                        active_relative_paths.add(photo["thumb_url"])
                        if is_single_photo_post and photo.get("feed_url"):
                            active_relative_paths.add(photo["feed_url"])
                        mirrored_photos.append(
                            with_local_variant_dimensions(
                                photo,
                                thumb_path=local_thumb_path,
                                feed_path=local_feed_path if is_single_photo_post else None,
                                full_path=local_full_path,
                            )
                        )
                        continue
                else:
                    for candidate_path in (local_full_path, local_thumb_path, local_feed_path):
                        if candidate_path and candidate_path.exists() and not is_valid_local_image(candidate_path):
                            try:
                                candidate_path.unlink()
                                changes_detected = True
                                log.warning("Deleted invalid mirrored image %s", candidate_path.relative_to(ROOT))
                            except Exception as error:  # pragma: no cover - runtime/filesystem path
                                log.warning("Failed to delete invalid mirrored image %s: %s", candidate_path.relative_to(ROOT), error)
                    active_relative_paths.add(full_source)
                    active_relative_paths.add(photo["thumb_url"])
                    if is_single_photo_post and photo.get("feed_url"):
                        active_relative_paths.add(photo["feed_url"])
                    mirrored_photos.append(photo)
                    continue

            digest_source = override_bytes if override_bytes else source_fetch_url.encode("utf-8")
            digest = hashlib.sha256(digest_source).hexdigest()[:12]
            filename = f"{post['id']}-{index + 1}-{digest}-{IMAGE_VARIANT_VERSION}.jpg"
            full_path = POSTS_MEDIA_DIR / filename
            feed_path = POSTS_FEED_DIR / filename
            thumb_path = POSTS_THUMBS_DIR / filename

            try:
                if (
                    not full_path.exists()
                    or not thumb_path.exists()
                    or (is_single_photo_post and not feed_path.exists())
                ):
                    raw_bytes = override_bytes or fetch_binary(source_fetch_url)
                    if optimize_image_variants(
                        raw_bytes,
                        full_path,
                        feed_path,
                        thumb_path,
                        allow_single_image_upscale=is_single_photo_post,
                    ):
                        log.info("Prepared image variants for post %s", post["id"])
                        changes_detected = True
                if not is_valid_local_image(full_path) or not is_valid_local_image(thumb_path) or (is_single_photo_post and not is_valid_local_image(feed_path)):
                    raise ValueError("Generated image variants are invalid")
            except Exception as error:  # pragma: no cover - network/runtime path
                log.warning("Failed to mirror image for post %s: %s", post["id"], error)
                mirrored_photos.append(photo)
                continue

            full_relative_url = full_path.relative_to(DOCS_DIR).as_posix()
            feed_relative_url = feed_path.relative_to(DOCS_DIR).as_posix()
            thumb_relative_url = thumb_path.relative_to(DOCS_DIR).as_posix()
            mirrored_entry = {
                "thumb_url": thumb_relative_url,
                "full_url": full_relative_url,
            }
            if re.match(r"^https?://", source_fetch_url):
                mirrored_entry["source_url"] = source_fetch_url
            if is_single_photo_post:
                mirrored_entry["feed_url"] = feed_relative_url
                active_relative_paths.add(feed_relative_url)
            mirrored_photos.append(
                with_local_variant_dimensions(
                    mirrored_entry,
                    thumb_path=thumb_path,
                    feed_path=feed_path if is_single_photo_post else None,
                    full_path=full_path,
                )
            )
            active_relative_paths.add(full_relative_url)
            active_relative_paths.add(thumb_relative_url)

        if post.get("photos") != mirrored_photos:
            post["photos"] = mirrored_photos

    deleted_files = 0
    deleted_bytes = 0
    for base_dir in (POSTS_MEDIA_DIR, POSTS_THUMBS_DIR, POSTS_FEED_DIR):
        for path in base_dir.glob("*"):
            if not path.is_file():
                continue

            relative_url = path.relative_to(DOCS_DIR).as_posix()
            if relative_url in active_relative_paths:
                continue

            stat = path.stat()
            age_seconds = max(0, time.time() - stat.st_mtime)
            if not should_delete_inactive_media(path, age_seconds, {IMAGE_VARIANT_VERSION}):
                continue

            path.unlink()
            log.info("Deleted stale mirrored image %s", path.relative_to(ROOT))
            changes_detected = True
            deleted_files += 1
            deleted_bytes += stat.st_size

    if deleted_files:
        log.info(
            "Media maintenance deleted %s files (%.1f MB) for %s",
            deleted_files,
            deleted_bytes / (1024 * 1024),
            CHANNEL_KEY or "channel",
        )

    return changes_detected


def reuse_existing_round_video_assets(
    post: dict[str, Any],
    existing_post: dict[str, Any] | None,
    active_relative_paths: set[str],
) -> bool:
    if not existing_post:
        return False

    reused = False

    existing_video_url = existing_post.get("video_url")
    existing_video_path = resolve_local_asset_path(existing_video_url)
    if is_valid_local_video(existing_video_path):
        if post.get("video_url") != existing_video_url:
            post["video_url"] = existing_video_url
            reused = True
        active_relative_paths.add(str(existing_video_url))

    existing_poster = normalize_video_poster_entry(existing_post.get("video_poster"))
    if existing_poster:
        poster_urls = [
            existing_poster.get("thumb_url"),
            existing_poster.get("feed_url"),
            existing_poster.get("full_url"),
        ]
        if any(is_valid_local_image(resolve_local_asset_path(url)) for url in poster_urls):
            if post.get("video_poster") != existing_poster:
                post["video_poster"] = existing_poster
                reused = True
            for url in poster_urls:
                if is_local_asset_url(url):
                    active_relative_paths.add(str(url))

    if existing_post.get("video_width") and existing_post.get("video_height"):
        if not post.get("video_width") or not post.get("video_height"):
            post["video_width"] = existing_post.get("video_width")
            post["video_height"] = existing_post.get("video_height")
            reused = True

    return reused


def mirror_round_videos(
    posts: list[dict[str, Any]],
    video_metadata: dict[int, dict[str, Any]] | None = None,
    existing_posts: list[dict[str, Any]] | None = None,
) -> bool:
    POSTS_VIDEOS_DIR.mkdir(parents=True, exist_ok=True)
    POSTS_VIDEO_POSTERS_DIR.mkdir(parents=True, exist_ok=True)
    active_relative_paths: set[str] = set()
    changes_detected = False
    video_metadata = video_metadata or {}
    existing_posts_by_id = {
        int(entry.get("id")): entry
        for entry in (existing_posts or [])
        if entry.get("id")
    }

    for post in posts:
        metadata = video_metadata.get(post["id"])
        existing_post = existing_posts_by_id.get(int(post["id"]))
        if metadata is not None:
            if metadata.get("video_note"):
                post["video_note"] = True
            else:
                post.pop("video_note", None)

            if metadata.get("video_width") and metadata.get("video_height"):
                post["video_width"] = metadata["video_width"]
                post["video_height"] = metadata["video_height"]
            else:
                post.pop("video_width", None)
                post.pop("video_height", None)

        raw_bytes = (metadata or {}).get("video_bytes")
        extension = (metadata or {}).get("video_extension") or infer_video_extension_from_url(post.get("video_url"))
        local_video_url = post.get("video_url") if is_local_asset_url(post.get("video_url")) else None
        local_video_path = resolve_local_asset_path(local_video_url)
        if local_video_url and is_valid_local_video(local_video_path):
            active_relative_paths.add(str(local_video_url))
        else:
            local_video_url = None
            local_video_path = None

        if not raw_bytes and not local_video_url:
            reused_existing_assets = reuse_existing_round_video_assets(post, existing_post, active_relative_paths)
            changes_detected = reused_existing_assets or changes_detected
            local_video_url = post.get("video_url") if is_local_asset_url(post.get("video_url")) else None
            local_video_path = resolve_local_asset_path(local_video_url)

        if not raw_bytes and post.get("video_note") and not local_video_url and re.match(r"^https?://", str(post.get("video_url") or ""), re.IGNORECASE):
            try:
                raw_bytes = fetch_binary(
                    str(post["video_url"]),
                    timeout=40,
                    retry_delays=DIRECT_POST_PROBE_FETCH_RETRY_DELAYS,
                    accept="video/*,*/*;q=0.8",
                    extra_headers={"Referer": str(post.get("tg_url") or "")},
                )
                log.info("Mirrored round video source fetched for post %s", post["id"])
            except Exception as error:  # pragma: no cover - network/runtime path
                log.warning("Round video source fetch failed on post %s: %s", post["id"], error)

        if not raw_bytes and local_video_path and is_valid_local_video(local_video_path):
            try:
                raw_bytes = local_video_path.read_bytes()
            except Exception:
                raw_bytes = None

        if not raw_bytes and not local_video_url:
            if post.get("video_note") and not post.get("video_url"):
                log.warning("Round video post %s has no mirrored video bytes or fallback URL", post["id"])
            continue

        if raw_bytes:
            optimized_bytes = optimize_video_for_streaming(raw_bytes, extension)
            if optimized_bytes != raw_bytes:
                raw_bytes = optimized_bytes
                changes_detected = True

        digest = hashlib.sha256(raw_bytes).hexdigest()[:12]
        filename = f"{post['id']}-video-note-{digest}-{VIDEO_VARIANT_VERSION}.{extension}"
        video_path = POSTS_VIDEOS_DIR / filename
        if not video_path.exists() or video_path.read_bytes() != raw_bytes:
            video_path.write_bytes(raw_bytes)
            log.info("Mirrored round video for post %s", post["id"])
            changes_detected = True

        relative_url = video_path.relative_to(DOCS_DIR).as_posix()
        active_relative_paths.add(relative_url)
        if post.get("video_url") != relative_url:
            post["video_url"] = relative_url

        poster_bytes = (metadata or {}).get("poster_bytes")
        if not poster_bytes:
            existing_poster = normalize_video_poster_entry(post.get("video_poster")) or normalize_video_poster_entry(
                existing_post.get("video_poster") if existing_post else None
            )
            if existing_poster:
                poster_urls = [
                    existing_poster.get("thumb_url"),
                    existing_poster.get("feed_url"),
                    existing_poster.get("full_url"),
                ]
                for url in poster_urls:
                    poster_path = resolve_local_asset_path(url)
                    if is_valid_local_image(poster_path):
                        try:
                            poster_bytes = poster_path.read_bytes()
                            break
                        except Exception:
                            continue

        if not poster_bytes and raw_bytes:
            poster_bytes = extract_video_poster_bytes(raw_bytes, extension)

        if poster_bytes:
            poster_digest = hashlib.sha256(poster_bytes).hexdigest()[:12]
            poster_filename = f"{post['id']}-video-note-poster-{poster_digest}-{VIDEO_VARIANT_VERSION}.jpg"
            poster_path = POSTS_VIDEO_POSTERS_DIR / poster_filename
            if optimize_single_image(poster_bytes, poster_path, (1024, 1024), quality=88):
                changes_detected = True
            relative_poster_url = poster_path.relative_to(DOCS_DIR).as_posix()
            active_relative_paths.add(relative_poster_url)
            poster_entry = with_local_variant_dimensions(
                {
                    "thumb_url": relative_poster_url,
                    "feed_url": relative_poster_url,
                    "full_url": relative_poster_url,
                },
                thumb_path=poster_path,
                feed_path=poster_path,
                full_path=poster_path,
            )
            if post.get("video_poster") != poster_entry:
                post["video_poster"] = poster_entry
        elif post.get("video_poster"):
            poster_entry = normalize_video_poster_entry(post.get("video_poster"))
            if poster_entry:
                for key in ("thumb_url", "feed_url", "full_url"):
                    if poster_entry.get(key):
                        active_relative_paths.add(poster_entry[key])

    deleted_files = 0
    deleted_bytes = 0
    for base_dir in (POSTS_VIDEOS_DIR, POSTS_VIDEO_POSTERS_DIR):
        for path in base_dir.glob("*"):
            if not path.is_file():
                continue

            relative_url = path.relative_to(DOCS_DIR).as_posix()
            if relative_url in active_relative_paths:
                continue

            stat = path.stat()
            age_seconds = max(0, time.time() - stat.st_mtime)
            if not should_delete_inactive_media(path, age_seconds, {VIDEO_VARIANT_VERSION}):
                continue

            path.unlink()
            changes_detected = True
            deleted_files += 1
            deleted_bytes += stat.st_size
            log.info("Deleted stale mirrored video asset %s", path.relative_to(ROOT))

    if deleted_files:
        log.info(
            "Round video maintenance deleted %s files (%.1f MB) for %s",
            deleted_files,
            deleted_bytes / (1024 * 1024),
            CHANNEL_KEY or "channel",
        )

    return changes_detected


def reuse_existing_attached_video_assets(
    post: dict[str, Any],
    existing_post: dict[str, Any] | None,
    active_relative_paths: set[str],
) -> bool:
    if not existing_post:
        return False

    existing_videos = normalize_video_entries(existing_post.get("videos"))
    if not existing_videos:
        return False

    reused = False
    reusable_entries: list[dict[str, Any]] = []
    for entry in existing_videos:
        local_video_path = resolve_local_asset_path(entry.get("url"))
        if not is_valid_local_video(local_video_path):
            return False

        normalized_entry: dict[str, Any] = {"url": entry["url"]}
        active_relative_paths.add(entry["url"])

        source_url = str(entry.get("source_url") or "").strip()
        if source_url and re.match(r"^https?://", source_url, re.IGNORECASE):
            normalized_entry["source_url"] = source_url

        for key in ("width", "height"):
            parsed = parse_positive_int(entry.get(key))
            if parsed:
                normalized_entry[key] = parsed

        poster = normalize_video_poster_entry(entry.get("poster"))
        if poster:
            poster_urls = [
                poster.get("thumb_url"),
                poster.get("feed_url"),
                poster.get("full_url"),
            ]
            if any(is_valid_local_image(resolve_local_asset_path(url)) for url in poster_urls):
                normalized_entry["poster"] = poster
                for url in poster_urls:
                    if is_local_asset_url(url):
                        active_relative_paths.add(str(url))

        reusable_entries.append(normalized_entry)

    if post.get("videos") != reusable_entries:
        post["videos"] = reusable_entries
        reused = True

    return reused


def mirror_attached_videos(
    posts: list[dict[str, Any]],
    video_metadata: dict[int, dict[str, Any]] | None = None,
    existing_posts: list[dict[str, Any]] | None = None,
) -> bool:
    POSTS_ATTACHED_VIDEOS_DIR.mkdir(parents=True, exist_ok=True)
    POSTS_ATTACHED_VIDEO_POSTERS_DIR.mkdir(parents=True, exist_ok=True)
    active_relative_paths: set[str] = set()
    changes_detected = False
    video_metadata = video_metadata or {}
    existing_posts_by_id = {
        int(entry.get("id")): entry
        for entry in (existing_posts or [])
        if entry.get("id")
    }

    for post in posts:
        if post.get("video_note"):
            continue

        existing_post = existing_posts_by_id.get(int(post["id"]))
        current_entries = normalize_video_entries(post.get("videos"))
        metadata_entries = (video_metadata.get(post["id"]) or {}).get("attached_videos") or []

        if not metadata_entries:
            if reuse_existing_attached_video_assets(post, existing_post, active_relative_paths):
                changes_detected = True
                continue

            if current_entries:
                for entry in current_entries:
                    video_url = entry.get("url")
                    if is_local_asset_url(video_url):
                        local_video_path = resolve_local_asset_path(video_url)
                        if is_valid_local_video(local_video_path):
                            active_relative_paths.add(str(video_url))
                    poster = normalize_video_poster_entry(entry.get("poster"))
                    if not poster:
                        continue
                    for key in ("thumb_url", "feed_url", "full_url"):
                        url = poster.get(key)
                        if is_local_asset_url(url):
                            poster_path = resolve_local_asset_path(url)
                            if is_valid_local_image(poster_path):
                                active_relative_paths.add(str(url))
                if post.get("videos") != current_entries:
                    post["videos"] = current_entries
                    changes_detected = True
            elif post.get("videos"):
                post.pop("videos", None)
                changes_detected = True
            continue

        mirrored_entries: list[dict[str, Any]] = []
        for index, metadata_entry in enumerate(metadata_entries):
            existing_entry = current_entries[index] if index < len(current_entries) else None
            raw_bytes = metadata_entry.get("video_bytes")
            extension = (
                str(metadata_entry.get("video_extension") or "").strip().lower()
                or infer_video_extension_from_url(
                    (existing_entry or {}).get("source_url") or (existing_entry or {}).get("url")
                )
                or "mp4"
            )

            if not raw_bytes and existing_entry:
                local_existing_path = resolve_local_asset_path(existing_entry.get("url"))
                if is_valid_local_video(local_existing_path):
                    try:
                        raw_bytes = local_existing_path.read_bytes()
                    except Exception:
                        raw_bytes = None

            if not raw_bytes and existing_entry:
                remote_video_url = str(existing_entry.get("source_url") or existing_entry.get("url") or "").strip()
                if re.match(r"^https?://", remote_video_url, re.IGNORECASE):
                    try:
                        raw_bytes = fetch_binary(
                            remote_video_url,
                            timeout=40,
                            retry_delays=FAST_EXTERNAL_RETRY_DELAYS,
                            accept="video/*,*/*;q=0.8",
                            extra_headers={"Referer": str(post.get("tg_url") or "")},
                        )
                    except Exception:
                        raw_bytes = None

            if not raw_bytes:
                continue

            optimized_bytes = optimize_video_for_streaming(raw_bytes, extension)
            if optimized_bytes != raw_bytes:
                raw_bytes = optimized_bytes
                changes_detected = True

            digest = hashlib.sha256(raw_bytes).hexdigest()[:12]
            filename = f"{post['id']}-video-{index + 1}-{digest}-{VIDEO_VARIANT_VERSION}.{extension}"
            video_path = POSTS_ATTACHED_VIDEOS_DIR / filename
            if not video_path.exists() or video_path.read_bytes() != raw_bytes:
                video_path.write_bytes(raw_bytes)
                changes_detected = True

            relative_video_url = video_path.relative_to(DOCS_DIR).as_posix()
            active_relative_paths.add(relative_video_url)
            mirrored_entry: dict[str, Any] = {"url": relative_video_url}

            source_url = str((existing_entry or {}).get("source_url") or "").strip()
            if source_url and re.match(r"^https?://", source_url, re.IGNORECASE):
                mirrored_entry["source_url"] = source_url

            for key in ("width", "height"):
                parsed = parse_positive_int(metadata_entry.get(key) or (existing_entry or {}).get(key))
                if parsed:
                    mirrored_entry[key] = parsed

            poster_bytes = metadata_entry.get("poster_bytes")
            if not poster_bytes and existing_entry:
                existing_poster = normalize_video_poster_entry(existing_entry.get("poster"))
                if existing_poster:
                    for key in ("full_url", "feed_url", "thumb_url"):
                        poster_path = resolve_local_asset_path(existing_poster.get(key))
                        if is_valid_local_image(poster_path):
                            try:
                                poster_bytes = poster_path.read_bytes()
                                break
                            except Exception:
                                continue

            if not poster_bytes:
                poster_bytes = extract_video_poster_bytes(raw_bytes, extension)

            if poster_bytes:
                poster_digest = hashlib.sha256(poster_bytes).hexdigest()[:12]
                poster_filename = f"{post['id']}-video-{index + 1}-poster-{poster_digest}-{VIDEO_VARIANT_VERSION}.jpg"
                poster_path = POSTS_ATTACHED_VIDEO_POSTERS_DIR / poster_filename
                if optimize_single_image(poster_bytes, poster_path, (1280, 1280), quality=88):
                    changes_detected = True
                relative_poster_url = poster_path.relative_to(DOCS_DIR).as_posix()
                active_relative_paths.add(relative_poster_url)
                mirrored_entry["poster"] = with_local_variant_dimensions(
                    {
                        "thumb_url": relative_poster_url,
                        "feed_url": relative_poster_url,
                        "full_url": relative_poster_url,
                    },
                    thumb_path=poster_path,
                    feed_path=poster_path,
                    full_path=poster_path,
                )

            mirrored_entries.append(mirrored_entry)

        if mirrored_entries:
            if post.get("videos") != mirrored_entries:
                post["videos"] = mirrored_entries
                changes_detected = True
            if post.get("video_url"):
                post.pop("video_url", None)
                post.pop("video_width", None)
                post.pop("video_height", None)
                post.pop("video_poster", None)
                changes_detected = True
        elif reuse_existing_attached_video_assets(post, existing_post, active_relative_paths):
            changes_detected = True
        elif current_entries:
            for entry in current_entries:
                if is_local_asset_url(entry.get("url")):
                    local_video_path = resolve_local_asset_path(entry.get("url"))
                    if is_valid_local_video(local_video_path):
                        active_relative_paths.add(str(entry["url"]))
                poster = normalize_video_poster_entry(entry.get("poster"))
                if poster:
                    for key in ("thumb_url", "feed_url", "full_url"):
                        url = poster.get(key)
                        if is_local_asset_url(url):
                            poster_path = resolve_local_asset_path(url)
                            if is_valid_local_image(poster_path):
                                active_relative_paths.add(str(url))
            if post.get("videos") != current_entries:
                post["videos"] = current_entries
                changes_detected = True

    deleted_files = 0
    deleted_bytes = 0
    for base_dir in (POSTS_ATTACHED_VIDEOS_DIR, POSTS_ATTACHED_VIDEO_POSTERS_DIR):
        for path in base_dir.glob("*"):
            if not path.is_file():
                continue

            relative_url = path.relative_to(DOCS_DIR).as_posix()
            if relative_url in active_relative_paths:
                continue

            stat = path.stat()
            age_seconds = max(0, time.time() - stat.st_mtime)
            if not should_delete_inactive_media(path, age_seconds, {VIDEO_VARIANT_VERSION}):
                continue

            path.unlink()
            changes_detected = True
            deleted_files += 1
            deleted_bytes += stat.st_size
            log.info("Deleted stale mirrored attached video asset %s", path.relative_to(ROOT))

    if deleted_files:
        log.info(
            "Attached video maintenance deleted %s files (%.1f MB) for %s",
            deleted_files,
            deleted_bytes / (1024 * 1024),
            CHANNEL_KEY or "channel",
        )

    return changes_detected


def mirror_link_previews(
    posts: list[dict[str, Any]],
    existing_posts: list[dict[str, Any]] | None = None,
) -> bool:
    POSTS_LINK_PREVIEWS_DIR.mkdir(parents=True, exist_ok=True)
    active_relative_paths: set[str] = set()
    changes_detected = False
    existing_posts_by_id = {
        int(entry.get("id")): entry
        for entry in (existing_posts or [])
        if entry.get("id")
    }

    for post in posts:
        existing_post = existing_posts_by_id.get(int(post.get("id") or 0))
        existing_preview = normalize_link_preview_entry(existing_post.get("link_preview") if existing_post else None)
        if post_has_physical_media(post):
            if post.pop("link_preview", None) is not None:
                changes_detected = True
            continue
        candidate_urls = select_link_preview_candidates(post.get("text_html"))
        if not candidate_urls:
            if post.pop("link_preview", None) is not None:
                changes_detected = True
            continue

        if existing_preview and existing_preview.get("href") in candidate_urls:
            post["link_preview"] = existing_preview
            remember_link_preview_assets(existing_preview, active_relative_paths)
            continue

        preview_entry = None
        for link_url in candidate_urls:
            hostname = normalize_host_label(urlparse(link_url).hostname)
            if hostname and hostname in FAILED_LINK_PREVIEW_HOSTS:
                continue

            try:
                page_html = fetch_page(
                    link_url,
                    timeout=LINK_PREVIEW_PAGE_TIMEOUT_SECONDS,
                    retry_delays=FAST_EXTERNAL_RETRY_DELAYS,
                    log_failures=False,
                )
            except Exception as error:  # pragma: no cover - network/runtime path
                if hostname:
                    FAILED_LINK_PREVIEW_HOSTS.add(hostname)
                log.warning("Failed to fetch link preview page for post %s: %s", post.get("id"), error)
                continue

            preview_metadata = extract_link_preview_metadata(page_html, link_url, link_url)
            if not preview_metadata or not preview_metadata.get("is_video"):
                continue

            preview_entry = normalize_link_preview_entry(preview_metadata)
            if not preview_entry:
                continue

            image_url = preview_metadata.get("image_url")
            if image_url:
                try:
                    image_bytes = fetch_binary(
                        image_url,
                        timeout=LINK_PREVIEW_IMAGE_TIMEOUT_SECONDS,
                        retry_delays=FAST_EXTERNAL_RETRY_DELAYS,
                        log_failures=False,
                    )
                    digest = hashlib.sha256(image_bytes).hexdigest()[:12]
                    image_path = POSTS_LINK_PREVIEWS_DIR / f"{post['id']}-link-preview-{digest}-{IMAGE_VARIANT_VERSION}.jpg"
                    if optimize_single_image(image_bytes, image_path, (1280, 960), quality=86):
                        changes_detected = True
                    relative_url = image_path.relative_to(DOCS_DIR).as_posix()
                    active_relative_paths.add(relative_url)
                    preview_entry["image"] = with_local_variant_dimensions(
                        {
                            "thumb_url": relative_url,
                            "feed_url": relative_url,
                            "full_url": relative_url,
                            "source_url": image_url,
                        },
                        thumb_path=image_path,
                        feed_path=image_path,
                        full_path=image_path,
                    )
                except Exception as error:  # pragma: no cover - network/runtime path
                    image_host = normalize_host_label(urlparse(image_url).hostname)
                    if image_host:
                        FAILED_LINK_PREVIEW_HOSTS.add(image_host)
                    log.warning("Failed to mirror link preview image for post %s: %s", post.get("id"), error)

            remember_link_preview_assets(preview_entry, active_relative_paths)
            break

        if preview_entry:
            if post.get("link_preview") != preview_entry:
                post["link_preview"] = preview_entry
                changes_detected = True
        elif post.pop("link_preview", None) is not None:
            changes_detected = True

    deleted_files = 0
    deleted_bytes = 0
    for path in POSTS_LINK_PREVIEWS_DIR.glob("*"):
        if not path.is_file():
            continue

        relative_url = path.relative_to(DOCS_DIR).as_posix()
        if relative_url in active_relative_paths:
            continue

        stat = path.stat()
        age_seconds = max(0, time.time() - stat.st_mtime)
        if not should_delete_inactive_media(path, age_seconds, {IMAGE_VARIANT_VERSION}):
            continue

        path.unlink()
        changes_detected = True
        deleted_files += 1
        deleted_bytes += stat.st_size
        log.info("Deleted stale mirrored link preview %s", path.relative_to(ROOT))

    if deleted_files:
        log.info(
            "Link preview maintenance deleted %s files (%.1f MB) for %s",
            deleted_files,
            deleted_bytes / (1024 * 1024),
            CHANNEL_KEY or "channel",
        )

    return changes_detected


def validate_round_video_posts(posts: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []

    for post in posts:
        if not post.get("video_note"):
            continue

        video_url = post.get("video_url")
        video_path = resolve_local_asset_path(video_url)
        if not is_valid_local_video(video_path):
            errors.append(f"post {post.get('id')}: round video is not mirrored locally ({video_url})")
            continue
        if video_path and video_path.suffix.lower() in {".mp4", ".mov", ".m4v"}:
            try:
                if not has_faststart_layout(video_path.read_bytes(), video_path.suffix.lstrip(".")):
                    errors.append(f"post {post.get('id')}: round video is not faststart optimized ({video_url})")
                    continue
            except Exception as error:
                errors.append(f"post {post.get('id')}: failed to inspect round video container ({error})")
                continue

        poster = normalize_video_poster_entry(post.get("video_poster"))
        poster_urls = [
            poster.get("thumb_url") if poster else None,
            poster.get("feed_url") if poster else None,
            poster.get("full_url") if poster else None,
        ]
        if not any(is_valid_local_image(resolve_local_asset_path(url)) for url in poster_urls):
            errors.append(f"post {post.get('id')}: round video poster is missing")

    return errors


def parse_count(raw: str | None) -> int:
    if not raw:
        return 0

    token = re.sub(r"\s+", "", html_lib.unescape(raw)).upper()
    token = token.replace(",", ".")

    try:
        if token.endswith("K"):
            return int(float(token[:-1]) * 1000)
        if token.endswith("M"):
            return int(float(token[:-1]) * 1_000_000)
        digits = re.sub(r"[^\d]", "", token)
        return int(digits) if digits else 0
    except ValueError:
        return 0


def extract_inline_emoji_fallback(attrs: str | None = None, inner_html: str | None = None) -> str:
    attr_text = attrs or ""
    candidates: list[str] = []

    for attr_name in ("alt", "data-content", "aria-label", "emoji-text", "title"):
        match = re.search(rf'{attr_name}=["\']([^"\']+)["\']', attr_text, re.IGNORECASE)
        if match:
            candidates.append(match.group(1))

    if inner_html:
        inner_text = html_lib.unescape(strip_tags(inner_html)).strip()
        if inner_text:
            candidates.append(inner_text)

    for candidate in candidates:
        value = html_lib.unescape(strip_tags(candidate or "")).strip()
        if not value:
            continue
        if re.match(r"^https?://", value, re.IGNORECASE):
            continue
        return value

    return ""


def replace_inline_emoji_markup(raw_html: str) -> str:
    if not raw_html:
        return raw_html

    def replace_img(match: re.Match[str]) -> str:
        fallback = extract_inline_emoji_fallback(match.group(1))
        return html_lib.escape(fallback) if fallback else ""

    def replace_emoji_tag(match: re.Match[str]) -> str:
        tag_name = (match.group(1) or "").lower()
        attrs = match.group(2) or ""
        inner_html = match.group(3) or ""
        has_emoji_class = bool(
            re.search(
                r'class=["\'][^"\']*\b(?:emoji|custom-emoji|tg-emoji|animated-emoji)\b[^"\']*["\']',
                attrs,
                re.IGNORECASE,
            )
        )
        if tag_name != "tg-emoji" and not has_emoji_class:
            return match.group(0)

        fallback = extract_inline_emoji_fallback(attrs, inner_html)
        return html_lib.escape(fallback) if fallback else ""

    replaced = re.sub(r"<img\b([^>]*)>", replace_img, raw_html, flags=re.IGNORECASE | re.DOTALL)
    replaced = re.sub(
        r"<(tg-emoji|span|i)\b([^>]*)>(.*?)</\1>",
        replace_emoji_tag,
        replaced,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return replaced


def build_text_fields(raw_html: str) -> tuple[str | None, str | None]:
    raw_html = raw_html or ""
    raw_with_breaks = replace_inline_emoji_markup(re.sub(r"<br\s*/?>", "\n", raw_html))
    anchors: list[str] = []

    def anchor_replacer(match: re.Match[str]) -> str:
        href = html_lib.unescape(match.group(1)).strip()
        label = html_lib.unescape(re.sub(r"<[^>]+>", "", match.group(2))).strip() or href
        if not href.startswith(("http://", "https://")):
            return html_lib.escape(label)
        token = f"__ANCHOR_{len(anchors)}__"
        anchors.append(
            f'<a href="{html_lib.escape(href)}" target="_blank" rel="noopener noreferrer">{html_lib.escape(label)}</a>'
        )
        return token

    html_markup = re.sub(
        r"<a[^>]+href=\"([^\"]+)\"[^>]*>(.*?)</a>",
        anchor_replacer,
        raw_with_breaks,
        flags=re.DOTALL,
    )
    html_markup = re.sub(r"<[^>]+>", "", html_markup)
    html_markup = html_lib.unescape(html_markup)
    html_markup = re.sub(
        r"(https?://[^\s<]+)",
        r'<a href="\1" target="_blank" rel="noopener noreferrer">\1</a>',
        html_markup,
    )
    for index, anchor in enumerate(anchors):
        html_markup = html_markup.replace(f"__ANCHOR_{index}__", anchor)
    html_markup, removed_url_anchors = strip_redundant_url_anchors(html_markup)
    html_markup = re.sub(r"(?<=[0-9A-Za-zА-Яа-яЁё«»„“\"'()])(?=<a\b)", " ", html_markup)
    html_markup = re.sub(r"(?<=</a>)(?=[0-9A-Za-zА-Яа-яЁё«»„“\"'(])", " ", html_markup)
    html_markup = html_markup.replace("\n", "<br>").strip() or None

    plain = re.sub(
        r"<a[^>]+href=\"([^\"]+)\"[^>]*>(.*?)</a>",
        lambda match: html_lib.unescape(re.sub(r"<[^>]+>", "", match.group(2))).strip() or html_lib.unescape(match.group(1)),
        raw_with_breaks,
        flags=re.DOTALL,
    )
    plain = re.sub(r"<[^>]+>", "", plain)
    plain = html_lib.unescape(plain).strip() or None
    plain = strip_redundant_urls_from_plain_text(plain, removed_url_anchors)
    if plain:
        plain = re.sub(r"(?<=[0-9A-Za-zА-Яа-яЁё«»„“\"'()])(?=https?://)", " ", plain)
        plain = re.sub(r"(https?://\S+)(?=[A-Za-zА-Яа-яЁё«»„“\"'(])", r"\1 ", plain)

    return plain, html_markup


def extract_div_inner_html_by_class(html_text: str, class_name: str, *, prefer_last: bool = False) -> str:
    open_tag_pattern = re.compile(
        rf'<div[^>]+class="[^"]*\b{re.escape(class_name)}\b[^"]*"[^>]*>',
        re.IGNORECASE,
    )
    open_tag_matches = list(open_tag_pattern.finditer(html_text))
    if not open_tag_matches:
        return ""

    if prefer_last:
        open_tag_matches = list(reversed(open_tag_matches))

    token_pattern = re.compile(r"<div\b[^>]*>|</div>", re.IGNORECASE)

    for open_tag_match in open_tag_matches:
        start_index = open_tag_match.end()
        depth = 1

        for token_match in token_pattern.finditer(html_text, start_index):
            token = token_match.group(0).lower()
            if token.startswith("</div"):
                depth -= 1
                if depth == 0:
                    return html_text[start_index:token_match.start()]
            else:
                depth += 1

        if start_index < len(html_text):
            return html_text[start_index:]

    return ""


def strip_anchor_block_by_class(html_text: str, class_name: str) -> str:
    open_tag_match = re.search(
        rf'<a[^>]+class="[^"]*\b{re.escape(class_name)}\b[^"]*"[^>]*>',
        html_text,
        re.IGNORECASE,
    )
    if not open_tag_match:
        return html_text

    close_tag_match = re.search(r"</a>", html_text[open_tag_match.end():], re.IGNORECASE)
    if not close_tag_match:
        return html_text

    end_index = open_tag_match.end() + close_tag_match.end()
    return f"{html_text[:open_tag_match.start()]}{html_text[end_index:]}"


def normalize_anchor_href(href: str | None) -> str | None:
    if not href:
        return None

    raw_value = html_lib.unescape(href).strip()
    if not raw_value:
        return None

    parsed = urlparse(raw_value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None

    path = parsed.path.rstrip("/") or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}{query}"


def is_url_like_label(label: str | None, href: str | None = None) -> bool:
    visible_text = html_lib.unescape(label or "").strip()
    if not visible_text:
        return False

    if re.match(r"^(?:https?://|www\.)\S+$", visible_text, re.IGNORECASE):
        return True

    normalized_label = normalize_anchor_href(visible_text)
    normalized_href = normalize_anchor_href(href)
    return bool(normalized_label and normalized_href and normalized_label == normalized_href)


def strip_redundant_url_anchors(html_markup: str | None) -> tuple[str | None, list[str]]:
    if not html_markup:
        return html_markup, []

    anchor_pattern = re.compile(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
    anchors: list[dict[str, Any]] = []

    for match in anchor_pattern.finditer(html_markup):
        href = html_lib.unescape(match.group(1)).strip()
        label = html_lib.unescape(re.sub(r"<[^>]+>", "", match.group(2))).strip() or href
        anchors.append(
            {
                "start": match.start(),
                "end": match.end(),
                "href": href,
                "label": label,
                "normalized_href": normalize_anchor_href(href),
            }
        )

    named_hrefs = {
        anchor["normalized_href"]
        for anchor in anchors
        if anchor["normalized_href"] and not is_url_like_label(anchor["label"], anchor["href"])
    }
    if not named_hrefs and not anchors:
        return html_markup, []

    cleaned_markup = html_markup
    removed_urls: list[str] = []

    for anchor in reversed(anchors):
        if not is_url_like_label(anchor["label"], anchor["href"]):
            continue

        prefix_markup = cleaned_markup[:anchor["start"]]
        following_markup = cleaned_markup[anchor["end"]:]
        has_attached_visible_text = bool(re.match(r"^[^\s<]", following_markup))
        has_named_duplicate = bool(anchor["normalized_href"] and anchor["normalized_href"] in named_hrefs)
        if not has_attached_visible_text and not has_named_duplicate:
            continue

        removed_urls.append(anchor["href"])
        should_restore_paragraph_break = bool(
            re.search(r"<br>\s*$", prefix_markup, re.IGNORECASE)
            and starts_new_block_markup(following_markup)
        )
        separator = "<br>" if should_restore_paragraph_break else ""
        cleaned_markup = f"{prefix_markup}{separator}{following_markup}"

    cleaned_markup = re.sub(r"(?:<br>){3,}", "<br><br>", cleaned_markup).strip() or None
    return cleaned_markup, removed_urls


def strip_redundant_urls_from_plain_text(plain_text: str | None, removed_urls: list[str]) -> str | None:
    if not plain_text or not removed_urls:
        return plain_text

    cleaned_text = plain_text
    for url in sorted({url for url in removed_urls if url}, key=len, reverse=True):
        escaped_url = re.escape(url)
        cleaned_text = re.sub(rf"(?<=\n){escaped_url}(?=[^\s])", "\n", cleaned_text)
        cleaned_text = re.sub(rf"{escaped_url}(?=[^\s])", "", cleaned_text)
        cleaned_text = re.sub(rf"(^|\n)\s*{escaped_url}\s*(?=\n|$)", r"\1", cleaned_text)

    cleaned_text = re.sub(r"\n{3,}", "\n\n", cleaned_text).strip() or None
    return cleaned_text


def starts_new_block_markup(markup: str | None) -> bool:
    if not markup:
        return False

    visible_markup = re.sub(r"^(?:\s|&nbsp;|<[^>]+>)+", "", markup, flags=re.IGNORECASE)
    return bool(re.match(r"^(?:▫️|🔘|📌|👉|#|[A-ZА-ЯЁ])", visible_markup))


def extract_external_links(text_html: str | None) -> list[str]:
    if not text_html:
        return []

    links: list[str] = []
    seen: set[str] = set()

    for href in re.findall(r'<a[^>]+href="([^"]+)"', text_html, re.IGNORECASE):
        normalized_href = html_lib.unescape(href).strip()
        if not normalized_href or not re.match(r"^https?://", normalized_href):
            continue

        hostname = (urlparse(normalized_href).hostname or "").lower()
        if hostname.endswith("t.me") or hostname.endswith("telegram.me"):
            continue

        if normalized_href in seen:
            continue

        seen.add(normalized_href)
        links.append(normalized_href)

    return links


def extract_preview_image_url(page_html: str, page_url: str) -> str | None:
    patterns = [
        r'<meta[^>]+property=["\']og:image:secure_url["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+itemprop=["\']image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<link[^>]+rel=["\']image_src["\'][^>]+href=["\']([^"\']+)["\']',
    ]

    for pattern in patterns:
        match = re.search(pattern, page_html, re.IGNORECASE)
        if match:
            return urljoin(page_url, html_lib.unescape(match.group(1)))

    return None


def extract_meta_content(
    page_html: str,
    *,
    property_names: tuple[str, ...] = (),
    name_names: tuple[str, ...] = (),
    itemprop_names: tuple[str, ...] = (),
) -> str | None:
    patterns: list[str] = []
    for name in property_names:
        patterns.extend(
            [
                rf'<meta[^>]+property=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
                rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']{re.escape(name)}["\']',
            ]
        )
    for name in name_names:
        patterns.extend(
            [
                rf'<meta[^>]+name=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
                rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']{re.escape(name)}["\']',
            ]
        )
    for name in itemprop_names:
        patterns.extend(
            [
                rf'<meta[^>]+itemprop=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
                rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+itemprop=["\']{re.escape(name)}["\']',
            ]
        )

    for pattern in patterns:
        match = re.search(pattern, page_html, re.IGNORECASE)
        if match:
            value = collapse_whitespace(html_lib.unescape(strip_tags(match.group(1))))
            if value:
                return value
    return None


def extract_page_title(page_html: str) -> str | None:
    match = re.search(r"<title[^>]*>(.*?)</title>", page_html, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return collapse_whitespace(html_lib.unescape(strip_tags(match.group(1))))


def normalize_host_label(hostname: str | None) -> str:
    host = (hostname or "").strip().lower()
    if not host:
        return ""
    return re.sub(r"^www\.", "", host)


def score_link_preview_candidate(url: str) -> int:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()
    score = 0

    if any(
        token in host
        for token in (
            "youtube.com",
            "youtu.be",
            "rutube.ru",
            "vkvideo.ru",
            "vimeo.com",
            "dzen.ru",
            "smotrim.ru",
            "twitch.tv",
            "tiktok.com",
        )
    ):
        score += 8

    if host.endswith("vk.com") and "/video" in path:
        score += 7

    if any(token in path for token in ("/video", "/watch", "/shorts", "/clip", "/reel", "/live")):
        score += 4

    if "v=" in query or "video" in query:
        score += 2

    return score


def select_link_preview_candidates(text_html: str | None) -> list[str]:
    links = extract_external_links(text_html)
    if not links:
        return []
    if len(links) == 1:
        return links

    ranked = sorted(
        enumerate(links),
        key=lambda item: (-score_link_preview_candidate(item[1]), item[0]),
    )
    selected = [url for _, url in ranked if score_link_preview_candidate(url) > 0][:MAX_LINK_PREVIEW_CANDIDATES]
    return selected or links[:1]


def extract_link_preview_metadata(page_html: str, page_url: str, link_url: str) -> dict[str, Any] | None:
    title = (
        extract_meta_content(page_html, property_names=("og:title",), name_names=("twitter:title",), itemprop_names=("headline",))
        or extract_page_title(page_html)
    )
    description = extract_meta_content(
        page_html,
        property_names=("og:description",),
        name_names=("description", "twitter:description"),
        itemprop_names=("description",),
    )
    site_name = extract_meta_content(
        page_html,
        property_names=("og:site_name",),
        name_names=("application-name",),
    ) or normalize_host_label(urlparse(link_url).hostname)
    image_url = extract_preview_image_url(page_html, page_url)
    video_marker = (
        extract_meta_content(
            page_html,
            property_names=("og:video", "og:video:url", "og:video:secure_url", "og:type"),
            name_names=("twitter:player",),
            itemprop_names=("embedURL",),
        )
        or ""
    )
    is_video = "video" in video_marker.lower() or score_link_preview_candidate(link_url) > 0

    if not title and not description and not image_url:
        return None

    metadata: dict[str, Any] = {
        "href": link_url,
        "title": shorten_text(title or site_name or normalize_host_label(urlparse(link_url).hostname), 120),
        "site_name": shorten_text(site_name, 48) if site_name else "",
        "host": normalize_host_label(urlparse(link_url).hostname),
        "is_video": bool(is_video),
    }
    if description:
        cleaned_description = shorten_text(description, 220)
        if cleaned_description and cleaned_description != metadata["title"]:
            metadata["description"] = cleaned_description
    if image_url:
        metadata["image_url"] = image_url
    return metadata


def normalize_link_preview_entry(preview: Any) -> dict[str, Any] | None:
    if not isinstance(preview, dict):
        return None

    href = str(preview.get("href") or "").strip()
    if not href or not re.match(r"^https?://", href, re.IGNORECASE):
        return None

    entry: dict[str, Any] = {
        "href": href,
        "title": shorten_text(preview.get("title") or normalize_host_label(urlparse(href).hostname), 120),
        "site_name": shorten_text(preview.get("site_name"), 48) if preview.get("site_name") else "",
        "host": normalize_host_label(preview.get("host") or urlparse(href).hostname),
        "is_video": bool(preview.get("is_video")),
    }
    description = preview.get("description")
    if description:
        entry["description"] = shorten_text(description, 220)

    image = normalize_photo_entry(preview.get("image"))
    if image:
        entry["image"] = image

    return entry


def remember_link_preview_assets(preview: dict[str, Any] | None, active_relative_paths: set[str]) -> None:
    image = normalize_photo_entry((preview or {}).get("image"))
    if not image:
        return

    for key in ("thumb_url", "feed_url", "full_url"):
        url = image.get(key)
        if is_local_asset_url(url):
            active_relative_paths.add(str(url))


def post_has_physical_media(post: dict[str, Any]) -> bool:
    photos = [normalize_photo_entry(photo) for photo in post.get("photos") or []]
    videos = normalize_video_entries(post.get("videos"))
    return bool([photo for photo in photos if photo]) or bool(post.get("video_url")) or bool(videos)


def get_image_dimensions(raw_bytes: bytes) -> tuple[int, int] | None:
    try:
        from PIL import Image

        with Image.open(io.BytesIO(raw_bytes)) as image:
            return image.size
    except Exception:
        return None


def preview_is_already_large_enough(current_bytes: bytes | None) -> bool:
    if not current_bytes:
        return False

    size = get_image_dimensions(current_bytes)
    if not size:
        return False

    width, height = size
    return max(width, height) >= MIN_EXTERNAL_OVERRIDE_WIDTH


def preview_needs_quality_help(current_bytes: bytes | None) -> bool:
    if not current_bytes:
        return True

    size = get_image_dimensions(current_bytes)
    if not size:
        return True

    width, height = size
    longest_side = max(width, height)
    shortest_side = max(min(width, height), 1)
    aspect_ratio = longest_side / shortest_side

    if longest_side < MIN_EXTERNAL_OVERRIDE_WIDTH:
        return True

    return aspect_ratio >= 1.9 and longest_side < 1400


def is_safe_external_preview_match(current_bytes: bytes | None, candidate_bytes: bytes | None) -> bool:
    if not candidate_bytes:
        return False

    candidate_size = get_image_dimensions(candidate_bytes)
    if not candidate_size:
        return False

    if not current_bytes:
        return True

    current_size = get_image_dimensions(current_bytes)
    if not current_size:
        return True

    current_width, current_height = current_size
    candidate_width, candidate_height = candidate_size
    current_ratio = current_width / max(current_height, 1)
    candidate_ratio = candidate_width / max(candidate_height, 1)
    ratio_delta = abs(math.log(max(candidate_ratio, 0.01) / max(current_ratio, 0.01)))

    if ratio_delta > MAX_EXTERNAL_OVERRIDE_RATIO_DELTA:
        return False

    current_area = current_width * current_height
    candidate_area = candidate_width * candidate_height
    return candidate_area >= current_area * MIN_EXTERNAL_OVERRIDE_RATIO_GAIN


def choose_better_preview_bytes(current_bytes: bytes | None, candidate_bytes: bytes | None) -> bytes | None:
    if not candidate_bytes:
        return None

    candidate_size = get_image_dimensions(candidate_bytes)
    if not candidate_size:
        return None

    if not current_bytes:
        return candidate_bytes

    current_size = get_image_dimensions(current_bytes)
    if not current_size:
        return candidate_bytes

    current_width, current_height = current_size
    candidate_width, candidate_height = candidate_size
    current_area = current_width * current_height
    candidate_area = candidate_width * candidate_height

    if candidate_area == current_area:
        if len(candidate_bytes) > len(current_bytes) * 1.35:
            return candidate_bytes
        return None

    if candidate_area <= current_area:
        return None

    current_ratio = current_width / max(current_height, 1)
    candidate_ratio = candidate_width / max(candidate_height, 1)
    ratio_delta = abs(math.log(max(candidate_ratio, 0.01) / max(current_ratio, 0.01)))

    if ratio_delta > 0.55 and candidate_area < current_area * 1.8:
        return None

    return candidate_bytes


def build_telegram_post_page_urls(tg_url: str) -> list[str]:
    if not tg_url.startswith("https://t.me/"):
        return []

    variants = [
        f"{tg_url}?embed=1&mode=tme",
        f"{tg_url}?embed=1",
        tg_url,
        f"{tg_url}?single",
        tg_url.replace("https://t.me/", "https://t.me/s/", 1),
        f"{tg_url.replace('https://t.me/', 'https://t.me/s/', 1)}?single",
    ]

    urls: list[str] = []
    seen: set[str] = set()
    for url in variants:
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)

    return urls


def build_post_media_page_urls(post: dict[str, Any]) -> list[str]:
    return build_telegram_post_page_urls((post.get("tg_url") or "").strip())


def extract_post_id_from_telegram_url(tg_url: str | None) -> int | None:
    normalized = normalize_telegram_post_url(str(tg_url or ""))
    if not normalized.startswith("https://t.me/"):
        return None

    parsed = urlparse(normalized)
    segments = [segment for segment in (parsed.path or "").split("/") if segment]
    if len(segments) >= 2 and segments[1].isdigit():
        return int(segments[1])
    return None


def extract_telegram_post_block(page_html: str, post_id: int) -> str | None:
    blocks = split_telegram_post_blocks(page_html)

    for block in blocks:
        id_match = re.search(r'tgme_widget_message_date[^>]*href="[^"]+/(\d+)"', block)
        if not id_match:
            continue

        if int(id_match.group(1)) == post_id:
            return block

    return None


def extract_telegram_page_photo_urls(page_html: str, page_url: str, post_id: int) -> list[str]:
    post_block = extract_telegram_post_block(page_html, post_id)
    if not post_block:
        return []

    urls = [
        urljoin(page_url, html_lib.unescape(url))
        for url in re.findall(r"tgme_widget_message_photo_wrap[^>]+url\('([^']+)'\)", post_block)
    ]
    if urls:
        return urls

    link_preview_match = re.search(r"link_preview_image[^>]+url\('([^']+)'\)", post_block)
    if link_preview_match:
        return [urljoin(page_url, html_lib.unescape(link_preview_match.group(1)))]

    return []


def fetch_telegram_post_page_override(post: dict[str, Any], current_bytes: bytes | None = None) -> bytes | None:
    photos = post.get("photos") or []
    if len(photos) != 1:
        return None

    for page_url in build_post_media_page_urls(post):
        try:
            page_html = fetch_page(page_url)
        except Exception as error:  # pragma: no cover - network/runtime path
            log.warning("Failed to fetch Telegram post page for post %s: %s", post.get("id"), error)
            continue

        photo_urls = extract_telegram_page_photo_urls(page_html, page_url, int(post.get("id") or 0))
        for photo_url in photo_urls:
            try:
                candidate_bytes = fetch_binary(photo_url)
            except Exception as error:  # pragma: no cover - network/runtime path
                log.warning("Failed to fetch Telegram post media for post %s: %s", post.get("id"), error)
                continue

            preferred_bytes = choose_better_preview_bytes(current_bytes, candidate_bytes)
            if preferred_bytes:
                log.info("Using dedicated Telegram post media for post %s from %s", post.get("id"), page_url)
                return preferred_bytes

    return None


def fetch_external_preview_override(post: dict[str, Any], current_bytes: bytes | None = None) -> bytes | None:
    if not ENABLE_EXTERNAL_PREVIEW_OVERRIDE:
        return None

    photos = post.get("photos") or []
    if len(photos) != 1:
        return None

    if not preview_needs_quality_help(current_bytes):
        return None

    links = extract_external_links(post.get("text_html"))
    if not links:
        return None

    for link_url in list(reversed(links))[:MAX_EXTERNAL_LINKS_TO_TRY]:
        hostname = (urlparse(link_url).hostname or "").lower()
        if hostname in FAILED_EXTERNAL_PREVIEW_HOSTS:
            continue

        try:
            page_html = fetch_page(
                link_url,
                timeout=EXTERNAL_PAGE_TIMEOUT_SECONDS,
                retry_delays=FAST_EXTERNAL_RETRY_DELAYS,
                log_failures=False,
            )
        except Exception as error:  # pragma: no cover - network/runtime path
            if hostname:
                FAILED_EXTERNAL_PREVIEW_HOSTS.add(hostname)
            log.warning("Failed to fetch external page for post %s: %s", post.get("id"), error)
            continue

        preview_url = extract_preview_image_url(page_html, link_url)
        if not preview_url:
            continue

        try:
            candidate_bytes = fetch_binary(
                preview_url,
                timeout=EXTERNAL_IMAGE_TIMEOUT_SECONDS,
                retry_delays=FAST_EXTERNAL_RETRY_DELAYS,
                log_failures=False,
            )
        except Exception as error:  # pragma: no cover - network/runtime path
            preview_host = (urlparse(preview_url).hostname or "").lower()
            if preview_host:
                FAILED_EXTERNAL_PREVIEW_HOSTS.add(preview_host)
            log.warning("Failed to fetch external preview image for post %s: %s", post.get("id"), error)
            continue

        if not is_safe_external_preview_match(current_bytes, candidate_bytes):
            continue

        preferred_bytes = choose_better_preview_bytes(current_bytes, candidate_bytes)
        if preferred_bytes:
            log.info("Using external preview image for post %s from %s", post.get("id"), link_url)
            return preferred_bytes

    return None


def get_current_preview_bytes(post: dict[str, Any], override_bytes: bytes | None = None) -> bytes | None:
    if override_bytes:
        return override_bytes

    photos = [normalize_photo_entry(photo) for photo in post.get("photos") or []]
    photos = [photo for photo in photos if photo]
    if len(photos) != 1:
        return None

    source_url = photos[0]["full_url"]
    try:
        return fetch_binary(source_url)
    except Exception as error:  # pragma: no cover - network/runtime path
        log.warning("Failed to fetch current preview image for post %s: %s", post.get("id"), error)
        return None


def extract_forwarded_source(block: str) -> dict[str, str] | None:
    if "Переслано из" not in block and "forwarded" not in block.lower():
        return None

    source_url = None
    for pattern in (
        r'tgme_widget_message_forwarded_from[^>]*>[\s\S]*?href="([^"]+)"',
        r'Переслано из:?[\s\S]{0,800}?href="([^"]+)"',
        r'Переслано из:?[\s\S]{0,800}?(https?://t\.me/[^\s"<]+)',
    ):
        match = re.search(pattern, block, re.IGNORECASE)
        if match:
            source_url = urljoin("https://t.me", html_lib.unescape(match.group(1)).strip())
            break

    if not source_url:
        return None

    username_match = re.search(r"https?://t\.me/(?:s/)?([^/?#]+)/?", source_url, re.IGNORECASE)
    if not username_match:
        return {
            "source_url": source_url,
            "channel_url": source_url,
        }

    channel_username = username_match.group(1)
    channel_url = f"https://t.me/s/{channel_username}"

    channel_title = None
    title_patterns = (
        r'tgme_widget_message_forwarded_from[^>]*>[\s\S]*?<a[^>]*>(.*?)</a>',
        r'Переслано из:?[\s\S]{0,300}?<a[^>]*>(.*?)</a>',
    )
    for pattern in title_patterns:
        match = re.search(pattern, block, re.IGNORECASE | re.DOTALL)
        if not match:
            continue

        candidate = re.sub(r"<[^>]+>", "", match.group(1))
        candidate = html_lib.unescape(candidate).strip()
        if candidate and "t.me/" not in candidate.lower() and "переслано из" not in candidate.lower():
            channel_title = candidate
            break

    result = {
        "source_url": source_url,
        "channel_url": channel_url,
        "channel_username": channel_username,
    }
    if channel_title:
        result["channel_title"] = channel_title

    return result


def split_telegram_post_blocks(html_text: str) -> list[str]:
    if 'class="tgme_widget_message_wrap' in html_text:
        return re.split(r'(?=<div class="tgme_widget_message_wrap)', html_text)
    return re.split(r'(?=<div class="tgme_widget_message\b)', html_text)


def extract_reply_reference(block: str, config: SiteConfig) -> dict[str, Any] | None:
    reply_match = re.search(
        r'<a[^>]+class="[^"]*tgme_widget_message_reply[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
        block,
        re.IGNORECASE | re.DOTALL,
    )
    if not reply_match:
        return None

    reply_url = urljoin("https://t.me", html_lib.unescape(reply_match.group(1)).strip())
    target_match = re.search(r"https?://t\.me/(?:s/)?([^/?#]+)/(\d+)", reply_url, re.IGNORECASE)
    if not target_match:
        return None

    channel_username = target_match.group(1)
    post_id = int(target_match.group(2))
    reply_markup = reply_match.group(2)
    title = None

    for pattern in (
        r'tgme_widget_message_reply_text[^>]*>(.*?)</div>',
        r'tgme_widget_message_reply_text[^>]*>(.*?)</span>',
        r'tgme_widget_message_reply_title[^>]*>(.*?)</div>',
        r'tgme_widget_message_reply_title[^>]*>(.*?)</span>',
    ):
        title_match = re.search(pattern, reply_markup, re.IGNORECASE | re.DOTALL)
        if not title_match:
            continue

        candidate = shorten_text(strip_tags(html_lib.unescape(title_match.group(1))), 110)
        if candidate:
            title = candidate
            break

    if not title:
        fallback_title = shorten_text(strip_tags(html_lib.unescape(reply_markup)), 110)
        if fallback_title:
            title = fallback_title

    return {
        "post_id": post_id,
        "title": title or f"пост #{post_id}",
        "tg_url": f"https://t.me/{channel_username}/{post_id}",
        "channel_username": channel_username or config.channel_username,
    }


def parse_posts(html_text: str, config: SiteConfig) -> list[dict[str, Any]]:
    blocks = split_telegram_post_blocks(html_text)
    posts: list[dict[str, Any]] = []

    for block in blocks:
        id_match = re.search(r'tgme_widget_message_date[^>]*href="[^"]+/(\d+)"', block)
        if not id_match:
            continue

        post_id = int(id_match.group(1))
        date_match = re.search(r'<time[^>]+datetime="([^"]+)"', block)
        views_match = re.search(r'tgme_widget_message_views[^>]*>([^<]+)<', block)
        comments_count_match = re.search(r'tgme_widget_message_replies_count[^>]*>([^<]*)<', block)
        comments_link_match = re.search(r'tgme_widget_message_replies[^>]*href="([^"]+)"', block)
        photos = [
            urljoin("https://t.me", html_lib.unescape(url))
            for url in re.findall(r"tgme_widget_message_photo_wrap[^>]+url\('([^']+)'\)", block)
        ]
        standard_videos = extract_standard_video_entries_from_html(block)
        primary_video = standard_videos[0] if standard_videos else None
        video_width = primary_video.get("width") if primary_video else None
        video_height = primary_video.get("height") if primary_video else None
        if not video_width or not video_height:
            video_width, video_height = extract_video_dimensions_from_html(block)
        video_media_hint = bool(primary_video) or detect_video_media_hint(block)
        video_note = detect_round_video_hint(block, video_width, video_height) if video_media_hint else False
        video_url = primary_video.get("url") if (primary_video and video_note) else None
        videos = [] if video_note else standard_videos
        reply_to = extract_reply_reference(block, config)
        text_source_block = strip_anchor_block_by_class(block, "tgme_widget_message_reply") if reply_to else block
        raw_text = extract_div_inner_html_by_class(
            text_source_block,
            "tgme_widget_message_text",
            prefer_last=bool(reply_to),
        )
        text, text_html = build_text_fields(raw_text)
        forwarded_from = extract_forwarded_source(block)

        if not text and not photos and not videos and not video_url and not video_media_hint and not select_link_preview_candidates(text_html):
            continue

        comments_url = None
        if comments_link_match:
            comments_url = urljoin("https://t.me", html_lib.unescape(comments_link_match.group(1)))

        entry = {
            "id": post_id,
            "date": date_match.group(1) if date_match else None,
            "text": text,
            "text_html": text_html,
            "views": parse_count(views_match.group(1) if views_match else None),
            "comments_count": parse_count(comments_count_match.group(1) if comments_count_match else None),
            "comments_url": comments_url,
            "photos": photos,
            "video_url": video_url,
            "videos": videos,
            "tg_url": f"https://t.me/{config.channel_username}/{post_id}",
            "forwarded_from": forwarded_from,
            "reply_to": reply_to,
        }
        if video_note:
            entry["video_note"] = True
            if primary_video and primary_video.get("poster"):
                entry["video_poster"] = primary_video["poster"]
        if video_width and video_height:
            entry["video_width"] = video_width
            entry["video_height"] = video_height

        posts.append(entry)

    return posts


def collect_posts(config: SiteConfig, initial_page_html: str | None = None) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    before_id: int | None = None
    now = datetime.now(timezone.utc)
    cutoff = subtract_months(now, config.recent_posts_months)
    first_iteration = True

    while len(posts) < config.messages_limit:
        url = config.channel_web_url if before_id is None else f"{config.channel_web_url}?before={before_id}"
        if first_iteration and initial_page_html is not None:
            page_html = initial_page_html
            log.info("Using prefetched channel page %s", url)
        else:
            log.info("Fetching %s", url)
            try:
                page_html = fetch_page(url)
            except Exception as error:  # pragma: no cover - network/runtime path
                log.warning("Failed to fetch paginated channel page %s: %s", url, error)
                break
        page_posts = parse_posts(page_html, config)
        if not page_posts:
            break

        added = 0
        page_reached_cutoff = False
        for post in page_posts:
            post_date = parse_iso_datetime(post.get("date"))
            if post_date and post_date < cutoff:
                page_reached_cutoff = True
                continue
            if post["id"] in seen_ids:
                continue
            posts.append(post)
            seen_ids.add(post["id"])
            added += 1
            if len(posts) >= config.messages_limit:
                break

        if added == 0 and page_reached_cutoff:
            break

        if added == 0:
            break

        before_id = min(post["id"] for post in page_posts)
        oldest_page_date = min(
            (parse_iso_datetime(post.get("date")) for post in page_posts if parse_iso_datetime(post.get("date"))),
            default=None,
        )
        if oldest_page_date and oldest_page_date < cutoff:
            break
        if len(page_posts) < 5:
            break
        first_iteration = False
        time.sleep(1)

    posts = [
        post
        for post in posts
        if (post_date := parse_iso_datetime(post.get("date"))) is None or post_date >= cutoff
    ]
    posts.sort(key=lambda post: post["date"] or "", reverse=True)
    log.info("Collected %s posts from the last %s months", len(posts), config.recent_posts_months)
    return posts[: config.messages_limit]


def get_highest_post_id(posts: list[dict[str, Any]] | None) -> int:
    ids = [int(post.get("id") or 0) for post in (posts or [])]
    return max(ids, default=0)


def probe_newer_posts_from_direct_pages(
    config: SiteConfig,
    posts: list[dict[str, Any]],
    *,
    existing_top_post_id: int = 0,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    current_top_post_id = get_highest_post_id(posts)
    if existing_top_post_id <= 0 and current_top_post_id <= 0:
        return posts

    start_post_id = max(existing_top_post_id, current_top_post_id)
    stale_root_detected = bool(existing_top_post_id and current_top_post_id <= existing_top_post_id)
    max_probes = (
        DIRECT_POST_PROBE_STALE_MAX_IDS
        if existing_top_post_id and current_top_post_id <= existing_top_post_id
        else DIRECT_POST_PROBE_FOLLOWUP_MAX_IDS
    )
    if max_probes <= 0:
        return posts

    if stale_root_detected:
        log.warning(
            "Latest collected post for @%s did not advance beyond %s. Probing direct Telegram post pages.",
            config.channel_username,
            existing_top_post_id,
        )

    discovered: list[dict[str, Any]] = []
    seen_ids = {int(post.get("id") or 0) for post in posts}
    consecutive_misses = 0
    direct_probe_stale_candidates: list[str] = []
    direct_probe_hard_errors = 0
    direct_probe_fetch_successes = 0

    for candidate_id in range(start_post_id + 1, start_post_id + max_probes + 1):
        candidate_post = None
        observed_ids: set[int] = set()
        candidate_urls = (
            f"https://t.me/{config.channel_username}/{candidate_id}?embed=1&mode=tme",
            f"https://t.me/{config.channel_username}/{candidate_id}?embed=1",
        )
        not_found_count = 0
        for candidate_url in candidate_urls:
            try:
                page_html = fetch_page(
                    candidate_url,
                    timeout=DIRECT_POST_PROBE_TIMEOUT_SECONDS,
                    retry_delays=DIRECT_POST_PROBE_FETCH_RETRY_DELAYS,
                    log_failures=stale_root_detected,
                )
                direct_probe_fetch_successes += 1
            except HTTPError as error:
                if error.code == 404:
                    not_found_count += 1
                    continue
                direct_probe_hard_errors += 1
                continue
            except Exception:
                direct_probe_hard_errors += 1
                continue

            parsed_posts = parse_posts(page_html, config)
            observed_ids.update(int(post.get("id") or 0) for post in parsed_posts if int(post.get("id") or 0))
            candidate_post = next(
                (
                    post
                    for post in parsed_posts
                    if int(post.get("id") or 0) == candidate_id
                ),
                None,
            )
            if candidate_post:
                break

        if not candidate_post:
            if candidate_id == start_post_id + 1 and not_found_count == len(candidate_urls):
                if stale_root_detected:
                    log.warning(
                        "Direct probe for @%s did not find post %s, but the root page is stale. "
                        "Continuing probe to account for gaps in Telegram post ids.",
                        config.channel_username,
                        candidate_id,
                    )
                else:
                    log.info("No newer direct Telegram posts detected after %s for @%s.", start_post_id, config.channel_username)
                    break
            if observed_ids and max(observed_ids, default=0) < candidate_id:
                direct_probe_stale_candidates.append(f"{candidate_id}:{','.join(str(item) for item in sorted(observed_ids)[-4:])}")
            consecutive_misses += 1
            if consecutive_misses >= DIRECT_POST_PROBE_MAX_CONSECUTIVE_MISSES:
                break
            continue

        candidate_date = parse_iso_datetime(candidate_post.get("date"))
        if candidate_date and candidate_date < cutoff:
            consecutive_misses += 1
            if consecutive_misses >= DIRECT_POST_PROBE_MAX_CONSECUTIVE_MISSES:
                break
            continue

        if candidate_id in seen_ids:
            consecutive_misses = 0
            continue

        discovered.append(candidate_post)
        seen_ids.add(candidate_id)
        consecutive_misses = 0

    if not discovered:
        if stale_root_detected and existing_top_post_id and current_top_post_id < existing_top_post_id:
            stale_tail = ", ".join(direct_probe_stale_candidates[:4]) or "n/a"
            raise RuntimeError(
                f"Direct probe for @{config.channel_username} did not recover newer posts "
                f"after the top post regressed from {existing_top_post_id} to {current_top_post_id} "
                f"(stale candidates: {stale_tail}; hard_errors={direct_probe_hard_errors})"
            )
        if stale_root_detected and (direct_probe_stale_candidates or (direct_probe_hard_errors and direct_probe_fetch_successes == 0)):
            stale_tail = ", ".join(direct_probe_stale_candidates[:4]) or "n/a"
            log.warning(
                "Direct probe for @%s did not confirm newer posts; keeping current top %s "
                "(stale candidates: %s; hard_errors=%s)",
                config.channel_username,
                current_top_post_id,
                stale_tail,
                direct_probe_hard_errors,
            )
        return posts

    log.info(
        "Discovered %s newer posts via direct Telegram post probes: %s",
        len(discovered),
        ", ".join(str(int(post.get("id") or 0)) for post in discovered),
    )
    combined_posts = posts + discovered
    combined_posts.sort(key=lambda post: post.get("date") or "", reverse=True)
    return combined_posts[: config.messages_limit]


def select_posts_for_comment_refresh(posts: list[dict[str, Any]], config: SiteConfig) -> list[int]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=config.comments_max_age_days)
    selected: list[int] = []

    for index, post in enumerate(posts):
        post_date = None
        if post.get("date"):
            try:
                post_date = datetime.fromisoformat(post["date"])
            except ValueError:
                post_date = None

        if index < config.comments_posts_limit or (post_date and post_date >= cutoff):
            selected.append(post["id"])

    return selected


def select_posts_for_high_res_media(posts: list[dict[str, Any]]) -> list[int]:
    selected: list[int] = []
    for index, post in enumerate(posts):
        if index >= FEED_PAGE_SIZE * 3:
            break
        if post.get("video_url"):
            continue
        photos = post.get("photos") or []
        if not photos:
            continue
        selected.append(post["id"])
    return selected


def chunked(sequence: list[int], size: int) -> list[list[int]]:
    return [sequence[index:index + size] for index in range(0, len(sequence), size)]


def extract_reply_target_post_id(message: Any) -> int | None:
    reply_header = getattr(message, "reply_to", None)
    if not reply_header:
        return None

    for attribute in ("reply_to_msg_id", "reply_to_top_id"):
        candidate = getattr(reply_header, attribute, None)
        if isinstance(candidate, int) and candidate > 0:
            return candidate

    return None


def build_reply_reference_title(
    target_post: dict[str, Any] | None = None,
    target_message: Any | None = None,
    fallback_post_id: int | None = None,
) -> str:
    def extract_compact_label(value: str | None) -> str | None:
        source = (value or "").replace("\r", "\n")
        if not source.strip():
            return None

        paragraphs = [
            collapse_whitespace(part)
            for part in re.split(r"\n\s*\n+", source)
            if collapse_whitespace(part)
        ]
        candidate = paragraphs[0] if paragraphs else collapse_whitespace(source)
        return shorten_text(candidate, 120) if candidate else None

    if target_post:
        candidate = extract_compact_label(target_post.get("text") or strip_tags(target_post.get("text_html") or ""))
        if candidate:
            return candidate

    if target_message:
        candidate = extract_compact_label(getattr(target_message, "message", None) or "")
        if candidate:
            return candidate

    if fallback_post_id is not None:
        return f"пост #{fallback_post_id}"

    return "исходный пост"


def build_reply_target_page_urls(reply_reference: dict[str, Any], config: SiteConfig) -> list[str]:
    tg_url = (reply_reference.get("tg_url") or "").strip()
    channel_username = (reply_reference.get("channel_username") or config.channel_username or "").strip()
    post_id = reply_reference.get("post_id")
    if not tg_url and channel_username and post_id:
        tg_url = f"https://t.me/{channel_username}/{post_id}"
    if not tg_url.startswith("https://t.me/"):
        return []

    variants = [
        tg_url,
        f"{tg_url}?single",
        tg_url.replace("https://t.me/", "https://t.me/s/", 1),
        f"{tg_url.replace('https://t.me/', 'https://t.me/s/', 1)}?single",
    ]

    urls: list[str] = []
    seen: set[str] = set()
    for url in variants:
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def fetch_reply_reference_from_page(reply_reference: dict[str, Any], config: SiteConfig) -> dict[str, Any] | None:
    post_id = int(reply_reference.get("post_id") or 0)
    if post_id <= 0:
        return None

    for page_url in build_reply_target_page_urls(reply_reference, config):
        try:
            page_html = fetch_page(page_url, timeout=10, retry_delays=FAST_EXTERNAL_RETRY_DELAYS, log_failures=False)
        except Exception:
            continue

        for post in parse_posts(page_html, config):
            if int(post.get("id") or 0) != post_id:
                continue

            title = build_reply_reference_title(post, fallback_post_id=post_id)
            return {
                "post_id": post_id,
                "title": title,
                "tg_url": reply_reference.get("tg_url") or post.get("tg_url") or f"https://t.me/{config.channel_username}/{post_id}",
                "channel_username": reply_reference.get("channel_username") or config.channel_username,
            }

    return None


def should_enrich_reply_post_from_page(post: dict[str, Any]) -> bool:
    if not post.get("reply_to"):
        return False

    text = collapse_whitespace(post.get("text"))
    reply_title = collapse_whitespace((post.get("reply_to") or {}).get("title"))
    if not text or not post.get("photos"):
        return True

    if not reply_title:
        return False

    compact_text = text.rstrip("…").strip()
    compact_reply = reply_title.rstrip("…").strip()
    if not compact_text or not compact_reply:
        return False

    return compact_text == compact_reply or compact_text.startswith(compact_reply)


def enrich_reply_posts_from_pages(posts: list[dict[str, Any]], config: SiteConfig) -> bool:
    reply_posts = [post for post in posts if should_enrich_reply_post_from_page(post)]
    if not reply_posts:
        return False

    changed = False
    log.info("Enriching %s reply posts from Telegram post pages", len(reply_posts))

    for post in reply_posts:
        post_id = int(post.get("id") or 0)
        if post_id <= 0:
            continue

        enriched_post = None
        for page_url in build_post_media_page_urls(post):
            try:
                page_html = fetch_page(page_url, timeout=10, retry_delays=FAST_EXTERNAL_RETRY_DELAYS, log_failures=False)
            except Exception:
                continue

            for candidate in parse_posts(page_html, config):
                if int(candidate.get("id") or 0) == post_id:
                    enriched_post = candidate
                    break

            if enriched_post:
                break

        if not enriched_post:
            continue

        if enriched_post.get("text") and enriched_post.get("text") != post.get("text"):
            post["text"] = enriched_post.get("text")
            changed = True

        if enriched_post.get("text_html") and enriched_post.get("text_html") != post.get("text_html"):
            post["text_html"] = enriched_post.get("text_html")
            changed = True

        if enriched_post.get("photos") and enriched_post.get("photos") != post.get("photos"):
            post["photos"] = enriched_post.get("photos")
            changed = True

        if enriched_post.get("video_url") and enriched_post.get("video_url") != post.get("video_url"):
            post["video_url"] = enriched_post.get("video_url")
            changed = True

        if enriched_post.get("videos") and enriched_post.get("videos") != post.get("videos"):
            post["videos"] = enriched_post.get("videos")
            changed = True

        if enriched_post.get("video_poster") and enriched_post.get("video_poster") != post.get("video_poster"):
            post["video_poster"] = enriched_post.get("video_poster")
            changed = True

        if enriched_post.get("video_note") and not post.get("video_note"):
            post["video_note"] = True
            changed = True

        if enriched_post.get("video_width") and enriched_post.get("video_height"):
            if (
                enriched_post.get("video_width") != post.get("video_width")
                or enriched_post.get("video_height") != post.get("video_height")
            ):
                post["video_width"] = enriched_post.get("video_width")
                post["video_height"] = enriched_post.get("video_height")
                changed = True

    return changed


def enrich_forwarded_posts_from_source_pages(posts: list[dict[str, Any]]) -> bool:
    forwarded_posts = [
        post
        for post in posts
        if post.get("forwarded_from")
        and not post.get("video_note")
        and not (post.get("videos") or [])
    ]
    if not forwarded_posts:
        return False

    changed = False
    log.info("Enriching %s forwarded posts from forwarded source pages", len(forwarded_posts))

    for post in forwarded_posts:
        source_url = normalize_forwarded_source_url(post)
        source_post_id = extract_post_id_from_telegram_url(source_url)
        if not source_url or not source_post_id:
            continue

        enriched_videos: list[dict[str, Any]] = []
        for page_url in build_telegram_post_page_urls(source_url):
            try:
                page_html = fetch_page(page_url, timeout=10, retry_delays=FAST_EXTERNAL_RETRY_DELAYS, log_failures=False)
            except Exception:
                continue

            source_block = extract_telegram_post_block(page_html, source_post_id)
            if not source_block:
                continue

            enriched_videos = extract_standard_video_entries_from_html(source_block)
            if enriched_videos:
                break

        if enriched_videos and enriched_videos != post.get("videos"):
            post["videos"] = enriched_videos
            changed = True

    return changed


def get_downloadable_photo_targets(message: Any) -> list[Any]:
    targets: list[Any] = []

    if getattr(message, "photo", None):
        targets.append(message.photo)

    webpage = getattr(getattr(message, "media", None), "webpage", None)
    if webpage and getattr(webpage, "photo", None):
        targets.append(webpage.photo)

    return targets


def is_standard_video_message(message: Any) -> bool:
    if is_round_video_message(message):
        return False

    document = getattr(getattr(message, "media", None), "document", None)
    mime_type = (getattr(document, "mime_type", None) or "").lower()
    if mime_type.startswith("video/"):
        return True

    for attribute in getattr(document, "attributes", []) or []:
        if all(getattr(attribute, key, None) is not None for key in ("w", "h", "duration")):
            return True

    return False


def is_round_video_message(message: Any) -> bool:
    try:
        if bool(getattr(message, "video_note", None)):
            return True
    except Exception:
        pass

    document = getattr(getattr(message, "media", None), "document", None)
    for attribute in getattr(document, "attributes", []) or []:
        if getattr(attribute, "round_message", False):
            return True
    return False


def get_message_video_dimensions(message: Any) -> tuple[int | None, int | None]:
    document = getattr(getattr(message, "media", None), "document", None)
    for attribute in getattr(document, "attributes", []) or []:
        width = getattr(attribute, "w", None)
        height = getattr(attribute, "h", None)
        if isinstance(width, int) and width > 0 and isinstance(height, int) and height > 0:
            return width, height
    return None, None


def get_message_video_extension(message: Any) -> str:
    document = getattr(getattr(message, "media", None), "document", None)
    mime_type = (getattr(document, "mime_type", None) or "").lower()
    return {
        "video/mp4": "mp4",
        "video/webm": "webm",
        "video/quicktime": "mov",
    }.get(mime_type, "mp4")


async def download_message_video_poster_bytes(client: Any, message: Any) -> bytes | None:
    for thumb in (-1, 0):
        try:
            poster_bytes = await client.download_media(message, file=bytes, thumb=thumb)
            if poster_bytes:
                return poster_bytes
        except Exception:
            continue
    return None


def build_api_photo_placeholders(post_id: int, count: int) -> list[dict[str, str]]:
    placeholders: list[dict[str, str]] = []
    for index in range(count):
        marker = f"api-photo-placeholder/{post_id}-{index + 1}.jpg"
        placeholders.append({
            "thumb_url": marker,
            "feed_url": marker,
            "full_url": marker,
        })
    return placeholders


def get_message_comments_count(message: Any) -> int:
    replies = getattr(message, "replies", None)
    return int(getattr(replies, "replies", 0) or 0)


def build_post_from_api_message(
    message: Any,
    config: SiteConfig,
    *,
    photo_count: int = 0,
    video_count: int = 0,
    video_note: bool = False,
    video_width: int | None = None,
    video_height: int | None = None,
) -> dict[str, Any] | None:
    post_id = int(getattr(message, "id", 0) or 0)
    if post_id <= 0:
        return None

    text = getattr(message, "message", None) or None
    comments_count = get_message_comments_count(message)
    message_date = getattr(message, "date", None)
    if isinstance(message_date, datetime):
        date_value = message_date.astimezone(timezone.utc).isoformat()
    else:
        date_value = None

    entry: dict[str, Any] = {
        "id": post_id,
        "date": date_value,
        "text": text,
        "text_html": None,
        "views": int(getattr(message, "views", 0) or 0),
        "comments_count": comments_count,
        "comments_url": f"https://t.me/{config.channel_username}/{post_id}?comment=1" if comments_count > 0 else None,
        "photos": build_api_photo_placeholders(post_id, photo_count) if photo_count > 0 else [],
        "video_url": None,
        "videos": [],
        "tg_url": f"https://t.me/{config.channel_username}/{post_id}",
        "forwarded_from": None,
        "reply_to": None,
    }

    if video_note:
        entry["video_note"] = True
    if video_width and video_height:
        entry["video_width"] = video_width
        entry["video_height"] = video_height

    if not entry["text"] and not entry["photos"] and video_count <= 0 and not entry.get("video_note"):
        return None

    return entry


async def recover_newer_posts_from_api(
    config: SiteConfig,
    posts: list[dict[str, Any]],
    *,
    cutoff: datetime,
) -> tuple[list[dict[str, Any]], dict[int, list[bytes]], int | None]:
    credentials = get_telegram_session_credentials()
    if not credentials:
        return posts, {}, None

    current_top_post_id = get_highest_post_id(posts)
    if current_top_post_id <= 0:
        return posts, {}, None

    api_id, api_hash, session_string = credentials
    known_ids = {int(post.get("id") or 0) for post in posts}
    recovered_posts: list[dict[str, Any]] = []
    photo_overrides: dict[int, list[bytes]] = {}
    latest_api_post_id: int | None = None

    from telethon import TelegramClient
    from telethon.sessions import StringSession

    async with TelegramClient(StringSession(session_string), int(api_id), api_hash) as client:
        if not await client.is_user_authorized():
            raise RuntimeError("TELEGRAM_SESSION_STR is not authorized.")

        channel = await client.get_entity(config.channel_username)
        recent_messages: list[Any] = []
        max_scan = max(config.messages_limit * 2, 80)

        async for message in client.iter_messages(channel, limit=max_scan):
            if not message or not getattr(message, "id", None):
                continue

            message_date = getattr(message, "date", None)
            if isinstance(message_date, datetime) and message_date.astimezone(timezone.utc) < cutoff:
                break

            recent_messages.append(message)

        if not recent_messages:
            return posts, {}, None

        latest_api_post_id = max(int(getattr(message, "id", 0) or 0) for message in recent_messages)
        if latest_api_post_id <= current_top_post_id:
            return posts, {}, latest_api_post_id

        grouped_candidates: dict[int, list[Any]] = {}
        standalone_candidates: list[Any] = []
        for message in recent_messages:
            message_id = int(getattr(message, "id", 0) or 0)
            if message_id <= current_top_post_id or message_id in known_ids:
                continue

            grouped_id = getattr(message, "grouped_id", None)
            if grouped_id:
                grouped_candidates.setdefault(int(grouped_id), []).append(message)
            else:
                standalone_candidates.append(message)

        for message in standalone_candidates:
            photo_targets = get_downloadable_photo_targets(message)
            downloaded_photos: list[bytes] = []
            if photo_targets:
                for target in photo_targets:
                    raw_bytes = await client.download_media(target, file=bytes)
                    if raw_bytes:
                        downloaded_photos.append(raw_bytes)

            is_round_video = is_round_video_message(message)
            standard_video_count = 1 if is_standard_video_message(message) else 0
            video_width, video_height = get_message_video_dimensions(message)
            recovered_post = build_post_from_api_message(
                message,
                config,
                photo_count=len(downloaded_photos),
                video_count=standard_video_count,
                video_note=is_round_video,
                video_width=video_width,
                video_height=video_height,
            )
            if not recovered_post:
                continue

            if downloaded_photos:
                photo_overrides[int(recovered_post["id"])] = downloaded_photos
            recovered_posts.append(recovered_post)
            await asyncio.sleep(0.05)

        for grouped_messages in grouped_candidates.values():
            ordered_group = sorted(grouped_messages, key=lambda message: int(getattr(message, "id", 0) or 0))
            anchor_message = next(
                (message for message in ordered_group if (getattr(message, "message", None) or "").strip()),
                ordered_group[0],
            )

            downloaded_photos: list[bytes] = []
            standard_video_count = 0
            for message in ordered_group:
                for target in get_downloadable_photo_targets(message):
                    raw_bytes = await client.download_media(target, file=bytes)
                    if raw_bytes:
                        downloaded_photos.append(raw_bytes)
                if is_standard_video_message(message):
                    standard_video_count += 1

            recovered_post = build_post_from_api_message(
                anchor_message,
                config,
                photo_count=len(downloaded_photos),
                video_count=standard_video_count,
            )
            if not recovered_post:
                continue

            if downloaded_photos:
                photo_overrides[int(recovered_post["id"])] = downloaded_photos
            recovered_posts.append(recovered_post)
            await asyncio.sleep(0.05)

    if not recovered_posts:
        return posts, photo_overrides, latest_api_post_id

    log.info(
        "Recovered %s newer posts from Telegram API: %s",
        len(recovered_posts),
        ", ".join(str(int(post.get("id") or 0)) for post in sorted(recovered_posts, key=lambda item: int(item.get("id") or 0))),
    )
    combined_posts = posts + recovered_posts
    combined_posts.sort(key=lambda post: post.get("date") or "", reverse=True)
    return combined_posts[: config.messages_limit], photo_overrides, latest_api_post_id


async def fetch_video_metadata_for_posts(config: SiteConfig, posts: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    credentials = get_telegram_session_credentials()
    if not credentials:
        log.info("Telegram user session is not configured. Video media sync skipped.")
        return {}

    def should_fetch_video_metadata(post: dict[str, Any]) -> bool:
        if not post.get("id"):
            return False
        if post.get("video_note") or post.get("video_url"):
            return True
        if post.get("videos"):
            return False
        if post.get("forwarded_from") and (post.get("photos") or []):
            return True
        return False

    post_ids = [int(post["id"]) for post in posts if should_fetch_video_metadata(post)]
    if not post_ids:
        return {}

    api_id, api_hash, session_string = credentials
    results: dict[int, dict[str, Any]] = {}

    from telethon import TelegramClient
    from telethon.sessions import StringSession

    async with TelegramClient(StringSession(session_string), int(api_id), api_hash) as client:
        if not await client.is_user_authorized():
            raise RuntimeError("TELEGRAM_SESSION_STR is not authorized.")

        channel = await client.get_entity(config.channel_username)

        grouped_messages_cache: dict[int, list[Any]] = {}

        for batch in chunked(post_ids, 100):
            for message in await client.get_messages(channel, ids=batch):
                if not message or not getattr(message, "id", None):
                    continue

                is_round_video = is_round_video_message(message)
                grouped_id = getattr(message, "grouped_id", None)
                related_messages = [message]
                if grouped_id:
                    grouped_id = int(grouped_id)
                    related_messages = grouped_messages_cache.get(grouped_id) or []
                    if not related_messages:
                        ids = list(range(max(1, int(message.id) - 10), int(message.id) + 11))
                        neighbours = await client.get_messages(channel, ids=ids)
                        related_messages = sorted(
                            [
                                neighbour
                                for neighbour in neighbours
                                if neighbour and getattr(neighbour, "grouped_id", None) == grouped_id
                            ],
                            key=lambda item: int(getattr(item, "id", 0) or 0),
                        )
                        if not related_messages:
                            related_messages = [message]
                        grouped_messages_cache[grouped_id] = related_messages

                video_width, video_height = get_message_video_dimensions(message)
                attached_videos: list[dict[str, Any]] = []
                if not is_round_video:
                    for related_message in related_messages:
                        if not is_standard_video_message(related_message):
                            continue

                        payload_entry: dict[str, Any] = {}
                        video_dimensions = get_message_video_dimensions(related_message)
                        if video_dimensions[0] and video_dimensions[1]:
                            payload_entry["width"] = video_dimensions[0]
                            payload_entry["height"] = video_dimensions[1]

                        try:
                            raw_bytes = await client.download_media(related_message, file=bytes)
                            if raw_bytes:
                                payload_entry["video_bytes"] = raw_bytes
                                payload_entry["video_extension"] = get_message_video_extension(related_message)
                        except Exception as error:  # pragma: no cover - network/runtime path
                            log.warning("Attached video download failed on post %s: %s", int(message.id), error)

                        try:
                            poster_bytes = await download_message_video_poster_bytes(client, related_message)
                            if poster_bytes:
                                payload_entry["poster_bytes"] = poster_bytes
                        except Exception as error:  # pragma: no cover - network/runtime path
                            log.warning("Attached video poster download failed on post %s: %s", int(message.id), error)

                        if payload_entry:
                            attached_videos.append(payload_entry)

                if not is_round_video and not attached_videos and not video_width and not video_height:
                    continue

                payload: dict[str, Any] = {}
                if video_width and video_height:
                    payload["video_width"] = video_width
                    payload["video_height"] = video_height

                if is_round_video:
                    payload["video_note"] = True
                    try:
                        raw_bytes = await client.download_media(message, file=bytes)
                        if raw_bytes:
                            payload["video_bytes"] = raw_bytes
                            payload["video_extension"] = get_message_video_extension(message)
                    except Exception as error:  # pragma: no cover - network/runtime path
                        log.warning("Round video download failed on post %s: %s", int(message.id), error)
                    try:
                        poster_bytes = await download_message_video_poster_bytes(client, message)
                        if poster_bytes:
                            payload["poster_bytes"] = poster_bytes
                    except Exception as error:  # pragma: no cover - network/runtime path
                        log.warning("Round video poster download failed on post %s: %s", int(message.id), error)

                if attached_videos:
                    payload["attached_videos"] = attached_videos

                if payload:
                    results[int(message.id)] = payload

            await asyncio.sleep(0.1)

    return results


async def fetch_reply_references_for_posts(config: SiteConfig, posts: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    posts_by_id = {int(post["id"]): post for post in posts if post.get("id")}
    existing_reply_refs = {
        int(post["id"]): post.get("reply_to")
        for post in posts
        if post.get("id") and post.get("reply_to")
    }
    result: dict[int, dict[str, Any]] = {}

    for post_id, reply_reference in existing_reply_refs.items():
        target_post_id = int(reply_reference.get("post_id") or 0)
        if target_post_id <= 0:
            continue

        target_post = posts_by_id.get(target_post_id)
        if not target_post:
            continue

        result[post_id] = {
            "post_id": target_post_id,
            "title": build_reply_reference_title(target_post, fallback_post_id=target_post_id),
            "tg_url": reply_reference.get("tg_url") or target_post.get("tg_url") or f"https://t.me/{config.channel_username}/{target_post_id}",
            "channel_username": reply_reference.get("channel_username") or config.channel_username,
        }

    missing_page_targets = [
        (post_id, reply_reference)
        for post_id, reply_reference in existing_reply_refs.items()
        if post_id not in result
    ]
    if missing_page_targets:
        log.info("Resolving %s reply references from Telegram web pages", len(missing_page_targets))
        for post_id, reply_reference in missing_page_targets:
            enriched_reference = fetch_reply_reference_from_page(reply_reference, config)
            if enriched_reference:
                result[post_id] = enriched_reference

    credentials = get_telegram_session_credentials()
    if not credentials:
        if existing_reply_refs:
            unresolved_count = len(existing_reply_refs) - len(result)
            if unresolved_count > 0:
                log.info("Telegram user session is not configured. %s reply references kept as Telegram snippets.", unresolved_count)
        else:
            log.info("Telegram user session is not configured. Reply reference sync skipped.")
        return result

    post_ids = [int(post["id"]) for post in posts if post.get("id")]
    if not post_ids:
        return result

    api_id, api_hash, session_string = credentials

    from telethon import TelegramClient
    from telethon.sessions import StringSession

    async with TelegramClient(StringSession(session_string), int(api_id), api_hash) as client:
        if not await client.is_user_authorized():
            raise RuntimeError("TELEGRAM_SESSION_STR is not authorized.")

        channel = await client.get_entity(config.channel_username)
        post_messages: dict[int, Any] = {}

        for batch in chunked(post_ids, 100):
            for message in await client.get_messages(channel, ids=batch):
                if message and getattr(message, "id", None):
                    post_messages[int(message.id)] = message
            await asyncio.sleep(0.1)

        reply_targets: dict[int, int] = {}
        target_ids: set[int] = set()

        for post_id, message in post_messages.items():
            target_post_id = extract_reply_target_post_id(message)
            if not target_post_id:
                continue

            reply_targets[post_id] = target_post_id
            target_ids.add(target_post_id)

        if not reply_targets:
            return result

        target_messages: dict[int, Any] = {}
        missing_target_ids = [target_id for target_id in target_ids if target_id not in posts_by_id]

        for batch in chunked(missing_target_ids, 100):
            for message in await client.get_messages(channel, ids=batch):
                if message and getattr(message, "id", None):
                    target_messages[int(message.id)] = message
            await asyncio.sleep(0.1)

        for post_id, target_post_id in reply_targets.items():
            title = build_reply_reference_title(
                posts_by_id.get(target_post_id),
                target_messages.get(target_post_id),
                fallback_post_id=target_post_id,
            )
            result[post_id] = {
                "post_id": target_post_id,
                "title": title,
                "tg_url": f"https://t.me/{config.channel_username}/{target_post_id}",
                "channel_username": config.channel_username,
            }

        return result


async def fetch_high_res_photos_for_posts(config: SiteConfig, posts: list[dict[str, Any]]) -> dict[int, list[bytes]]:
    selected_ids = select_posts_for_high_res_media(posts)
    if not selected_ids:
        return {}

    results: dict[int, list[bytes]] = {}
    selected_posts = {post["id"]: post for post in posts if post["id"] in selected_ids}
    external_override_attempts = 0

    credentials = get_telegram_session_credentials()
    if not credentials and not ENABLE_EXTERNAL_PREVIEW_OVERRIDE:
        log.info("Telegram user session is not configured. High-resolution media refresh skipped.")
        return {}

    log.info("Refreshing high-resolution media for %s posts", len(selected_ids))
    if credentials:
        api_id, api_hash, session_string = credentials

        from telethon import TelegramClient
        from telethon.sessions import StringSession

        async with TelegramClient(StringSession(session_string), int(api_id), api_hash) as client:
            if not await client.is_user_authorized():
                raise RuntimeError("TELEGRAM_SESSION_STR is not authorized.")

            channel = await client.get_entity(config.channel_username)

            for post_id in selected_ids:
                try:
                    message = await client.get_messages(channel, ids=post_id)
                    if not message:
                        continue

                    photo_targets = get_downloadable_photo_targets(message)
                    grouped_id = getattr(message, "grouped_id", None)

                    if grouped_id:
                        ids = list(range(max(1, post_id - 10), post_id + 11))
                        neighbours = await client.get_messages(channel, ids=ids)
                        album_targets: list[Any] = []
                        for neighbour in neighbours:
                            if not neighbour or getattr(neighbour, "grouped_id", None) != grouped_id:
                                continue
                            album_targets.extend(get_downloadable_photo_targets(neighbour))
                        if album_targets:
                            photo_targets = album_targets

                    if not photo_targets:
                        continue

                    downloaded: list[bytes] = []
                    for target in photo_targets:
                        raw_bytes = await client.download_media(target, file=bytes)
                        if raw_bytes:
                            downloaded.append(raw_bytes)

                    if not downloaded:
                        continue

                    results[post_id] = downloaded
                    log.info("Fetched high-resolution media for post %s (%s item(s))", post_id, len(downloaded))
                except Exception as error:  # pragma: no cover - network/runtime path
                    log.warning("High-resolution media sync failed on post %s: %s", post_id, error)

                await asyncio.sleep(0.15)
    else:
        log.info("Telegram user session is not configured. Telegram media override skipped; external preview override is enabled explicitly.")

    for post_id in selected_ids:
        post = selected_posts.get(post_id)
        if not post or len(post.get("photos") or []) != 1:
            continue

        current_override_bytes = None
        if post_id in results and len(results[post_id]) == 1:
            current_override_bytes = results[post_id][0]

        current_bytes = get_current_preview_bytes(post, current_override_bytes)

        telegram_post_override = fetch_telegram_post_page_override(post, current_bytes=current_bytes)
        if telegram_post_override:
            results[post_id] = [telegram_post_override]
            current_bytes = telegram_post_override

        if not ENABLE_EXTERNAL_PREVIEW_OVERRIDE:
            continue

        if external_override_attempts >= MAX_EXTERNAL_PREVIEW_OVERRIDE_POSTS:
            continue

        external_override = fetch_external_preview_override(post, current_bytes=current_bytes)
        external_override_attempts += 1
        if external_override:
            results[post_id] = [external_override]

    return results


async def fetch_comments_for_posts(config: SiteConfig, posts: list[dict[str, Any]]) -> tuple[bool, dict[int, list[dict[str, Any]]]]:
    credentials = get_telegram_session_credentials()
    if not credentials:
        log.info("Telegram user session is not configured. Comment sync skipped.")
        return False, {}
    api_id, api_hash, session_string = credentials

    from telethon import TelegramClient
    from telethon.errors import ChannelPrivateError, MsgIdInvalidError, RPCError
    from telethon.sessions import StringSession
    from telethon.tl.functions.messages import GetDiscussionMessageRequest
    from telethon.utils import get_display_name

    selected_ids = select_posts_for_comment_refresh(posts, config)
    log.info("Refreshing comments for %s posts", len(selected_ids))
    results: dict[int, list[dict[str, Any]]] = {}

    async with TelegramClient(StringSession(session_string), int(api_id), api_hash) as client:
        if not await client.is_user_authorized():
            raise RuntimeError("TELEGRAM_SESSION_STR is not authorized.")

        channel = await client.get_entity(config.channel_username)

        for post_id in selected_ids:
            try:
                discussion = await client(GetDiscussionMessageRequest(peer=channel, msg_id=post_id))
                if not discussion.messages:
                    results[post_id] = []
                    continue

                root_message = discussion.messages[0]
                discussion_peer = await client.get_input_entity(root_message.peer_id)
                comments: list[dict[str, Any]] = []

                async for message in client.iter_messages(discussion_peer, reply_to=root_message.id, reverse=True):
                    text = (message.message or "").strip()
                    if not text and message.media:
                        text = "[media]"
                    if not text:
                        continue

                    author = message.post_author
                    if not author:
                        sender = await message.get_sender()
                        if sender:
                            author = get_display_name(sender) or None
                            if not author and getattr(sender, "username", None):
                                author = f"@{sender.username}"

                    comments.append(
                        {
                            "id": message.id,
                            "author": author or "Telegram user",
                            "text": text,
                            "date": message.date.astimezone(timezone.utc).isoformat() if message.date else None,
                        }
                    )

                results[post_id] = comments
                log.info("Fetched %s comments for post %s", len(comments), post_id)
            except (MsgIdInvalidError, ChannelPrivateError):
                results[post_id] = []
            except RPCError as error:
                log.warning("Telegram RPC error on post %s: %s", post_id, error)
                results[post_id] = []
            except Exception as error:  # pragma: no cover - network/runtime path
                log.warning("Comment sync failed on post %s: %s", post_id, error)
                results[post_id] = []

            await asyncio.sleep(0.3)

    return True, results


def write_manifest(config: SiteConfig) -> bool:
    manifest = {
        "name": config.site_name,
        "short_name": config.channel_title[:12] or config.site_name[:12],
        "description": config.site_description,
        "start_url": "./",
        "display": "standalone",
        "background_color": config.background_color,
        "theme_color": config.accent_color,
        "lang": config.language,
        "icons": [
            {
                "src": "assets/icon.svg",
                "sizes": "any",
                "type": "image/svg+xml",
                "purpose": "any maskable",
            },
            {
                "src": "assets/icon-192.png",
                "sizes": "192x192",
                "type": "image/png",
                "purpose": "any maskable",
            },
            {
                "src": "assets/icon-512.png",
                "sizes": "512x512",
                "type": "image/png",
                "purpose": "any maskable",
            }
        ],
    }
    return write_text_if_changed(MANIFEST_PATH, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")


def build_site_payload(config: SiteConfig) -> dict[str, Any]:
    return {
        "channel_username": config.channel_username,
        "channel_title": config.channel_title,
        "site_name": config.site_name,
        "site_description": config.site_description,
        "language": config.language,
        "accent_color": config.accent_color,
        "background_color": config.background_color,
        "avatar_path": resolve_avatar_path(config),
    }


def build_source_payload(config: SiteConfig, comments_enabled: bool) -> dict[str, Any]:
    return {
        "channel_key": CHANNEL_KEY or config.channel_username.lower(),
        "channel_url": config.channel_web_url,
        "comments_enabled": comments_enabled,
        "recent_posts_months": config.recent_posts_months,
    }


def normalize_telegram_post_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""

    try:
        parsed = urlparse(raw)
    except Exception:
        return raw.rstrip("/")

    host = (parsed.netloc or "").lower().replace("www.", "")
    if host != "t.me":
        return raw.rstrip("/")

    segments = [segment for segment in (parsed.path or "").split("/") if segment]
    normalized_segments = segments[1:] if segments[:1] == ["s"] else segments
    if len(normalized_segments) >= 2 and normalized_segments[1].isdigit():
        return f"https://t.me/{normalized_segments[0]}/{normalized_segments[1]}"

    return f"https://t.me/{'/'.join(normalized_segments)}".rstrip("/")


def normalize_post_text_for_fingerprint(value: Any) -> str:
    return collapse_whitespace(strip_tags(str(value or "")))


def normalize_forwarded_source_url(post: dict[str, Any]) -> str:
    forwarded = post.get("forwarded_from") or {}
    return normalize_telegram_post_url(str(forwarded.get("source_url") or forwarded.get("channel_url") or ""))


def build_post_media_fingerprint(post: dict[str, Any]) -> str:
    photos = [photo for photo in (post.get("photos") or []) if photo]
    video_url = normalize_telegram_post_url(str(post.get("video_url") or ""))
    videos = normalize_video_entries(post.get("videos"))
    video_fingerprint = ",".join(
        (
            normalize_telegram_post_url(str(video.get("source_url") or ""))
            or f"{parse_positive_int(video.get('width')) or 0}x{parse_positive_int(video.get('height')) or 0}:{int(bool(video.get('poster')))}"
        )
        for video in videos
    )
    link_preview = post.get("link_preview") or {}
    link_preview_url = normalize_telegram_post_url(str(link_preview.get("href") or ""))
    return "|".join(
        [
            str(len(photos)),
            video_url,
            str(len(videos)),
            video_fingerprint,
            "note" if bool(post.get("video_note")) else "",
            link_preview_url,
        ]
    )


def get_post_duplicate_fingerprint(post: dict[str, Any]) -> str:
    forwarded_source_url = normalize_forwarded_source_url(post)
    if not forwarded_source_url:
        return ""

    date_value = str(post.get("date") or "").strip()
    text_value = normalize_post_text_for_fingerprint(post.get("text_html") or post.get("text"))
    media_fingerprint = build_post_media_fingerprint(post)
    return f"fwd:{forwarded_source_url}|{date_value}|{text_value}|{media_fingerprint}"


def dedupe_posts(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique_posts: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    seen_tg_urls: set[str] = set()
    seen_duplicate_fingerprints: set[str] = set()

    for post in posts:
        post_id = int(post.get("id") or 0)
        canonical_tg_url = normalize_telegram_post_url(str(post.get("tg_url") or ""))
        duplicate_fingerprint = get_post_duplicate_fingerprint(post)
        if post_id and post_id in seen_ids:
            continue
        if canonical_tg_url and canonical_tg_url in seen_tg_urls:
            continue
        if duplicate_fingerprint and duplicate_fingerprint in seen_duplicate_fingerprints:
            continue

        if post_id:
            seen_ids.add(post_id)
        if canonical_tg_url:
            seen_tg_urls.add(canonical_tg_url)
            post["tg_url"] = canonical_tg_url
        if duplicate_fingerprint:
            seen_duplicate_fingerprints.add(duplicate_fingerprint)
        unique_posts.append(post)

    return unique_posts


def build_channel_build_id(posts: list[dict[str, Any]], comments_enabled: bool) -> str:
    digest = hashlib.sha1()
    digest.update(f"comments:{int(comments_enabled)}|count:{len(posts)}".encode("utf-8"))
    for post in posts:
        digest.update(
            (
                f"{post.get('id')}|{post.get('date') or ''}|"
                f"{post.get('comments_count') or 0}|"
                f"{build_post_media_fingerprint(post)}|"
                f"{int(bool(post.get('video_note')))};"
            ).encode("utf-8")
        )
    return digest.hexdigest()[:12]


def build_feed_index_payload(
    config: SiteConfig,
    posts: list[dict[str, Any]],
    comments_enabled: bool,
    build_id: str,
) -> dict[str, Any]:
    total_pages = max(1, math.ceil(len(posts) / FEED_PAGE_SIZE))
    return {
        "generated_at": None,
        "build_id": build_id,
        "site": build_site_payload(config),
        "source": build_source_payload(config, comments_enabled),
        "pagination": {
            "page": 1,
            "page_size": FEED_PAGE_SIZE,
            "total_posts": len(posts),
            "total_pages": total_pages,
        },
        "posts": posts[:FEED_PAGE_SIZE],
    }


def build_feed_page_payload(page: int, posts: list[dict[str, Any]], total_posts: int, build_id: str) -> dict[str, Any]:
    total_pages = max(1, math.ceil(total_posts / FEED_PAGE_SIZE))
    start = (page - 1) * FEED_PAGE_SIZE
    end = start + FEED_PAGE_SIZE
    return {
        "generated_at": None,
        "build_id": build_id,
        "pagination": {
            "page": page,
            "page_size": FEED_PAGE_SIZE,
            "total_posts": total_posts,
            "total_pages": total_pages,
        },
        "posts": posts[start:end],
    }


def build_post_payload(config: SiteConfig, post: dict[str, Any], comments_enabled: bool, build_id: str) -> dict[str, Any]:
    return {
        "generated_at": None,
        "build_id": build_id,
        "site": build_site_payload(config),
        "source": build_source_payload(config, comments_enabled),
        "post": post,
    }


def strip_tags(value: str | None) -> str:
    return re.sub(r"<[^>]+>", "", value or "").strip()


def collapse_whitespace(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def shorten_text(value: str | None, limit: int) -> str:
    collapsed = collapse_whitespace(value)
    if len(collapsed) <= limit:
        return collapsed
    clipped = collapsed[:limit].rsplit(" ", 1)[0].strip()
    return f"{clipped}…" if clipped else collapsed[:limit]


def render_post_page_link_preview(post: dict[str, Any], root_prefix: str) -> str:
    if post_has_physical_media(post):
        return ""

    preview = normalize_link_preview_entry(post.get("link_preview"))
    if not preview:
        return ""

    image = normalize_photo_entry(preview.get("image"))
    image_markup = ""
    if image and image.get("full_url"):
        image_src = image.get("feed_url") or image.get("full_url") or image.get("thumb_url")
        srcset_parts: list[str] = []
        for key, width_key in (("thumb_url", "thumb_width"), ("feed_url", "feed_width"), ("full_url", "full_width")):
            candidate_url = image.get(key)
            candidate_width = image.get(width_key)
            if not candidate_url or not candidate_width:
                continue
            resolved_url = candidate_url if re.match(r"^https?://", candidate_url) else f"{root_prefix}{candidate_url.lstrip('./')}"
            srcset_parts.append(f"{resolved_url} {candidate_width}w")
        resolved_src = image_src if re.match(r"^https?://", image_src) else f"{root_prefix}{image_src.lstrip('./')}"
        srcset_attr = (
            f'srcset="{html_lib.escape(", ".join(srcset_parts))}" sizes="(max-width: 860px) calc(100vw - 88px), 220px" '
            if srcset_parts
            else ""
        )
        badge_markup = '<span class="post-card__link-preview-badge">Видео</span>' if preview.get("is_video") else ""
        image_markup = (
            '<div class="post-card__link-preview-media">'
            f'<img src="{html_lib.escape(resolved_src)}" {srcset_attr}'
            'alt="" loading="lazy" decoding="async">'
            f"{badge_markup}"
            '</div>'
        )

    description_markup = (
        f'<div class="post-card__link-preview-description">{html_lib.escape(preview["description"])}</div>'
        if preview.get("description")
        else ""
    )
    caption = preview.get("site_name") or preview.get("host") or normalize_host_label(urlparse(preview["href"]).hostname)
    return (
        f'<a class="post-card__link-preview" href="{html_lib.escape(preview["href"])}" target="_blank" rel="noopener noreferrer">'
        f"{image_markup}"
        '<div class="post-card__link-preview-copy">'
        f'<div class="post-card__link-preview-caption">{html_lib.escape(caption)}</div>'
        f'<div class="post-card__link-preview-title">{html_lib.escape(preview["title"])}</div>'
        f"{description_markup}"
        '</div>'
        '</a>'
    )


def render_post_page_media(post: dict[str, Any]) -> str:
    root_prefix = "../../../" if CHANNEL_KEY else "../../"
    photos = [normalize_photo_entry(photo) for photo in post.get("photos") or []]
    photos = [photo for photo in photos if photo]
    videos = normalize_video_entries(post.get("videos"))
    if not photos and not videos and not post.get("video_url"):
        return ""

    media_items: list[str] = []
    total_items = len(photos) + len(videos) + (1 if post.get("video_url") else 0)
    is_gallery = total_items > 1
    sizes_attr = (
        "(max-width: 480px) calc(100vw - 44px), (max-width: 860px) calc(50vw - 28px), 520px"
        if is_gallery
        else "(max-width: 860px) calc(100vw - 44px), 980px"
    )
    for index, photo in enumerate(photos):
        src = photo.get("feed_url") or photo["thumb_url"]
        candidate_pairs = (
            [
                (photo.get("thumb_url"), photo.get("thumb_width")),
                (photo.get("full_url"), photo.get("full_width")),
            ]
            if is_gallery
            else [
                (photo.get("thumb_url"), photo.get("thumb_width")),
                (photo.get("feed_url"), photo.get("feed_width")),
                (photo.get("full_url"), photo.get("full_width")),
            ]
        )
        seen_urls: set[str] = set()
        srcset_parts = []
        for candidate_url, candidate_width in candidate_pairs:
            if not candidate_url or not candidate_width or candidate_url in seen_urls:
                continue
            seen_urls.add(candidate_url)
            srcset_parts.append(f'{root_prefix}{candidate_url} {candidate_width}w')
        srcset = ", ".join(srcset_parts)
        intrinsic_width = (
            photo.get("thumb_width") or photo.get("full_width") or photo.get("source_width")
            if is_gallery
            else photo.get("feed_width") or photo.get("full_width") or photo.get("thumb_width") or photo.get("source_width")
        )
        intrinsic_height = (
            photo.get("thumb_height") or photo.get("full_height") or photo.get("source_height")
            if is_gallery
            else photo.get("feed_height") or photo.get("full_height") or photo.get("thumb_height") or photo.get("source_height")
        )
        render_max_width = None if is_gallery else (
            photo.get("full_width") or photo.get("feed_width") or photo.get("thumb_width") or photo.get("source_width")
        )
        media_items.append(
            (
                '<a class="media-trigger" href="{root_prefix}{full_url}" target="_blank" rel="noopener">'
                '<img src="{root_prefix}{src}" '
                '{srcset_attr}'
                '{dimensions_attr}'
                '{style_attr}'
                'sizes="{sizes_attr}" '
                'alt="Media {index}" loading="lazy" decoding="async"></a>'
            ).format(
                root_prefix=root_prefix,
                full_url=html_lib.escape(photo["full_url"]),
                src=html_lib.escape(src),
                srcset_attr=f'srcset="{html_lib.escape(srcset)}" ' if srcset else "",
                dimensions_attr=(
                    f'width="{intrinsic_width}" height="{intrinsic_height}" '
                    if intrinsic_width and intrinsic_height else ""
                ),
                style_attr=f'style="--media-max-inline-size:{render_max_width}px" ' if render_max_width else "",
                sizes_attr=html_lib.escape(sizes_attr),
                index=index + 1,
            )
        )

    for video in videos:
        video_src = video["url"]
        if not re.match(r"^https?://", video_src):
            video_src = f"{root_prefix}{video_src.lstrip('./')}"
        poster = normalize_video_poster_entry(video.get("poster"))
        poster_attr = ""
        if poster:
            poster_src = poster.get("full_url") or poster.get("feed_url") or poster.get("thumb_url")
            if poster_src:
                poster_src = poster_src if re.match(r"^https?://", poster_src) else f"{root_prefix}{poster_src.lstrip('./')}"
                poster_attr = f' poster="{html_lib.escape(poster_src)}"'
        media_items.append(
            f'<video src="{html_lib.escape(video_src)}"{poster_attr} preload="metadata" controls playsinline></video>'
        )

    video_poster = normalize_video_poster_entry(post.get("video_poster"))
    if post.get("video_url"):
        video_src = post["video_url"]
        if not re.match(r"^https?://", video_src):
            video_src = f"{root_prefix}{video_src.lstrip('./')}"
        video_class = "post-card__round-video" if post.get("video_note") and not photos else ""
        poster_attr = ""
        if video_poster and video_poster.get("full_url"):
            poster_src = f"{root_prefix}{video_poster['full_url'].lstrip('./')}"
            poster_attr = f' poster="{html_lib.escape(poster_src)}"'
        media_items.append(
            f'<video class="{video_class}" src="{html_lib.escape(video_src)}"{poster_attr} preload="metadata" controls playsinline></video>'
        )

    if len(media_items) > 1:
        gallery_class = "post-card__media post-card__media--gallery"
    elif post.get("video_note") and not photos and not videos and post.get("video_url"):
        gallery_class = "post-card__media post-card__media--round-video"
    else:
        gallery_class = "post-card__media"
    return f'<div class="{gallery_class}">{"".join(media_items)}</div>'


def render_post_page_html(config: SiteConfig, post: dict[str, Any], comments_enabled: bool) -> str:
    root_prefix = "../../../" if CHANNEL_KEY else "../../"
    feed_href = f"{root_prefix}?channel={CHANNEL_KEY}" if CHANNEL_KEY else root_prefix
    title = shorten_text(post.get("text") or f"Пост #{post['id']}", 72) or f"Пост #{post['id']}"
    description = shorten_text(strip_tags(post.get("text_html") or post.get("text") or ""), 180) or config.site_description
    first_photo = normalize_photo_entry((post.get("photos") or [None])[0])
    first_video = normalize_video_entry((post.get("videos") or [None])[0])
    first_video_poster = normalize_video_poster_entry((first_video or {}).get("poster")) or normalize_video_poster_entry(post.get("video_poster"))
    if first_photo:
        og_image = f"{root_prefix}{first_photo['full_url']}"
    elif first_video_poster and first_video_poster.get("full_url"):
        og_image = f"{root_prefix}{first_video_poster['full_url']}"
    else:
        og_image = f"{root_prefix}{config.avatar_path}"
    comments_link = f'{feed_href}#comments-{post["id"]}' if comments_enabled else feed_href
    comments_cta = ""
    if comments_enabled and (post.get("comments_count") or post.get("comments_url") or post.get("comments_available")):
        comments_cta = (
            f'<a class="button button--ghost" href="{comments_link}">'
            f'Комментарии ({post.get("comments_count") or 0})</a>'
        )

    return f"""<!DOCTYPE html>
<html lang="{html_lib.escape(config.language)}">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{html_lib.escape(title)} | {html_lib.escape(config.channel_title)}</title>
  <meta name="description" content="{html_lib.escape(description)}">
  <meta property="og:type" content="article">
  <meta property="og:title" content="{html_lib.escape(title)}">
  <meta property="og:description" content="{html_lib.escape(description)}">
  <meta property="og:image" content="{html_lib.escape(og_image)}">
  <meta property="article:published_time" content="{html_lib.escape(post.get("date") or "")}">
  <meta name="theme-color" content="{html_lib.escape(config.accent_color)}">
  <link rel="icon" href="{root_prefix}assets/icon.svg" type="image/svg+xml">
  <link rel="icon" href="{root_prefix}assets/icon-192.png" sizes="192x192" type="image/png">
  <link rel="apple-touch-icon" href="{root_prefix}assets/apple-touch-icon.png">
  <link rel="manifest" href="{root_prefix}manifest.webmanifest">
  <link rel="preload" href="{root_prefix}assets/fonts/manrope-cyrillic.woff2" as="font" type="font/woff2" crossorigin>
  <link rel="preload" href="{root_prefix}assets/fonts/manrope-latin.woff2" as="font" type="font/woff2" crossorigin>
  <link rel="stylesheet" href="{root_prefix}assets/fonts/fonts.css">
  <link rel="stylesheet" href="{root_prefix}style.css">
  <script>
    (function() {{
      var theme = localStorage.getItem('theme');
      if (theme) document.documentElement.setAttribute('data-theme', theme);
    }})();
  </script>
</head>
<body>
  <div class="site-shell post-shell">
    <header class="post-header">
      <div>
        <p class="eyebrow">Отдельная страница поста</p>
        <h1 class="post-page-title">{html_lib.escape(title)}</h1>
      </div>
      <div class="post-header__actions">
        <a class="button button--ghost" href="{feed_href}">К ленте</a>
        <a class="button button--primary" href="{html_lib.escape(post["tg_url"])}" target="_blank" rel="noopener">В Telegram</a>
      </div>
    </header>

    <article class="post-card">
      {render_post_page_media(post)}
      <div class="post-card__body">
        <div class="post-card__text">{post.get("text_html") or html_lib.escape(post.get("text") or "").replace(chr(10), "<br>")}</div>
        {render_post_page_link_preview(post, root_prefix)}
      </div>
      <div class="post-card__footer">
        <div class="post-card__stats">
          <span class="chip">{html_lib.escape(post.get("date") or "")}</span>
          <span class="chip">Просмотры: {post.get("views") or 0}</span>
        </div>
        <div class="post-card__links">
          {comments_cta}
          <a class="post-card__link" href="{html_lib.escape(post["tg_url"])}" target="_blank" rel="noopener">Открыть в Telegram</a>
        </div>
      </div>
    </article>
  </div>
</body>
</html>
"""


def cleanup_removed_page_files(total_pages: int) -> bool:
    changed = False
    for path in PAGES_DIR.glob("*.json"):
        try:
            page = int(path.stem)
        except ValueError:
            continue
        if 2 <= page <= total_pages:
            continue
        path.unlink()
        changed = True
        log.info("Deleted stale page file %s", path.relative_to(ROOT))
    return changed


def cleanup_removed_post_detail_files(active_post_ids: set[int]) -> bool:
    changed = False

    for path in POST_DETAILS_DIR.glob("*.json"):
        try:
            post_id = int(path.stem)
        except ValueError:
            continue
        if post_id in active_post_ids:
            continue
        path.unlink()
        changed = True
        log.info("Deleted stale post payload %s", path.relative_to(ROOT))

    for directory in (POST_PAGES_DIR.iterdir() if POST_PAGES_DIR.exists() else []):
        if not directory.is_dir():
            continue
        try:
            post_id = int(directory.name)
        except ValueError:
            continue
        if post_id in active_post_ids:
            continue
        for child in directory.glob("*"):
            child.unlink()
        directory.rmdir()
        changed = True
        log.info("Deleted stale post page %s", directory.relative_to(ROOT))

    return changed


def cleanup_removed_comment_files(active_post_ids: set[int]) -> bool:
    changed = False
    for path in COMMENTS_DIR.glob("*.json"):
        try:
            post_id = int(path.stem)
        except ValueError:
            continue

        if post_id not in active_post_ids:
            path.unlink()
            changed = True
            log.info("Deleted stale comment file %s", path.name)
    return changed


def write_feed_files(config: SiteConfig, posts: list[dict[str, Any]], comments_enabled: bool, build_id: str) -> bool:
    changed = False
    total_pages = max(1, math.ceil(len(posts) / FEED_PAGE_SIZE))
    if write_json_if_changed(POSTS_PATH, build_feed_index_payload(config, posts, comments_enabled, build_id)):
        changed = True

    PAGES_DIR.mkdir(parents=True, exist_ok=True)
    for page in range(2, total_pages + 1):
        payload = build_feed_page_payload(page, posts, len(posts), build_id)
        if write_json_if_changed(PAGES_DIR / f"{page}.json", payload):
            changed = True

    changed = cleanup_removed_page_files(total_pages) or changed
    return changed


def write_post_detail_files(config: SiteConfig, posts: list[dict[str, Any]], comments_enabled: bool, build_id: str) -> bool:
    changed = False
    POST_DETAILS_DIR.mkdir(parents=True, exist_ok=True)
    POST_PAGES_DIR.mkdir(parents=True, exist_ok=True)

    for post in posts:
        payload = build_post_payload(config, post, comments_enabled, build_id)
        if write_json_if_changed(POST_DETAILS_DIR / f"{post['id']}.json", payload):
            changed = True

        page_dir = POST_PAGES_DIR / str(post["id"])
        page_dir.mkdir(parents=True, exist_ok=True)
        page_html = render_post_page_html(config, post, comments_enabled)
        if write_text_if_changed(page_dir / "index.html", page_html):
            changed = True

    changed = cleanup_removed_post_detail_files({post["id"] for post in posts}) or changed
    return changed


def main() -> int:
    config = load_config()
    if not CHANNEL_KEY:
        write_manifest(config)

    existing_payload = load_json(POSTS_PATH, {})
    existing_posts = load_existing_feed_posts(existing_payload)
    try:
        initial_page_html = fetch_page(config.channel_web_url)
    except Exception as error:  # pragma: no cover - network/runtime path
        if existing_posts:
            log.error(
                "Failed to fetch initial page for @%s. Keeping existing mirror data and marking sync as failed: %s",
                config.channel_username,
                error,
            )
            return 1
        raise

    avatar_changed = mirror_channel_avatar(config, initial_page_html)
    parsed_channel_description = parse_channel_description(initial_page_html)
    if parsed_channel_description:
        config.site_description = parsed_channel_description
    posts = collect_posts(config, initial_page_html=initial_page_html)
    if not posts and existing_posts:
        log.error(
            "No posts were collected for @%s. Existing mirror data was left untouched and sync marked as failed.",
            config.channel_username,
        )
        return 1
    posts = probe_newer_posts_from_direct_pages(
        config,
        posts,
        existing_top_post_id=get_highest_post_id(existing_posts),
        cutoff=subtract_months(datetime.now(timezone.utc), config.recent_posts_months),
    )
    api_recovery_photo_overrides: dict[int, list[bytes]] = {}
    latest_api_post_id: int | None = None
    posts, api_recovery_photo_overrides, latest_api_post_id = asyncio.run(
        recover_newer_posts_from_api(
            config,
            posts,
            cutoff=subtract_months(datetime.now(timezone.utc), config.recent_posts_months),
        )
    )
    deduped_posts = dedupe_posts(posts)
    if len(deduped_posts) != len(posts):
        log.warning(
            "Deduplicated %s repeated post entries for @%s before media sync",
            len(posts) - len(deduped_posts),
            config.channel_username,
        )
        posts = deduped_posts
    enrich_reply_posts_from_pages(posts, config)
    enrich_forwarded_posts_from_source_pages(posts)

    reply_references = asyncio.run(fetch_reply_references_for_posts(config, posts))
    photo_overrides = asyncio.run(fetch_high_res_photos_for_posts(config, posts))
    if api_recovery_photo_overrides:
        photo_overrides = {
            **photo_overrides,
            **api_recovery_photo_overrides,
        }
    video_metadata = asyncio.run(fetch_video_metadata_for_posts(config, posts))
    comments_enabled, comment_results = asyncio.run(fetch_comments_for_posts(config, posts))

    for post in posts:
        reply_to = reply_references.get(post["id"]) or post.get("reply_to")
        if reply_to:
            post["reply_to"] = reply_to

    for post in posts:
        if post["id"] in comment_results:
            post["comments_count"] = len(comment_results[post["id"]])

    if latest_api_post_id and get_highest_post_id(posts) < latest_api_post_id:
        raise RuntimeError(
            f"Telegram API exposed newer post {latest_api_post_id} for @{config.channel_username}, "
            f"but sync only collected up to {get_highest_post_id(posts)}"
        )

    changes_detected = avatar_changed
    changes_detected = mirror_post_photos(posts, photo_overrides=photo_overrides) or changes_detected
    changes_detected = mirror_round_videos(
        posts,
        video_metadata=video_metadata,
        existing_posts=existing_posts,
    ) or changes_detected
    changes_detected = mirror_attached_videos(
        posts,
        video_metadata=video_metadata,
        existing_posts=existing_posts,
    ) or changes_detected
    changes_detected = mirror_link_previews(
        posts,
        existing_posts=existing_posts,
    ) or changes_detected
    round_video_errors = validate_round_video_posts(posts)
    if round_video_errors:
        for error in round_video_errors:
            log.error("Round video validation failed: %s", error)
        return 1
    posts = [
        post
        for post in posts
        if post.get("text") or (post.get("photos") or []) or (post.get("videos") or []) or post.get("video_url") or post.get("link_preview")
    ]
    deduped_posts = dedupe_posts(posts)
    if len(deduped_posts) != len(posts):
        log.warning(
            "Deduplicated %s repeated post entries for @%s before writing feed",
            len(posts) - len(deduped_posts),
            config.channel_username,
        )
        posts = deduped_posts
    build_id = build_channel_build_id(posts, comments_enabled)
    active_ids = {post["id"] for post in posts}
    changes_detected = cleanup_removed_comment_files(active_ids) or changes_detected

    for post_id, comments in comment_results.items():
        if post_id not in active_ids:
            continue
        payload = {
            "generated_at": None,
            "build_id": build_id,
            "post_id": post_id,
            "comments": comments,
        }
        if write_json_if_changed(COMMENTS_DIR / f"{post_id}.json", payload):
            changes_detected = True

    changes_detected = write_feed_files(config, posts, comments_enabled, build_id) or changes_detected
    changes_detected = cleanup_removed_post_detail_files(set()) or changes_detected

    log.info("Done. Material changes detected: %s", "yes" if changes_detected else "no")
    return 0


if __name__ == "__main__":
    sys.exit(main())
