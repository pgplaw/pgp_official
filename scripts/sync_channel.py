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
import sys
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
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
CHANNEL_MEDIA_DIR = CHANNEL_DATA_DIR / "media"
CHANNEL_AVATAR_PATH = CHANNEL_MEDIA_DIR / "channel-avatar.jpg"
SUPERRES_MODEL_DIR = ROOT / "ops" / "models"
EDSR_X2_MODEL_PATH = SUPERRES_MODEL_DIR / "EDSR_x2.pb"
FSRCNN_X2_MODEL_PATH = SUPERRES_MODEL_DIR / "FSRCNN_x2.pb"
POST_PAGES_DIR = DOCS_DIR / "channels" / CHANNEL_KEY / "posts" if CHANNEL_KEY else DOCS_DIR / "posts"
MANIFEST_PATH = DOCS_DIR / "manifest.webmanifest"
FEED_PAGE_SIZE = 16
IMAGE_VARIANT_VERSION = "v8"

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TelegramPagesMirror/1.0)",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}
FETCH_RETRY_DELAYS = (1.0, 2.5, 5.0)
FAST_EXTERNAL_RETRY_DELAYS: tuple[float, ...] = ()
EXTERNAL_PAGE_TIMEOUT_SECONDS = 5
EXTERNAL_IMAGE_TIMEOUT_SECONDS = 6
MIN_EXTERNAL_OVERRIDE_WIDTH = 1000
MIN_EXTERNAL_OVERRIDE_RATIO_GAIN = 1.15
MAX_EXTERNAL_OVERRIDE_RATIO_DELTA = 0.12
MAX_EXTERNAL_PREVIEW_OVERRIDE_POSTS = 10
MAX_EXTERNAL_LINKS_TO_TRY = 2
ENABLE_EXTERNAL_PREVIEW_OVERRIDE = os.environ.get("TG_ENABLE_EXTERNAL_PREVIEW_OVERRIDE", "").strip().lower() in {"1", "true", "yes", "on"}
LOW_RES_SINGLE_UPSCALE_THRESHOLD = 1200
LOW_RES_SINGLE_FEED_TARGET = 1800
LOW_RES_SINGLE_FULL_TARGET = 2400
LOW_RES_SINGLE_MAX_UPSCALE_FACTOR = 2.35
EDSR_X2_MODEL_URL = "https://github.com/opencv/opencv_contrib/raw/4.x/modules/dnn_superres/samples/EDSR_x2.pb"
FSRCNN_X2_MODEL_URL = "https://raw.githubusercontent.com/Saafke/FSRCNN_Tensorflow/master/models/FSRCNN_x2.pb"
FAILED_EXTERNAL_PREVIEW_HOSTS: set[str] = set()

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
) -> str | bytes:
    headers = BASE_HEADERS if not binary else {
        **BASE_HEADERS,
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    }

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
) -> bytes:
    return bytes(fetch_url(url, binary=True, timeout=timeout, retry_delays=retry_delays, log_failures=log_failures))


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


def normalize_photo_entry(photo: Any) -> dict[str, str] | None:
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
            if source_url and re.match(r"^https?://", source_url):
                entry["source_url"] = source_url
            return entry
    return None


def ensure_superres_model(model_path: Path, model_url: str, label: str) -> Path:
    if model_path.exists():
        return model_path

    SUPERRES_MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_bytes = fetch_binary(model_url, timeout=20)
    model_path.write_bytes(model_bytes)
    log.info("Downloaded %s model to %s", label, model_path.relative_to(ROOT))
    return model_path


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
            if allow_single_image_upscale and max(original_size) < LOW_RES_SINGLE_UPSCALE_THRESHOLD:
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


def mirror_post_photos(posts: list[dict[str, Any]], photo_overrides: dict[int, list[bytes]] | None = None) -> bool:
    POSTS_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    POSTS_THUMBS_DIR.mkdir(parents=True, exist_ok=True)
    POSTS_FEED_DIR.mkdir(parents=True, exist_ok=True)
    active_relative_paths: set[str] = set()
    changes_detected = False
    photo_overrides = photo_overrides or {}

    for post in posts:
        mirrored_photos: list[dict[str, str]] = []
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
                    local_full_path.exists()
                    and local_thumb_path.exists()
                    and (not is_single_photo_post or (local_feed_path and local_feed_path.exists()))
                )

                if current_variant_present and current_files_present:
                    active_relative_paths.add(full_source)
                    active_relative_paths.add(photo["thumb_url"])
                    if is_single_photo_post and photo.get("feed_url"):
                        active_relative_paths.add(photo["feed_url"])
                    mirrored_photos.append(photo)
                    continue

                if local_full_path.exists():
                    try:
                        override_bytes = local_full_path.read_bytes()
                    except Exception as error:  # pragma: no cover - runtime/filesystem path
                        log.warning("Failed to read existing mirrored image for post %s: %s", post["id"], error)
                        active_relative_paths.add(full_source)
                        active_relative_paths.add(photo["thumb_url"])
                        if is_single_photo_post and photo.get("feed_url"):
                            active_relative_paths.add(photo["feed_url"])
                        mirrored_photos.append(photo)
                        continue
                else:
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
            mirrored_photos.append(mirrored_entry)
            active_relative_paths.add(full_relative_url)
            active_relative_paths.add(thumb_relative_url)

        if post.get("photos") != mirrored_photos:
            post["photos"] = mirrored_photos

    for base_dir in (POSTS_MEDIA_DIR, POSTS_THUMBS_DIR, POSTS_FEED_DIR):
        for path in base_dir.glob("*"):
            if not path.is_file():
                continue

            relative_url = path.relative_to(DOCS_DIR).as_posix()
            if relative_url in active_relative_paths:
                continue

            path.unlink()
            log.info("Deleted stale mirrored image %s", path.relative_to(ROOT))
            changes_detected = True

    return changes_detected


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


def build_text_fields(raw_html: str) -> tuple[str | None, str | None]:
    raw_html = raw_html or ""
    raw_with_breaks = re.sub(r"<br\s*/?>", "\n", raw_html)
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

    return plain, html_markup


def extract_div_inner_html_by_class(html_text: str, class_name: str) -> str:
    open_tag_match = re.search(
        rf'<div[^>]+class="[^"]*\b{re.escape(class_name)}\b[^"]*"[^>]*>',
        html_text,
        re.IGNORECASE,
    )
    if not open_tag_match:
        return ""

    start_index = open_tag_match.end()
    depth = 1
    token_pattern = re.compile(r"<div\b[^>]*>|</div>", re.IGNORECASE)

    for token_match in token_pattern.finditer(html_text, start_index):
        token = token_match.group(0).lower()
        if token.startswith("</div"):
            depth -= 1
            if depth == 0:
                return html_text[start_index:token_match.start()]
        else:
            depth += 1

    return html_text[start_index:]


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


def build_post_media_page_urls(post: dict[str, Any]) -> list[str]:
    tg_url = (post.get("tg_url") or "").strip()
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


def extract_telegram_post_block(page_html: str, post_id: int) -> str | None:
    blocks = re.split(r'(?=<div class="tgme_widget_message_wrap)', page_html)

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
    blocks = re.split(r'(?=<div class="tgme_widget_message_wrap)', html_text)
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
        video_match = re.search(r'<video[^>]+src="([^"]+)"', block)
        if not video_match:
            video_match = re.search(r'<source[^>]+src="([^"]+)"', block)

        photos = [
            urljoin("https://t.me", html_lib.unescape(url))
            for url in re.findall(r"tgme_widget_message_photo_wrap[^>]+url\('([^']+)'\)", block)
        ]
        if not photos:
            link_preview_match = re.search(r"link_preview_image[^>]+url\('([^']+)'\)", block)
            if link_preview_match:
                photos = [urljoin("https://t.me", html_lib.unescape(link_preview_match.group(1)))]
        video_url = urljoin("https://t.me", html_lib.unescape(video_match.group(1))) if video_match else None
        raw_text = extract_div_inner_html_by_class(block, "tgme_widget_message_text")
        text, text_html = build_text_fields(raw_text)
        forwarded_from = extract_forwarded_source(block)
        reply_to = extract_reply_reference(block, config)

        if not text and not photos and not video_url:
            continue

        comments_url = None
        if comments_link_match:
            comments_url = urljoin("https://t.me", html_lib.unescape(comments_link_match.group(1)))

        posts.append(
            {
                "id": post_id,
                "date": date_match.group(1) if date_match else None,
                "text": text,
                "text_html": text_html,
                "views": parse_count(views_match.group(1) if views_match else None),
                "comments_count": parse_count(comments_count_match.group(1) if comments_count_match else None),
                "comments_url": comments_url,
                "photos": photos,
                "video_url": video_url,
                "tg_url": f"https://t.me/{config.channel_username}/{post_id}",
                "forwarded_from": forwarded_from,
                "reply_to": reply_to,
            }
        )

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


def get_downloadable_photo_targets(message: Any) -> list[Any]:
    targets: list[Any] = []

    if getattr(message, "photo", None):
        targets.append(message.photo)

    webpage = getattr(getattr(message, "media", None), "webpage", None)
    if webpage and getattr(webpage, "photo", None):
        targets.append(webpage.photo)

    return targets


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


def build_feed_index_payload(config: SiteConfig, posts: list[dict[str, Any]], comments_enabled: bool) -> dict[str, Any]:
    total_pages = max(1, math.ceil(len(posts) / FEED_PAGE_SIZE))
    return {
        "generated_at": None,
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


def build_feed_page_payload(page: int, posts: list[dict[str, Any]], total_posts: int) -> dict[str, Any]:
    total_pages = max(1, math.ceil(total_posts / FEED_PAGE_SIZE))
    start = (page - 1) * FEED_PAGE_SIZE
    end = start + FEED_PAGE_SIZE
    return {
        "generated_at": None,
        "pagination": {
            "page": page,
            "page_size": FEED_PAGE_SIZE,
            "total_posts": total_posts,
            "total_pages": total_pages,
        },
        "posts": posts[start:end],
    }


def build_post_payload(config: SiteConfig, post: dict[str, Any], comments_enabled: bool) -> dict[str, Any]:
    return {
        "generated_at": None,
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


def render_post_page_media(post: dict[str, Any]) -> str:
    root_prefix = "../../../" if CHANNEL_KEY else "../../"
    photos = [normalize_photo_entry(photo) for photo in post.get("photos") or []]
    photos = [photo for photo in photos if photo]
    if not photos and not post.get("video_url"):
        return ""

    media_items: list[str] = []
    is_gallery = len(photos) > 1
    for index, photo in enumerate(photos):
        src = photo.get("feed_url") or photo["thumb_url"]
        srcset_parts = []
        if photo.get("thumb_url"):
            srcset_parts.append(f'{root_prefix}{photo["thumb_url"]} 1280w')
        if not is_gallery and photo.get("feed_url") and photo["feed_url"] not in {photo["thumb_url"], photo["full_url"]}:
            srcset_parts.append(f'{root_prefix}{photo["feed_url"]} 1800w')
        if photo.get("full_url") and photo["full_url"] != (photo.get("feed_url") or photo["thumb_url"]):
            srcset_parts.append(f'{root_prefix}{photo["full_url"]} 2400w')
        srcset = ", ".join(srcset_parts)
        media_items.append(
            (
                '<a class="media-trigger" href="{root_prefix}{full_url}" target="_blank" rel="noopener">'
                '<img src="{root_prefix}{src}" '
                '{srcset_attr}'
                'sizes="(max-width: 860px) calc(100vw - 44px), 980px" '
                'alt="Media {index}" loading="lazy" decoding="async"></a>'
            ).format(
                root_prefix=root_prefix,
                full_url=html_lib.escape(photo["full_url"]),
                src=html_lib.escape(src),
                srcset_attr=f'srcset="{html_lib.escape(srcset)}" ' if srcset else "",
                index=index + 1,
            )
        )

    if post.get("video_url"):
        media_items.append(
            f'<video src="{html_lib.escape(post["video_url"])}" preload="metadata" controls playsinline></video>'
        )

    gallery_class = "post-card__media post-card__media--gallery" if len(media_items) > 1 else "post-card__media"
    return f'<div class="{gallery_class}">{"".join(media_items)}</div>'


def render_post_page_html(config: SiteConfig, post: dict[str, Any], comments_enabled: bool) -> str:
    root_prefix = "../../../" if CHANNEL_KEY else "../../"
    feed_href = f"{root_prefix}?channel={CHANNEL_KEY}" if CHANNEL_KEY else root_prefix
    title = shorten_text(post.get("text") or f"Пост #{post['id']}", 72) or f"Пост #{post['id']}"
    description = shorten_text(strip_tags(post.get("text_html") or post.get("text") or ""), 180) or config.site_description
    first_photo = normalize_photo_entry((post.get("photos") or [None])[0])
    og_image = f"{root_prefix}{first_photo['full_url']}" if first_photo else f"{root_prefix}{config.avatar_path}"
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
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
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


def write_feed_files(config: SiteConfig, posts: list[dict[str, Any]], comments_enabled: bool) -> bool:
    changed = False
    total_pages = max(1, math.ceil(len(posts) / FEED_PAGE_SIZE))
    if write_json_if_changed(POSTS_PATH, build_feed_index_payload(config, posts, comments_enabled)):
        changed = True

    PAGES_DIR.mkdir(parents=True, exist_ok=True)
    for page in range(2, total_pages + 1):
        payload = build_feed_page_payload(page, posts, len(posts))
        if write_json_if_changed(PAGES_DIR / f"{page}.json", payload):
            changed = True

    changed = cleanup_removed_page_files(total_pages) or changed
    return changed


def write_post_detail_files(config: SiteConfig, posts: list[dict[str, Any]], comments_enabled: bool) -> bool:
    changed = False
    POST_DETAILS_DIR.mkdir(parents=True, exist_ok=True)
    POST_PAGES_DIR.mkdir(parents=True, exist_ok=True)

    for post in posts:
        payload = build_post_payload(config, post, comments_enabled)
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
    try:
        initial_page_html = fetch_page(config.channel_web_url)
    except Exception as error:  # pragma: no cover - network/runtime path
        if existing_payload.get("posts"):
            log.warning(
                "Failed to fetch initial page for @%s. Keeping existing mirror data: %s",
                config.channel_username,
                error,
            )
            return 0
        raise

    avatar_changed = mirror_channel_avatar(config, initial_page_html)
    parsed_channel_description = parse_channel_description(initial_page_html)
    if parsed_channel_description:
        config.site_description = parsed_channel_description
    posts = collect_posts(config, initial_page_html=initial_page_html)
    if not posts and existing_payload.get("posts"):
        log.warning("No posts were collected for @%s. Existing mirror data was left untouched.", config.channel_username)
        return 0

    reply_references = asyncio.run(fetch_reply_references_for_posts(config, posts))
    photo_overrides = asyncio.run(fetch_high_res_photos_for_posts(config, posts))
    comments_enabled, comment_results = asyncio.run(fetch_comments_for_posts(config, posts))

    for post in posts:
        reply_to = reply_references.get(post["id"]) or post.get("reply_to")
        if reply_to:
            post["reply_to"] = reply_to

    for post in posts:
        if post["id"] in comment_results:
            post["comments_count"] = len(comment_results[post["id"]])

    changes_detected = avatar_changed
    active_ids = {post["id"] for post in posts}
    changes_detected = mirror_post_photos(posts, photo_overrides=photo_overrides) or changes_detected
    changes_detected = cleanup_removed_comment_files(active_ids) or changes_detected

    for post_id, comments in comment_results.items():
        payload = {
            "generated_at": None,
            "post_id": post_id,
            "comments": comments,
        }
        if write_json_if_changed(COMMENTS_DIR / f"{post_id}.json", payload):
            changes_detected = True

    changes_detected = write_feed_files(config, posts, comments_enabled) or changes_detected
    changes_detected = cleanup_removed_post_detail_files(set()) or changes_detected

    log.info("Done. Material changes detected: %s", "yes" if changes_detected else "no")
    return 0


if __name__ == "__main__":
    sys.exit(main())
