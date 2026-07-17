from __future__ import annotations

import io
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from PIL import Image

from scripts import sync_channel


class ImageVariantTests(unittest.TestCase):
    def test_low_resolution_gallery_image_gets_retina_full_variant(self) -> None:
        source = Image.new("RGB", (800, 450), "#9f1b87")
        source_bytes = io.BytesIO()
        source.save(source_bytes, format="JPEG", quality=92)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            full_path = output_dir / "full.jpg"
            feed_path = output_dir / "feed.jpg"
            thumb_path = output_dir / "thumb.jpg"

            changed = sync_channel.optimize_image_variants(
                source_bytes.getvalue(),
                full_path,
                feed_path,
                thumb_path,
                include_feed_variant=False,
            )

            self.assertTrue(changed)
            self.assertTrue(full_path.exists())
            self.assertTrue(thumb_path.exists())
            self.assertFalse(feed_path.exists())

            with Image.open(full_path) as full_image:
                self.assertEqual(full_image.size, (1880, 1058))
            with Image.open(thumb_path) as thumb_image:
                self.assertEqual(thumb_image.size, (800, 450))

    def test_gallery_post_page_declares_full_mobile_width(self) -> None:
        photo = {
            "thumb_url": "media/thumb.jpg",
            "thumb_width": 800,
            "thumb_height": 450,
            "full_url": "media/full.jpg",
            "full_width": 1880,
            "full_height": 1058,
        }

        markup = sync_channel.render_post_page_media({
            "photos": [photo, dict(photo, full_url="media/full-2.jpg")],
            "videos": [],
        })

        self.assertIn(
            'sizes="(max-width: 860px) calc(100vw - 44px), 520px"',
            markup,
        )

    def test_legacy_gallery_uses_local_asset_when_remote_source_is_present(self) -> None:
        source = Image.new("RGB", (800, 450), "#9f1b87")
        source_bytes = io.BytesIO()
        source.save(source_bytes, format="JPEG", quality=92)

        with tempfile.TemporaryDirectory() as temp_dir:
            docs_dir = Path(temp_dir) / "docs"
            posts_dir = docs_dir / "media" / "posts"
            thumbs_dir = posts_dir / "thumbs"
            feed_dir = posts_dir / "feed"
            thumbs_dir.mkdir(parents=True)

            photos = []
            for index in (1, 2):
                file_name = f"42-{index}-legacy-v8.jpg"
                full_url = f"media/posts/{file_name}"
                thumb_url = f"media/posts/thumbs/{file_name}"
                (docs_dir / full_url).write_bytes(source_bytes.getvalue())
                (docs_dir / thumb_url).write_bytes(source_bytes.getvalue())
                photos.append({
                    "thumb_url": thumb_url,
                    "full_url": full_url,
                    "source_url": f"https://expired.example/{index}.jpg",
                })

            posts = [{
                "id": 42,
                "date": "2020-01-01T00:00:00+00:00",
                "photos": photos,
            }]

            with (
                patch.object(sync_channel, "DOCS_DIR", docs_dir),
                patch.object(sync_channel, "POSTS_MEDIA_DIR", posts_dir),
                patch.object(sync_channel, "POSTS_THUMBS_DIR", thumbs_dir),
                patch.object(sync_channel, "POSTS_FEED_DIR", feed_dir),
            ):
                changed = sync_channel.mirror_post_photos(
                    posts,
                    existing_posts=deepcopy(posts),
                )

            self.assertTrue(changed)
            self.assertEqual(len(posts[0]["photos"]), 2)
            self.assertTrue(all("-v9.jpg" in photo["full_url"] for photo in posts[0]["photos"]))
            self.assertTrue(all(photo.get("full_width") == 1880 for photo in posts[0]["photos"]))


if __name__ == "__main__":
    unittest.main()
