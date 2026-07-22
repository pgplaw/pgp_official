from __future__ import annotations

import io
import json
import unittest
from unittest.mock import patch

from PIL import Image

from scripts import sync_channel


class CustomEmojiTests(unittest.TestCase):
    def test_fetches_webp_from_telegrams_public_widget_endpoint(self) -> None:
        source = Image.new("RGBA", (100, 100), (145, 39, 141, 255))
        source_buffer = io.BytesIO()
        source.save(source_buffer, format="WEBP", lossless=True)
        config = sync_channel.SiteConfig(
            channel_username="pgp_official",
            channel_title="Pepeliaev Group",
            site_name="Pepeliaev Group",
            site_description="Test",
            language="ru",
            accent_color="#91278f",
            background_color="#ffffff",
            avatar_path="assets/channel-avatar.jpg",
            messages_limit=100,
            recent_posts_months=3,
            comments_posts_limit=10,
            comments_max_age_days=7,
        )

        with (
            patch.object(
                sync_channel,
                "fetch_url",
                return_value=json.dumps(
                    {
                        "type": "webp",
                        "emoji": "https://cdn.example/custom.webp",
                        "thumb": "https://cdn.example/thumb.webp",
                    }
                ),
            ) as fetch_metadata,
            patch.object(sync_channel, "fetch_binary", return_value=source_buffer.getvalue()) as fetch_asset,
        ):
            preview = sync_channel.fetch_public_custom_emoji_preview(config, 5321286874256412860)

        self.assertIsNotNone(preview)
        fetch_metadata.assert_called_once()
        fetch_asset.assert_called_once()
        self.assertEqual(fetch_asset.call_args.args[0], "https://cdn.example/custom.webp")
        with Image.open(io.BytesIO(preview or b"")) as image:
            self.assertEqual(image.format, "WEBP")
            self.assertEqual(image.size, (100, 100))


if __name__ == "__main__":
    unittest.main()
