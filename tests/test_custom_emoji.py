from __future__ import annotations

import io
import json
import colorsys
import unittest
from unittest.mock import patch

from PIL import Image

from scripts import sync_channel


class CustomEmojiTests(unittest.TestCase):
    @staticmethod
    def build_webp(colors: list[tuple[int, int, int, int]]) -> bytes:
        image = Image.new("RGBA", (len(colors), 1))
        image.putdata(colors)
        output = io.BytesIO()
        image.save(output, format="WEBP", lossless=True)
        return output.getvalue()

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

    def test_themes_monochrome_custom_emoji_to_channel_accent(self) -> None:
        source = self.build_webp([(145, 39, 141, 255)] * 32)

        themed = sync_channel.theme_monochrome_custom_emoji_preview(source, "#0060a0")

        self.assertNotEqual(themed, source)
        with Image.open(io.BytesIO(themed)) as image:
            red, green, blue, _ = image.convert("RGBA").getpixel((0, 0))
        themed_hue = colorsys.rgb_to_hls(red / 255, green / 255, blue / 255)[0]
        target_hue = colorsys.rgb_to_hls(0, 96 / 255, 160 / 255)[0]
        self.assertAlmostEqual(themed_hue, target_hue, delta=0.01)

    def test_keeps_multicolor_custom_emoji_unchanged(self) -> None:
        source = self.build_webp(
            [(230, 30, 40, 255)] * 16 + [(30, 190, 80, 255)] * 16
        )

        themed = sync_channel.theme_monochrome_custom_emoji_preview(source, "#0060a0")

        self.assertEqual(themed, source)

    def test_custom_emoji_theming_is_idempotent(self) -> None:
        source = self.build_webp([(145, 39, 141, 255)] * 32)

        first_pass = sync_channel.theme_monochrome_custom_emoji_preview(source, "#408060")
        second_pass = sync_channel.theme_monochrome_custom_emoji_preview(first_pass, "#408060")

        self.assertEqual(second_pass, first_pass)


if __name__ == "__main__":
    unittest.main()
