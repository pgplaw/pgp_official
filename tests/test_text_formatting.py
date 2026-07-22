from __future__ import annotations

import unittest

from scripts import sync_channel


class TelegramTextFormattingTests(unittest.TestCase):
    def test_preserves_supported_telegram_formatting(self) -> None:
        plain, markup = sync_channel.build_text_fields(
            "Обычный <b>жирный <i>и курсив</i></b><br>"
            "<u>подчеркнутый</u> <s>зачеркнутый</s> "
            "<code>код</code><blockquote>цитата</blockquote>"
            "<tg-spoiler>секрет</tg-spoiler>"
        )

        self.assertEqual(
            plain,
            "Обычный жирный и курсив\nподчеркнутый зачеркнутый кодцитата\nсекрет",
        )
        self.assertIn("<strong>жирный <em>и курсив</em></strong>", markup)
        self.assertIn("<u>подчеркнутый</u>", markup)
        self.assertIn("<s>зачеркнутый</s>", markup)
        self.assertIn("<code>код</code>", markup)
        self.assertIn("<blockquote>цитата</blockquote>", markup)
        self.assertIn('<span class="post-text-spoiler" tabindex="0">секрет</span>', markup)

    def test_keeps_safe_links_and_discards_unsafe_markup(self) -> None:
        plain, markup = sync_channel.build_text_fields(
            '<strong onclick="alert(1)">текст</strong>'
            '<script>alert(1)</script>'
            '<a href="javascript:alert(1)">опасная ссылка</a> '
            '<a href="https://example.com/path" style="color:red">сайт</a>'
        )

        self.assertNotIn("alert", plain)
        self.assertNotIn("script", markup.lower())
        self.assertNotIn("onclick", markup.lower())
        self.assertNotIn("javascript:", markup.lower())
        self.assertIn("опасная ссылка", markup)
        self.assertIn(
            '<a href="https://example.com/path" target="_blank" rel="noopener noreferrer">сайт</a>',
            markup,
        )
    def test_preserves_custom_emoji_id_until_asset_materialization(self) -> None:
        plain, markup = sync_channel.build_text_fields(
            '<tg-emoji emoji-id="5321286874256412860">'
            '<i class="emoji" style="background-image:url(emoji.png)"><b>umbrella</b></i>'
            '</tg-emoji>'
        )

        self.assertEqual(plain, "umbrella")
        self.assertEqual(
            markup,
            '<span class="post-custom-emoji" data-emoji-id="5321286874256412860">umbrella</span>',
        )

    def test_materializes_available_custom_emoji_and_keeps_fallback_for_missing_asset(self) -> None:
        posts = [
            {
                "text_html": (
                    '<span class="post-custom-emoji" data-emoji-id="100">one</span> '
                    '<span class="post-custom-emoji" data-emoji-id="200">two</span>'
                )
            }
        ]

        changed = sync_channel.materialize_custom_emoji_markup(posts, {100})

        self.assertTrue(changed)
        self.assertIn('class="post-custom-emoji"', posts[0]["text_html"])
        self.assertIn('data-emoji-id="100"', posts[0]["text_html"])
        self.assertIn('src="data/media/custom-emoji/100.webp"', posts[0]["text_html"])
        self.assertIn('alt="one"', posts[0]["text_html"])
        self.assertNotIn('data-emoji-id="200"', posts[0]["text_html"])
        self.assertTrue(posts[0]["text_html"].endswith(" two"))

if __name__ == "__main__":
    unittest.main()
