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
    def test_handles_text_that_is_empty_after_custom_emoji_cleanup(self) -> None:
        plain, markup = sync_channel.build_text_fields(
            '<tg-emoji emoji-id="1"><i class="emoji" style="background-image:url(emoji.png)"></i></tg-emoji>'
        )

        self.assertIsNone(plain)
        self.assertIsNone(markup)


if __name__ == "__main__":
    unittest.main()
