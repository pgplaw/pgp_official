from __future__ import annotations

import unittest

from scripts import sync_channel


class PostDeduplicationTests(unittest.TestCase):
    @staticmethod
    def build_forwarded_post(post_id: int, media_token: str, date: str = "2026-07-21T13:03:03+00:00") -> dict:
        return {
            "id": post_id,
            "tg_url": f"https://t.me/pgp_official/{post_id}",
            "date": date,
            "text": "Repeated forwarded album",
            "text_html": "<strong>Repeated forwarded album</strong>",
            "forwarded_from": {
                "source_url": "https://t.me/bankrotstvo_mustknow/547",
            },
            "photos": [
                {
                    "source_url": f"https://cdn4.telesco.pe/file/{media_token}.jpg",
                    "source_width": 1080,
                    "source_height": 1080,
                }
            ],
        }

    def test_deduplicates_repeated_forwards_with_different_media_urls(self) -> None:
        posts = [
            self.build_forwarded_post(1939, "copy-d"),
            self.build_forwarded_post(1938, "copy-c"),
            self.build_forwarded_post(1937, "copy-b"),
            self.build_forwarded_post(1936, "copy-a"),
        ]

        deduplicated = sync_channel.dedupe_posts(posts)

        self.assertEqual([post["id"] for post in deduplicated], [1939])

    def test_keeps_a_forward_republished_on_another_date(self) -> None:
        posts = [
            self.build_forwarded_post(1939, "new-copy", "2026-07-22T13:03:03+00:00"),
            self.build_forwarded_post(1936, "old-copy", "2026-07-21T13:03:03+00:00"),
        ]

        deduplicated = sync_channel.dedupe_posts(posts)

        self.assertEqual([post["id"] for post in deduplicated], [1939, 1936])


if __name__ == "__main__":
    unittest.main()
