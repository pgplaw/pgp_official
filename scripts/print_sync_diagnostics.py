from __future__ import annotations

import json
from pathlib import Path


def main() -> int:
    base = Path("docs/data/channels")
    print("== git status ==")

    if not base.exists():
        print(f"{base}: missing")
        return 0

    print("== channel feeds ==")
    for posts_path in sorted(base.glob("*/posts.json")):
        try:
            payload = json.loads(posts_path.read_text("utf-8"))
            generated_at = payload.get("generated_at")
            total_posts = payload.get("pagination", {}).get("total_posts")
            print(f"{posts_path}: generated_at={generated_at}, total_posts={total_posts}")
        except Exception as error:  # pragma: no cover - diagnostics only
            print(f"{posts_path}: failed to read ({error})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
