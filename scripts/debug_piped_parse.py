import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.piped import parse_search_items


def main() -> int:
    raw = sys.stdin.read().strip()
    if not raw:
        print("No input on stdin", file=sys.stderr)
        return 1
    data = json.loads(raw)
    items = parse_search_items(data, limit=10)
    print(f"parsed={len(items)}")
    if items:
        first = items[0]
        print(
            f"first id={first.youtube_id} title={first.title!r} "
            f"duration={first.duration} views={first.view_count} thumb={first.thumbnail_url}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
