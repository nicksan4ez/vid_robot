import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.piped import PipedClient  # noqa: E402


async def main() -> int:
    load_dotenv()
    query = sys.argv[1] if len(sys.argv) > 1 else "тест"
    base = os.getenv("PIPED_API_BASE_URL", "").strip()
    if not base:
        print("PIPED_API_BASE_URL is required", file=sys.stderr)
        return 1
    timeout_raw = os.getenv("PIPED_TIMEOUT_SECONDS", "4")
    try:
        timeout = float(timeout_raw)
    except ValueError:
        timeout = 4.0

    client = PipedClient(base, timeout)
    results = await client.search(query, 10)
    print(f"base={base}")
    print(f"results={len(results)}")
    if results:
        first = results[0]
        print(
            f"first id={first.youtube_id} title={first.title!r} "
            f"duration={first.duration} views={first.view_count}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
