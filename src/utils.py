from __future__ import annotations


def format_duration(seconds: int | None) -> str:
    if seconds is None:
        return "?"
    if seconds < 0:
        return "?"
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def truncate_text(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3].rstrip() + "..."


def parse_time_range(value: str) -> tuple[int, int] | None:
    raw = value.strip()
    if "-" not in raw:
        return None
    start_raw, end_raw = (part.strip() for part in raw.split("-", 1))
    if not start_raw or not end_raw:
        return None

    def parse_part(part: str) -> int | None:
        if ":" in part:
            chunks = part.split(":")
            if len(chunks) != 2:
                return None
            mm, ss = chunks
            if not (mm.isdigit() and ss.isdigit()):
                return None
            return int(mm) * 60 + int(ss)
        if part.isdigit():
            return int(part)
        return None

    start = parse_part(start_raw)
    end = parse_part(end_raw)
    if start is None or end is None:
        return None
    return start, end
