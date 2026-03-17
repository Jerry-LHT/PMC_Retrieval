from __future__ import annotations

from datetime import UTC, datetime


DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y %m %d",
    "%Y-%m",
    "%Y/%m",
    "%Y",
)


def parse_publication_date(value: object) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if text == "" or text.lower() in {"none", "null", "nan"}:
        return None

    if text.isdigit() and len(text) in {9, 10}:
        try:
            epoch = int(text)
            dt = datetime.fromtimestamp(epoch, tz=UTC)
            return dt.date().isoformat()
        except (OverflowError, ValueError, OSError):
            return None

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
            if fmt in {"%Y", "%Y-%m"}:
                # PubMed-style loose date tokens are normalized to month/day 01.
                if fmt == "%Y":
                    return f"{dt.year:04d}-01-01"
                return f"{dt.year:04d}-{dt.month:02d}-01"
            return dt.date().isoformat()
        except ValueError:
            continue

    return None
