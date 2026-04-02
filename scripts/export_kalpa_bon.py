"""
Export kalpa-bon.com scraped data:
- One .txt and one .html.gz per page → output/kalpa-bon.com/pages/
- index.csv: one row per page
- transcriptions.csv: one row per page-side (page_id / transcription pairs)
"""

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

SITE = "kalpa-bon.com"
DATA_DIR = Path("data/webs") / SITE / "data"
OUT_DIR = Path("output") / SITE
TXT_DIR = OUT_DIR / "txt"
HTML_DIR = OUT_DIR / "html"


def url_to_slug(url: str) -> str:
    url = re.sub(r"^https?://", "", url)
    url = url.rstrip("/")
    slug = re.sub(r"[^a-zA-Z0-9\-_]", "_", url)
    return slug[:200]


def make_unique_slugs(pages: list[dict]) -> list[str]:
    """Assign unique slugs, appending _2, _3 etc. for collisions."""
    raw = [url_to_slug(p["url"]) for p in pages]
    counts: Counter = Counter()
    seen: Counter = Counter()
    for s in raw:
        counts[s] += 1
    result = []
    for s in raw:
        if counts[s] == 1:
            result.append(s)
        else:
            seen[s] += 1
            result.append(f"{s}_{seen[s]}")
    return result


def parse_transcriptions(text: str) -> list[dict]:
    """
    Extract page_id / image_ref / transcription triples.

    Two marker formats exist on the site:
      - With image ref:  '00081_0001r (DSC_2908)'  — used on transcribed pages
      - Bare:            '00207_0001r'              — used on pages not yet transcribed

    In both cases the markers appear twice: first as a linked table-of-contents
    list, then again inline where transcription text (if any) follows each marker.
    We collect the second occurrence of each marker and take the text up to the
    next marker as the transcription (may be empty for un-transcribed pages).
    """
    # Try the richer pattern (with image ref) first
    rich_pattern = re.compile(r"(\d{5}_\d{4}[rv])\s*\(([^)]+)\)")
    rich_matches = list(rich_pattern.finditer(text))

    if rich_matches:
        by_pid: dict[str, list] = defaultdict(list)
        for m in rich_matches:
            by_pid[m.group(1)].append(m)
        second: list[tuple[int, int, str, str]] = []
        for pid, matches in by_pid.items():
            if len(matches) >= 2:
                m = matches[1]
                second.append((m.start(), m.end(), pid, m.group(2)))
    else:
        # Bare page IDs only — transcription text will be empty
        bare_pattern = re.compile(r"(\d{5}_\d{4}[rv])")
        bare_matches = list(bare_pattern.finditer(text))
        by_pid = defaultdict(list)
        for m in bare_matches:
            by_pid[m.group(1)].append(m)
        second = []
        for pid, matches in by_pid.items():
            if len(matches) >= 2:
                m = matches[1]
                second.append((m.start(), m.end(), pid, ""))

    second.sort(key=lambda x: x[0])

    # Trim the trailing footer from the last transcription block
    footer_pattern = re.compile(r"\s*©\s*\d{4}|\s*Back to top", re.IGNORECASE)

    pairs = []
    for i, (start, end, pid, img) in enumerate(second):
        next_start = second[i + 1][0] if i + 1 < len(second) else len(text)
        transcription = text[end:next_start]
        # Strip footer noise from the last block
        transcription = footer_pattern.split(transcription)[0].strip()
        pairs.append({"page_id": pid, "image_ref": img, "transcription": transcription})

    return pairs


def main():
    TXT_DIR.mkdir(parents=True, exist_ok=True)
    HTML_DIR.mkdir(parents=True, exist_ok=True)

    with open(DATA_DIR / "crawl_index.json", encoding="utf-8") as f:
        index = json.load(f)

    pages = index["pages"]
    slugs = make_unique_slugs(pages)

    index_rows = []
    transcription_rows = []

    for page_meta, slug in zip(pages, slugs):
        page_file = DATA_DIR / "pages" / page_meta["filename"]
        if not page_file.exists():
            print(f"  [MISSING] {page_file.name}", file=sys.stderr)
            continue

        with open(page_file, encoding="utf-8") as f:
            page = json.load(f)

        url = page["url"]
        title = page.get("title", "")
        text = page.get("text", "")
        html = page.get("html", "")

        # Write .txt
        (TXT_DIR / f"{slug}.txt").write_text(text, encoding="utf-8")

        # Write .html
        (HTML_DIR / f"{slug}.html").write_text(html, encoding="utf-8")

        index_rows.append(
            {
                "url": url,
                "title": title,
                "slug": slug,
                "filename_txt": f"txt/{slug}.txt",
                "filename_html": f"html/{slug}.html",
            }
        )

        # Parse transcriptions only for leaf text pages
        is_text_leaf = (
            "/texts/" in url
            and not url.rstrip("/").endswith("/texts")
            and not any(
                url.rstrip("/").endswith(f"/texts/{cat}")
                for cat in ("bla", "leu", "phya-gyang", "protectors", "sri",
                            "uncategorised", "yi-dam", "mdos-rgyab", "mgo-gsum")
            )
        )
        if is_text_leaf:
            pairs = parse_transcriptions(text)
            for pair in pairs:
                transcription_rows.append(
                    {
                        "url": url,
                        "title": title,
                        "slug": slug,
                        "page_id": pair["page_id"],
                        "image_ref": pair["image_ref"],
                        "transcription": pair["transcription"],
                    }
                )

    # Write index.csv
    index_csv = OUT_DIR / "index.csv"
    with open(index_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "title", "slug", "filename_txt", "filename_html"])
        writer.writeheader()
        writer.writerows(index_rows)

    # Write transcriptions.csv
    trans_csv = OUT_DIR / "transcriptions.csv"
    with open(trans_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "title", "slug", "page_id", "image_ref", "transcription"])
        writer.writeheader()
        writer.writerows(transcription_rows)

    print(f"kalpa-bon: {len(index_rows)} pages exported, {len(transcription_rows)} transcription rows")
    print(f"  index:          {index_csv}")
    print(f"  transcriptions: {trans_csv}")
    print(f"  txt dir:        {TXT_DIR}")
    print(f"  html dir:       {HTML_DIR}")


if __name__ == "__main__":
    main()
