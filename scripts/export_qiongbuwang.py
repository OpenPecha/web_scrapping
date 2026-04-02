"""
Export xz.qiongbuwang.com scraped data:
- Clean text: strip navigation, JavaScript, and site chrome using BeautifulSoup
- One .txt and one .html per page → output/xz.qiongbuwang.com/
- index.csv: one row per page
"""

import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path

from bs4 import BeautifulSoup

SITE = "xz.qiongbuwang.com_view_302.html"
OUT_SITE = "xz.qiongbuwang.com"
DATA_DIR = Path("data/webs") / SITE / "data"
OUT_DIR = Path("output") / OUT_SITE
TXT_DIR = OUT_DIR / "txt"
HTML_DIR = OUT_DIR / "html"


def clean_text_from_html(html: str) -> str:
    """Extract clean visible text from HTML, targeting the main content area."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove script, style, noscript, iframe tags entirely
    for tag in soup.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()

    # Try to find the main content area (class="main-bd" on qiongbuwang)
    content_area = soup.find(attrs={"class": re.compile(r"^main-bd$", re.I)})

    if content_area:
        text = content_area.get_text(separator="\n")
    else:
        # Fallback: strip navigation / site chrome
        for tag in soup.find_all(["nav", "header", "footer"]):
            tag.decompose()
        nav_class_re = re.compile(
            r"(nav|menu|header|footer|sidebar|breadcrumb|topbar|toolbar|"
            r"banner|bottomnav|copy|gongxiang|jiathis|carousel|cookie|popup)", re.I
        )
        for tag in soup.find_all(attrs={"class": nav_class_re}):
            tag.decompose()
        for tag in soup.find_all(attrs={"id": nav_class_re}):
            tag.decompose()
        text = soup.get_text(separator="\n")

    # Collapse blank lines and whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]*\n+", "\n\n", text)
    return text.strip()


def url_to_slug(url: str) -> str:
    url = re.sub(r"^https?://", "", url)
    url = url.rstrip("/")
    slug = re.sub(r"[^a-zA-Z0-9\-_]", "_", url)
    return slug[:200]


def make_unique_slugs(pages: list[dict]) -> list[str]:
    raw = [url_to_slug(p["url"]) for p in pages]
    counts: Counter = Counter(raw)
    seen: Counter = Counter()
    result = []
    for s in raw:
        if counts[s] == 1:
            result.append(s)
        else:
            seen[s] += 1
            result.append(f"{s}_{seen[s]}")
    return result


def main():
    TXT_DIR.mkdir(parents=True, exist_ok=True)
    HTML_DIR.mkdir(parents=True, exist_ok=True)

    with open(DATA_DIR / "crawl_index.json", encoding="utf-8") as f:
        index = json.load(f)

    pages = index["pages"]
    slugs = make_unique_slugs(pages)

    index_rows = []

    for page_meta, slug in zip(pages, slugs):
        page_file = DATA_DIR / "pages" / page_meta["filename"]
        if not page_file.exists():
            print(f"  [MISSING] {page_file.name}", file=sys.stderr)
            continue

        with open(page_file, encoding="utf-8") as f:
            page = json.load(f)

        url = page["url"]
        title = page.get("title", "")
        meta_description = page.get("meta_description", "")
        html = page.get("html", "")
        raw_text = page.get("text", "")

        # Clean text from HTML to remove nav/JS/chrome
        text = clean_text_from_html(html) if html else raw_text

        # Write .txt
        (TXT_DIR / f"{slug}.txt").write_text(text, encoding="utf-8")

        # Write .html
        (HTML_DIR / f"{slug}.html").write_text(html, encoding="utf-8")

        index_rows.append(
            {
                "url": url,
                "title": title,
                "meta_description": meta_description,
                "slug": slug,
                "filename_txt": f"txt/{slug}.txt",
                "filename_html": f"html/{slug}.html",
            }
        )

    index_csv = OUT_DIR / "index.csv"
    with open(index_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["url", "title", "meta_description", "slug", "filename_txt", "filename_html"]
        )
        writer.writeheader()
        writer.writerows(index_rows)

    print(f"qiongbuwang: {len(index_rows)} pages exported")
    print(f"  index:    {index_csv}")
    print(f"  txt dir:  {TXT_DIR}")
    print(f"  html dir: {HTML_DIR}")


if __name__ == "__main__":
    main()
