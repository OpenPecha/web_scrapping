"""
Export www.dharmadownload.net scraped data:
- Scans page files directly (crawl_index may list 0 pages)
- Clean text: strip navigation, JavaScript, and site chrome using BeautifulSoup
- One .txt and one .html per page → output/www.dharmadownload.net/
- index.csv: one row per page
"""

import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path

from bs4 import BeautifulSoup

SITE = "www.dharmadownload.net"
DATA_DIR = Path("data/webs") / SITE / "data"
OUT_DIR = Path("output") / SITE
TXT_DIR = OUT_DIR / "txt"
HTML_DIR = OUT_DIR / "html"

MIN_TEXT_LENGTH = 50  # characters of meaningful text content


def clean_text_from_html(html: str) -> str:
    """Extract clean visible text from HTML, removing nav/JS/site chrome."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove script, style, noscript, iframe tags entirely
    for tag in soup.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()

    # Remove navigation / site chrome elements
    for tag in soup.find_all(["nav", "header", "footer"]):
        tag.decompose()

    # Remove elements by class/id patterns
    nav_class_re = re.compile(
        r"(nav|menu|header|footer|sidebar|breadcrumb|topbar|toolbar|"
        r"banner|cookie|popup)", re.I
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


def load_pages() -> list[dict]:
    """Load pages from crawl_index, falling back to scanning the pages dir."""
    index_file = DATA_DIR / "crawl_index.json"
    if index_file.exists():
        with open(index_file, encoding="utf-8") as f:
            index = json.load(f)
        if index.get("pages"):
            return index["pages"]

    # Crawl index is empty — scan pages directory directly
    pages_dir = DATA_DIR / "pages"
    if not pages_dir.exists():
        print("  [ERROR] No pages directory found", file=sys.stderr)
        return []

    pages = []
    for page_file in sorted(pages_dir.glob("*.json")):
        pages.append({"filename": page_file.name, "url": ""})
    return pages


def main():
    TXT_DIR.mkdir(parents=True, exist_ok=True)
    HTML_DIR.mkdir(parents=True, exist_ok=True)

    pages = load_pages()
    if not pages:
        print("dharmadownload: no pages found", file=sys.stderr)
        return

    index_rows = []
    skipped = 0

    # First pass: load all pages to get URLs for slug generation
    loaded_pages = []
    for page_meta in pages:
        page_file = DATA_DIR / "pages" / page_meta["filename"]
        if not page_file.exists():
            print(f"  [MISSING] {page_file.name}", file=sys.stderr)
            continue
        try:
            with open(page_file, encoding="utf-8", errors="replace") as f:
                page = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"  [ERROR] {page_file.name}: {e}", file=sys.stderr)
            continue
        loaded_pages.append(page)

    slugs = make_unique_slugs(loaded_pages)

    for page, slug in zip(loaded_pages, slugs):
        url = page["url"]
        title = page.get("title", "")
        html = page.get("html", "")
        raw_text = page.get("text", "")

        # Clean text from HTML to remove nav/JS/chrome
        text = clean_text_from_html(html) if html else raw_text

        if len(text) < MIN_TEXT_LENGTH:
            skipped += 1
            continue

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

    index_csv = OUT_DIR / "index.csv"
    with open(index_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["url", "title", "slug", "filename_txt", "filename_html"]
        )
        writer.writeheader()
        writer.writerows(index_rows)

    print(f"dharmadownload: {len(index_rows)} pages exported, {skipped} skipped (too short)")
    print(f"  index:    {index_csv}")
    print(f"  txt dir:  {TXT_DIR}")
    print(f"  html dir: {HTML_DIR}")


if __name__ == "__main__":
    main()
