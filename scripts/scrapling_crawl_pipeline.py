"""Scrapling-based full-site crawl pipeline.

Reads site URLs from a text file, crawls each site using Scrapling's Spider
framework, downloads images and PDFs, saves video URLs, and writes per-site
JSON outputs.

Usage:
    python scripts/scrapling_crawl_pipeline.py
    python scripts/scrapling_crawl_pipeline.py --sites-file data/sites.txt --output-dir output_scrapling
    python scripts/scrapling_crawl_pipeline.py --stealth --concurrency 3
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import logging
import re
import shutil
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse
from urllib.request import Request as URLRequest
from urllib.request import urlopen

from scrapling.fetchers import FetcherSession
from scrapling.spiders import Request, Response, Spider

logger = logging.getLogger("scrapling_pipeline")

VIDEO_EXTENSIONS = frozenset({
    ".mp4", ".webm", ".avi", ".mov", ".mkv", ".flv", ".wmv", ".m4v", ".ogv",
})
IMAGE_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".ico", ".tiff",
    ".tif", ".avif",
})
PDF_EXTENSION = ".pdf"

VIDEO_EMBED_DOMAINS = frozenset({
    "youtube.com", "www.youtube.com", "youtu.be",
    "vimeo.com", "player.vimeo.com",
    "dailymotion.com", "www.dailymotion.com",
})


@dataclass
class PipelineConfig:
    sites_file: str = "data/sites.txt"
    output_dir: str = "output_scrapling"
    concurrency: int = 2
    concurrent_requests: int = 8
    download_delay: float = 0.5
    max_pages: int = 5000
    stealth: bool = False
    download_workers: int = 4
    timeout: int = 30
    retries: int = 3
    backoff_seconds: int = 2
    crawldir: Optional[str] = None
    spider_timeout: int = 60
    force: bool = False


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify_site(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc or parsed.path
    path = parsed.path.strip("/")
    joined = f"{host}_{path}" if path else host
    clean = re.sub(r"[^a-zA-Z0-9._-]+", "_", joined).strip("_").lower()
    return clean or "site"


def read_sites(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Sites file not found: {path}")
    sites: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        sites.append(value)
    if not sites:
        raise ValueError(f"No sites found in file: {path}")
    return sites


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


def _get_url_extension(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.lower().rstrip("/")
    if "." in path.split("/")[-1]:
        return "." + path.rsplit(".", 1)[-1]
    return ""


def _is_video_embed(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    return any(host == d or host.endswith("." + d) for d in VIDEO_EMBED_DOMAINS)


def classify_asset(url: str) -> str:
    """Classify a URL as 'image', 'pdf', 'video', or 'other'."""
    if _is_video_embed(url):
        return "video"
    ext = _get_url_extension(url)
    if ext in IMAGE_EXTENSIONS:
        return "image"
    if ext == PDF_EXTENSION:
        return "pdf"
    if ext in VIDEO_EXTENSIONS:
        return "video"
    return "other"


def extract_asset_urls(html: str, page_url: str) -> Dict[str, List[str]]:
    """Extract and classify asset URLs from HTML content.

    Returns a dict with keys 'images', 'pdfs', 'videos' each mapping to
    a deduplicated list of absolute URLs.
    """
    if not html:
        return {"images": [], "pdfs": [], "videos": []}

    raw_urls: Set[str] = set()

    for match in re.finditer(r'<img[^>]+src\s*=\s*["\']([^"\']+)["\']', html, re.I):
        raw_urls.add(match.group(1))

    for match in re.finditer(r'<img[^>]+srcset\s*=\s*["\']([^"\']+)["\']', html, re.I):
        for candidate in match.group(1).split(","):
            part = candidate.strip().split()[0]
            if part:
                raw_urls.add(part)

    for match in re.finditer(
        r'<(?:picture|source)[^>]+(?:src|srcset)\s*=\s*["\']([^"\']+)["\']', html, re.I
    ):
        for candidate in match.group(1).split(","):
            part = candidate.strip().split()[0]
            if part:
                raw_urls.add(part)

    for match in re.finditer(r'<a[^>]+href\s*=\s*["\']([^"\']+)["\']', html, re.I):
        href = match.group(1)
        ext = _get_url_extension(href)
        if ext in (PDF_EXTENSION,) or ext in IMAGE_EXTENSIONS or ext in VIDEO_EXTENSIONS:
            raw_urls.add(href)

    for match in re.finditer(
        r'<(?:embed|object)[^>]+(?:src|data)\s*=\s*["\']([^"\']+)["\']', html, re.I
    ):
        raw_urls.add(match.group(1))

    for match in re.finditer(
        r'<video[^>]+src\s*=\s*["\']([^"\']+)["\']', html, re.I
    ):
        raw_urls.add(match.group(1))

    for match in re.finditer(
        r'<video[^>]*>.*?</video>', html, re.I | re.DOTALL
    ):
        video_block = match.group(0)
        for src_match in re.finditer(
            r'<source[^>]+src\s*=\s*["\']([^"\']+)["\']', video_block, re.I
        ):
            raw_urls.add(src_match.group(1))
        poster_match = re.search(
            r'poster\s*=\s*["\']([^"\']+)["\']', video_block, re.I
        )
        if poster_match:
            raw_urls.add(poster_match.group(1))

    for match in re.finditer(
        r'<iframe[^>]+src\s*=\s*["\']([^"\']+)["\']', html, re.I
    ):
        url_val = match.group(1)
        if _is_video_embed(url_val):
            raw_urls.add(url_val)

    images: List[str] = []
    pdfs: List[str] = []
    videos: List[str] = []
    seen: Set[str] = set()

    for raw in raw_urls:
        if raw.startswith(("javascript:", "data:", "#", "mailto:", "tel:")):
            continue
        absolute = urljoin(page_url, raw)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)

        category = classify_asset(absolute)
        if category == "image":
            images.append(absolute)
        elif category == "pdf":
            pdfs.append(absolute)
        elif category == "video":
            videos.append(absolute)

    return {"images": images, "pdfs": pdfs, "videos": videos}


def _extension_from_content_type(content_type: str) -> str:
    if not content_type:
        return ""
    mapping = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/svg+xml": ".svg",
        "image/bmp": ".bmp",
        "image/x-icon": ".ico",
        "image/avif": ".avif",
        "application/pdf": ".pdf",
    }
    low = content_type.lower().split(";")[0].strip()
    return mapping.get(low, "")


def _safe_asset_filename(asset_url: str, content_type: str = "") -> str:
    parsed = urlparse(asset_url)
    original_name = Path(parsed.path).name
    ext = Path(original_name).suffix or _extension_from_content_type(content_type)
    digest = hashlib.sha1(asset_url.encode("utf-8")).hexdigest()[:16]
    return f"{digest}{ext}"


def _page_filename(page_url: str) -> str:
    """Generate a stable, filesystem-safe filename for a page URL."""
    digest = hashlib.sha1(page_url.encode("utf-8")).hexdigest()[:16]
    parsed = urlparse(page_url)
    path = parsed.path.strip("/").replace("/", "_")
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "_", path).strip("_").lower()
    if slug:
        slug = slug[:80]
        return f"{slug}_{digest}.json"
    return f"{digest}.json"


def download_single_asset(
    asset_url: str,
    dest_dir: Path,
    timeout: int = 30,
    retries: int = 3,
    backoff_seconds: int = 2,
) -> Dict[str, Any]:
    """Download a single asset file with retries."""
    attempt = 0
    while True:
        attempt += 1
        try:
            req = URLRequest(
                url=asset_url,
                method="GET",
                headers={"User-Agent": "Mozilla/5.0 (compatible; ScraplingPipeline/1.0)"},
            )
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                content_type = resp.headers.get("Content-Type", "")
                filename = _safe_asset_filename(asset_url, content_type)
                local_path = dest_dir / filename
                local_path.write_bytes(body)
                return {
                    "original_url": asset_url,
                    "local_path": str(local_path),
                    "content_type": content_type,
                    "size_bytes": len(body),
                    "status": "downloaded",
                    "error": None,
                }
        except Exception as exc:
            if attempt > retries:
                return {
                    "original_url": asset_url,
                    "local_path": None,
                    "content_type": None,
                    "size_bytes": 0,
                    "status": "failed",
                    "error": str(exc),
                }
            time.sleep(backoff_seconds * attempt)


def download_assets(
    urls: List[str],
    dest_dir: Path,
    max_workers: int = 4,
    timeout: int = 30,
    retries: int = 3,
    backoff_seconds: int = 2,
) -> List[Dict[str, Any]]:
    """Download a list of asset URLs concurrently."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    if not urls:
        return []

    manifest: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                download_single_asset, url, dest_dir, timeout, retries, backoff_seconds
            ): url
            for url in urls
        }
        for future in concurrent.futures.as_completed(future_map):
            result = future.result()
            manifest.append(result)
            status = result["status"]
            url = result["original_url"]
            if status == "downloaded":
                logger.info("Downloaded: %s (%d bytes)", url, result["size_bytes"])
            else:
                logger.warning("Failed: %s - %s", url, result.get("error"))

    return manifest


class SiteCrawlerSpider(Spider):
    """Scrapling Spider that crawls an entire website.

    Extracts page content (title, text, HTML) and collects asset URLs
    (images, PDFs, videos) from every page it visits.
    """

    name = "site_crawler"
    concurrent_requests = 8
    download_delay = 0.5
    max_blocked_retries = 2

    def __init__(
        self,
        site_url: str,
        *,
        stealth: bool = False,
        concurrent_reqs: int = 8,
        delay: float = 0.5,
        crawldir: Optional[str] = None,
        spider_timeout: int = 60,
    ):
        self._site_url = site_url
        self._stealth = stealth
        self._spider_timeout = spider_timeout
        self._pages_crawled = 0
        self._errors: List[Dict[str, str]] = []

        parsed = urlparse(site_url)
        self.start_urls = [site_url]
        self.allowed_domains = {parsed.netloc}
        self.name = f"crawler_{slugify_site(site_url)}"
        self.concurrent_requests = concurrent_reqs
        self.download_delay = delay

        super().__init__(crawldir=crawldir)

    def configure_sessions(self, manager):
        if self._stealth:
            from scrapling.fetchers import AsyncStealthySession

            manager.add(
                "stealth",
                AsyncStealthySession(headless=True, timeout=self._spider_timeout * 1000),
            )
        else:
            manager.add(
                "default",
                FetcherSession(impersonate="chrome", timeout=self._spider_timeout),
            )

    async def parse(self, response: Response):
        self._pages_crawled += 1
        page_url = response.request.url if response.request else self._site_url

        title = response.css("title::text").get("") or ""
        meta_desc = ""
        meta_el = response.css('meta[name="description"]')
        if meta_el:
            meta_desc = meta_el[0].attrib.get("content", "")

        text_parts = response.css("body *::text").getall()
        text_content = " ".join(t.strip() for t in text_parts if t.strip())

        html_content = ""
        if response.body:
            html_content = (
                response.body.decode("utf-8", errors="replace")
                if isinstance(response.body, bytes)
                else str(response.body)
            )

        assets = extract_asset_urls(html_content, page_url)

        yield {
            "url": page_url,
            "title": title.strip(),
            "meta_description": meta_desc.strip(),
            "text": text_content,
            "html": html_content,
            "assets": assets,
        }

        for href in response.css("a::attr(href)").getall():
            if not href:
                continue
            abs_url = urljoin(page_url, href)
            parsed = urlparse(abs_url)
            if parsed.fragment and not parsed.path:
                continue
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if parsed.query:
                clean_url += f"?{parsed.query}"
            if parsed.netloc in self.allowed_domains:
                yield Request(clean_url, callback=self.parse)

    async def on_scraped_item(self, item: Dict[str, Any]) -> Dict[str, Any] | None:
        logger.info(
            "[%s] Scraped page %d: %s",
            self.name,
            self._pages_crawled,
            item.get("url", "?"),
        )
        return item

    async def on_error(self, request: Request, error: Exception) -> None:
        logger.error("[%s] Error fetching %s: %s", self.name, request.url, error)
        self._errors.append({"url": request.url, "error": str(error)})


def _aggregate_assets(items: List[Dict[str, Any]]) -> Tuple[List[str], List[str], List[str]]:
    """Collect and deduplicate all asset URLs across crawled pages."""
    all_images: Set[str] = set()
    all_pdfs: Set[str] = set()
    all_videos: Set[str] = set()

    for item in items:
        assets = item.get("assets", {})
        all_images.update(assets.get("images", []))
        all_pdfs.update(assets.get("pdfs", []))
        all_videos.update(assets.get("videos", []))

    return sorted(all_images), sorted(all_pdfs), sorted(all_videos)


def process_site(
    site_url: str,
    output_dir: str,
    config: PipelineConfig,
) -> Dict[str, Any]:
    """Orchestrate spider crawl + asset download for a single site.

    If the initial crawl yields no pages and indicates a failed request,
    the site is automatically retried once using stealth mode, which is
    more resilient to aggressive anti-bot protections.
    """
    site_slug = slugify_site(site_url)
    site_dir = Path(output_dir) / site_slug
    data_dir = site_dir / "data"
    pages_dir = data_dir / "pages"
    images_dir = site_dir / "assets" / "images"
    pdfs_dir = site_dir / "assets" / "pdfs"

    index_path = data_dir / "crawl_index.json"
    if index_path.exists():
        try:
            existing = json.loads(index_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        if existing.get("total_pages", 0) > 0 and not config.force:
            logger.info("[skip] Existing crawl index found for %s", site_url)
            return {"site_url": site_url, "status": "skipped", "path": str(index_path)}
        logger.info(
            "[retry] Previous crawl got 0 pages for %s; removing old output and re-crawling",
            site_url,
        )
        shutil.rmtree(site_dir, ignore_errors=True)

    for d in (pages_dir, images_dir, pdfs_dir):
        d.mkdir(parents=True, exist_ok=True)

    crawldir_path = None
    if config.crawldir:
        crawldir_path = str(Path(config.crawldir) / site_slug)

    started_at = utcnow_iso()
    logger.info("[crawl] Starting spider for %s (stealth=%s)", site_url, config.stealth)

    def _run_spider(stealth: bool) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[Dict[str, str]]]:
        spider = SiteCrawlerSpider(
            site_url,
            stealth=stealth,
            concurrent_reqs=config.concurrent_requests,
            delay=config.download_delay,
            crawldir=crawldir_path,
            spider_timeout=config.spider_timeout,
        )
        try:
            result = spider.start()
        except Exception as exc:
            logger.error("[crawl] Spider failed for %s (stealth=%s): %s", site_url, stealth, exc)
            raise
        items_local = list(result.items)
        stats_local = result.stats.to_dict() if result.stats else {}
        errors_local = list(spider._errors)
        return items_local, stats_local, errors_local

    # First attempt: use the config's stealth setting
    try:
        items, stats_dict, crawl_errors = _run_spider(config.stealth)
    except Exception as exc:
        return {"site_url": site_url, "status": "error", "error": str(exc)}

    # If nothing was scraped and we appear to have a failed request, retry once with stealth.
    if (
        not items
        and not config.stealth
        and (stats_dict.get("failed_requests_count", 0) > 0 or stats_dict.get("requests_count", 0) == 0)
    ):
        logger.info(
            "[crawl] No pages scraped for %s on first attempt; retrying with stealth mode",
            site_url,
        )
        try:
            items, stats_dict, crawl_errors = _run_spider(True)
        except Exception as exc:
            return {
                "site_url": site_url,
                "status": "error",
                "error": f"stealth_retry_failed: {exc}",
            }
    finished_crawl_at = utcnow_iso()

    logger.info(
        "[crawl] Spider finished for %s: %d pages scraped",
        site_url,
        len(items),
    )

    index_entries: List[Dict[str, Any]] = []
    for item in items:
        page_url = item.get("url", "")
        filename = _page_filename(page_url)
        page_data = {
            "url": page_url,
            "title": item.get("title"),
            "meta_description": item.get("meta_description"),
            "text": item.get("text"),
            "html": item.get("html"),
            "assets": item.get("assets", {}),
        }
        write_json(pages_dir / filename, page_data)
        index_entries.append({
            "url": page_url,
            "title": item.get("title", ""),
            "filename": filename,
        })

    write_json(index_path, {
        "site_url": site_url,
        "crawl_started_at": started_at,
        "crawl_finished_at": finished_crawl_at,
        "total_pages": len(index_entries),
        "pages": index_entries,
    })

    all_images, all_pdfs, all_videos = _aggregate_assets(items)

    logger.info(
        "[assets] %s: %d images, %d PDFs, %d videos",
        site_slug,
        len(all_images),
        len(all_pdfs),
        len(all_videos),
    )

    image_manifest = download_assets(
        all_images,
        images_dir,
        max_workers=config.download_workers,
        timeout=config.timeout,
        retries=config.retries,
        backoff_seconds=config.backoff_seconds,
    )

    pdf_manifest = download_assets(
        all_pdfs,
        pdfs_dir,
        max_workers=config.download_workers,
        timeout=config.timeout,
        retries=config.retries,
        backoff_seconds=config.backoff_seconds,
    )

    if all_videos:
        write_json(data_dir / "video_urls.json", {
            "site_url": site_url,
            "total_videos": len(all_videos),
            "video_urls": all_videos,
        })

    write_json(site_dir / "crawl_stats.json", {
        "site_url": site_url,
        "crawl_started_at": started_at,
        "crawl_finished_at": finished_crawl_at,
        "asset_download_finished_at": utcnow_iso(),
        "spider_stats": stats_dict,
        "crawl_errors": crawl_errors,
        "asset_summary": {
            "images_found": len(all_images),
            "images_downloaded": sum(
                1 for m in image_manifest if m["status"] == "downloaded"
            ),
            "images_failed": sum(
                1 for m in image_manifest if m["status"] == "failed"
            ),
            "pdfs_found": len(all_pdfs),
            "pdfs_downloaded": sum(
                1 for m in pdf_manifest if m["status"] == "downloaded"
            ),
            "pdfs_failed": sum(
                1 for m in pdf_manifest if m["status"] == "failed"
            ),
            "video_urls_saved": len(all_videos),
        },
        "image_manifest": image_manifest,
        "pdf_manifest": pdf_manifest,
    })

    return {
        "site_url": site_url,
        "status": "completed",
        "pages_scraped": len(items),
        "images_downloaded": sum(
            1 for m in image_manifest if m["status"] == "downloaded"
        ),
        "pdfs_downloaded": sum(
            1 for m in pdf_manifest if m["status"] == "downloaded"
        ),
        "video_urls_saved": len(all_videos),
        "path": str(site_dir),
    }


def _process_site_wrapper(args: Tuple[str, str, PipelineConfig]) -> Dict[str, Any]:
    """Wrapper for ProcessPoolExecutor that unpacks arguments."""
    site_url, output_dir, config = args
    try:
        return process_site(site_url, output_dir, config)
    except Exception as exc:
        logger.error("[error] %s: %s", site_url, exc)
        return {"site_url": site_url, "status": "error", "error": str(exc)}


def run_pipeline(config: PipelineConfig) -> int:
    sites = read_sites(Path(config.sites_file))
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Starting Scrapling pipeline: %d sites, concurrency=%d",
        len(sites),
        config.concurrency,
    )

    results: List[Dict[str, Any]] = []

    if config.concurrency <= 1:
        for site in sites:
            result = process_site(site, config.output_dir, config)
            results.append(result)
            logger.info("[done] %s -> %s", site, result.get("status"))
    else:
        work_items = [(site, config.output_dir, config) for site in sites]
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=config.concurrency
        ) as executor:
            future_map = {
                executor.submit(_process_site_wrapper, item): item[0]
                for item in work_items
            }
            for future in concurrent.futures.as_completed(future_map):
                site = future_map[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info("[done] %s -> %s", site, result.get("status"))
                except Exception as exc:
                    logger.error("[error] %s: %s", site, exc)
                    results.append({
                        "site_url": site,
                        "status": "error",
                        "error": str(exc),
                    })

    summary = {
        "finished_at": utcnow_iso(),
        "total_sites": len(sites),
        "results": results,
    }
    summary_path = output_dir / "summary.json"
    write_json(summary_path, summary)
    logger.info("[summary] Wrote %s", summary_path)

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrapling-based full-site crawl pipeline"
    )
    parser.add_argument(
        "--sites-file",
        default="data/sites.txt",
        help="Path to .txt list of URLs (default: data/sites.txt)",
    )
    parser.add_argument(
        "--output-dir",
        default="output_scrapling",
        help="Output directory (default: output_scrapling)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=2,
        help="Number of sites to crawl in parallel (default: 2)",
    )
    parser.add_argument(
        "--concurrent-requests",
        type=int,
        default=8,
        help="Concurrent requests per spider (default: 8)",
    )
    parser.add_argument(
        "--download-delay",
        type=float,
        default=0.5,
        help="Delay between requests in seconds (default: 0.5)",
    )
    parser.add_argument(
        "--stealth",
        action="store_true",
        default=False,
        help="Use StealthySession with headless browser (slower but bypasses anti-bot)",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=4,
        help="Threads for asset downloads per site (default: 4)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout for asset downloads in seconds (default: 30)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retry attempts for failed asset downloads (default: 3)",
    )
    parser.add_argument(
        "--backoff-seconds",
        type=int,
        default=2,
        help="Backoff base seconds between retries (default: 2)",
    )
    parser.add_argument(
        "--crawldir",
        default=None,
        help="Directory for checkpoint files to enable pause/resume",
    )
    parser.add_argument(
        "--spider-timeout",
        type=int,
        default=60,
        help="HTTP timeout for spider fetcher sessions in seconds (default: 60)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force re-crawl even if a successful crawl index already exists",
    )
    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = build_parser()
    args = parser.parse_args()
    config = PipelineConfig(
        sites_file=args.sites_file,
        output_dir=args.output_dir,
        concurrency=max(1, args.concurrency),
        concurrent_requests=args.concurrent_requests,
        download_delay=args.download_delay,
        stealth=args.stealth,
        download_workers=args.download_workers,
        timeout=args.timeout,
        retries=args.retries,
        backoff_seconds=args.backoff_seconds,
        crawldir=args.crawldir,
        spider_timeout=args.spider_timeout,
        force=args.force,
    )
    return run_pipeline(config)


if __name__ == "__main__":
    raise SystemExit(main())
