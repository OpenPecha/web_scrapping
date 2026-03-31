"""Cloudflare crawl pipeline.

Reads site URLs from a text file, submits crawl jobs, polls completion, downloads
assets, and writes per-site JSON outputs.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen


TERMINAL_STATUSES = {"completed", "errored", "cancelled", "failed"}
TRANSIENT_HTTP_CODES = {429, 500, 502, 503, 504}


@dataclass
class CrawlConfig:
    account_id: str
    api_token: str
    timeout: int
    poll_interval: int
    max_poll_minutes: int
    retries: int
    backoff_seconds: int
    formats: Sequence[str]
    depth: int
    limit: int
    render: bool
    concurrency: int


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


def is_terminal_status(status: Optional[str]) -> bool:
    if not status:
        return False
    return status.lower() in TERMINAL_STATUSES


def _api_request(
    method: str,
    url: str,
    token: str,
    timeout: int,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = Request(url=url, method=method, headers=headers, data=data)
    try:
        with urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} calling {url}: {text}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error calling {url}: {exc}") from exc
    except socket.timeout as exc:
        raise RuntimeError(f"Timeout calling {url}") from exc


def _api_request_with_retry(
    method: str,
    url: str,
    token: str,
    timeout: int,
    payload: Optional[Dict[str, Any]],
    retries: int,
    backoff_seconds: int,
) -> Dict[str, Any]:
    attempt = 0
    while True:
        attempt += 1
        try:
            return _api_request(method, url, token, timeout, payload=payload)
        except RuntimeError as exc:
            code_match = re.search(r"HTTP (\d+)", str(exc))
            code = int(code_match.group(1)) if code_match else None
            if attempt > retries or code not in TRANSIENT_HTTP_CODES:
                raise
            sleep_s = backoff_seconds * attempt
            print(
                f"[retry] {method} {url} attempt {attempt}/{retries} failed; "
                f"sleeping {sleep_s}s"
            )
            time.sleep(sleep_s)


def extract_result(data: Dict[str, Any]) -> Dict[str, Any]:
    result = data.get("result")
    if isinstance(result, dict):
        return result
    return data


def extract_job_id(response: Dict[str, Any]) -> str:
    result = extract_result(response)
    for key in ("id", "job_id", "jobId"):
        value = result.get(key) if isinstance(result, dict) else None
        if isinstance(value, str) and value.strip():
            return value
    raise ValueError(f"Could not extract job id from response: {response}")


def submit_crawl_job(config: CrawlConfig, site_url: str) -> str:
    endpoint = (
        f"https://api.cloudflare.com/client/v4/accounts/{config.account_id}"
        "/browser-rendering/crawl"
    )
    payload = {
        "url": site_url,
        "formats": list(config.formats),
        "depth": config.depth,
        "limit": config.limit,
        "render": config.render,
    }
    response = _api_request_with_retry(
        method="POST",
        url=endpoint,
        token=config.api_token,
        timeout=config.timeout,
        payload=payload,
        retries=config.retries,
        backoff_seconds=config.backoff_seconds,
    )
    return extract_job_id(response)


def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    url = record.get("url") or record.get("pageUrl") or ""
    return {
        "url": url,
        "status": record.get("status"),
        "html": record.get("html"),
        "markdown": record.get("markdown"),
        "json": record.get("json"),
        "metadata": record.get("metadata") or {},
    }


def merge_paginated_records(pages: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for page in pages:
        result = extract_result(page)
        records = result.get("records") or []
        for record in records:
            normalized = normalize_record(record)
            key = normalized["url"] or json.dumps(normalized, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            merged.append(normalized)
    return merged


def poll_crawl_job(
    config: CrawlConfig,
    job_id: str,
) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
    endpoint_base = (
        f"https://api.cloudflare.com/client/v4/accounts/{config.account_id}"
        f"/browser-rendering/crawl/{job_id}"
    )
    started = time.time()
    all_pages: List[Dict[str, Any]] = []
    last_result: Dict[str, Any] = {}
    while True:
        if (time.time() - started) > (config.max_poll_minutes * 60):
            raise TimeoutError(
                f"Timed out waiting for job {job_id} after {config.max_poll_minutes} minutes"
            )

        cursor: Optional[str] = None
        pages_for_cycle: List[Dict[str, Any]] = []
        while True:
            query = {"limit": 200}
            if cursor:
                query["cursor"] = cursor
            endpoint = f"{endpoint_base}?{urlencode(query)}"
            page = _api_request_with_retry(
                method="GET",
                url=endpoint,
                token=config.api_token,
                timeout=config.timeout,
                payload=None,
                retries=config.retries,
                backoff_seconds=config.backoff_seconds,
            )
            pages_for_cycle.append(page)
            result = extract_result(page)
            last_result = result
            cursor = result.get("cursor")
            if not cursor:
                break

        all_pages = pages_for_cycle
        status = str(last_result.get("status") or "").lower()
        print(f"[poll] job={job_id} status={status or 'unknown'}")
        if is_terminal_status(status):
            records = merge_paginated_records(all_pages)
            return status, records, last_result
        time.sleep(config.poll_interval)


def _extract_attr_values(html: str, attr_name: str) -> Iterable[str]:
    pattern = re.compile(rf"{attr_name}\s*=\s*([\"'])(.*?)\1", re.IGNORECASE)
    for match in pattern.finditer(html):
        value = unescape(match.group(2)).strip()
        if value:
            yield value


def _extract_srcset_values(html: str) -> Iterable[str]:
    for srcset in _extract_attr_values(html, "srcset"):
        for part in srcset.split(","):
            candidate = part.strip().split(" ")[0]
            if candidate:
                yield candidate


def extract_asset_urls(html: str, page_url: str) -> List[str]:
    if not html:
        return []
    candidates: Set[str] = set()
    for attr in ("src", "href", "poster"):
        for value in _extract_attr_values(html, attr):
            candidates.add(value)
    for value in _extract_srcset_values(html):
        candidates.add(value)

    normalized: List[str] = []
    seen: Set[str] = set()
    for value in candidates:
        if value.startswith(("javascript:", "data:", "#", "mailto:", "tel:")):
            continue
        absolute = urljoin(page_url, value)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        normalized.append(absolute)
    return normalized


def _extension_from_content_type(content_type: str) -> str:
    if not content_type:
        return ""
    low = content_type.lower()
    mapping = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/svg+xml": ".svg",
        "text/css": ".css",
        "application/javascript": ".js",
        "text/javascript": ".js",
    }
    for key, ext in mapping.items():
        if key in low:
            return ext
    return ""


def _safe_asset_filename(asset_url: str, content_type: str) -> str:
    parsed = urlparse(asset_url)
    original_name = Path(parsed.path).name
    ext = Path(original_name).suffix or _extension_from_content_type(content_type)
    digest = hashlib.sha1(asset_url.encode("utf-8")).hexdigest()[:16]
    return f"{digest}{ext}"


def download_asset(
    asset_url: str,
    assets_dir: Path,
    timeout: int,
    retries: int,
    backoff_seconds: int,
) -> Dict[str, Any]:
    attempt = 0
    while True:
        attempt += 1
        try:
            req = Request(
                url=asset_url,
                method="GET",
                headers={"User-Agent": "cloudflare-crawl-pipeline/1.0"},
            )
            with urlopen(req, timeout=timeout) as response:
                body = response.read()
                content_type = response.headers.get("Content-Type", "")
                filename = _safe_asset_filename(asset_url, content_type)
                local_path = assets_dir / filename
                local_path.write_bytes(body)
                return {
                    "original_url": asset_url,
                    "local_path": str(local_path),
                    "content_type": content_type,
                    "size_bytes": len(body),
                    "download_status": "downloaded",
                    "error": None,
                }
        except Exception as exc:  # broad by design for robust pipeline behavior
            if attempt > retries:
                return {
                    "original_url": asset_url,
                    "local_path": None,
                    "content_type": None,
                    "size_bytes": 0,
                    "download_status": "failed",
                    "error": str(exc),
                }
            time.sleep(backoff_seconds * attempt)


def build_output_payload(
    site_url: str,
    job_id: str,
    status: str,
    submitted_at: str,
    finished_at: str,
    records: Sequence[Dict[str, Any]],
    manifest: Sequence[Dict[str, Any]],
    raw_result: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "site_url": site_url,
        "job_id": job_id,
        "submitted_at": submitted_at,
        "finished_at": finished_at,
        "status": status,
        "total_pages": len(records),
        "pages": list(records),
        "assets": list(manifest),
        "raw_result": raw_result,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def process_site(
    site_url: str,
    output_dir: Path,
    config: CrawlConfig,
    lock: threading.Lock,
) -> Dict[str, Any]:
    site_slug = slugify_site(site_url)
    site_dir = output_dir / site_slug
    assets_dir = site_dir / "assets"
    result_json_path = site_dir / "crawl_result.json"
    checkpoint_path = site_dir / "checkpoint.json"

    if result_json_path.exists():
        print(f"[skip] Existing result found for {site_url}")
        return {"site_url": site_url, "status": "skipped", "path": str(result_json_path)}

    site_dir.mkdir(parents=True, exist_ok=True)
    assets_dir.mkdir(parents=True, exist_ok=True)
    submitted_at = utcnow_iso()

    with lock:
        print(f"[submit] {site_url}")
    job_id = submit_crawl_job(config, site_url)
    write_json(
        checkpoint_path,
        {
            "site_url": site_url,
            "job_id": job_id,
            "stage": "submitted",
            "updated_at": utcnow_iso(),
        },
    )

    status, records, raw_result = poll_crawl_job(config, job_id)
    write_json(
        checkpoint_path,
        {
            "site_url": site_url,
            "job_id": job_id,
            "stage": "polled",
            "status": status,
            "updated_at": utcnow_iso(),
        },
    )

    asset_urls: Set[str] = set()
    for record in records:
        page_url = record.get("url") or site_url
        html = record.get("html") or ""
        for asset_url in extract_asset_urls(html, page_url):
            asset_urls.add(asset_url)

    manifest: List[Dict[str, Any]] = []
    for asset_url in sorted(asset_urls):
        manifest.append(
            download_asset(
                asset_url=asset_url,
                assets_dir=assets_dir,
                timeout=config.timeout,
                retries=config.retries,
                backoff_seconds=config.backoff_seconds,
            )
        )

    payload = build_output_payload(
        site_url=site_url,
        job_id=job_id,
        status=status,
        submitted_at=submitted_at,
        finished_at=utcnow_iso(),
        records=records,
        manifest=manifest,
        raw_result=raw_result,
    )
    write_json(result_json_path, payload)
    write_json(
        checkpoint_path,
        {
            "site_url": site_url,
            "job_id": job_id,
            "stage": "done",
            "status": status,
            "result_path": str(result_json_path),
            "updated_at": utcnow_iso(),
        },
    )
    return {"site_url": site_url, "status": status, "path": str(result_json_path)}


def run_pipeline(args: argparse.Namespace) -> int:
    account_id = os.getenv("CF_ACCOUNT_ID", "").strip()
    api_token = os.getenv("CF_API_TOKEN", "").strip()
    if not account_id or not api_token:
        raise EnvironmentError(
            "Missing required env vars CF_ACCOUNT_ID and/or CF_API_TOKEN."
        )

    sites = read_sites(Path(args.sites_file))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    config = CrawlConfig(
        account_id=account_id,
        api_token=api_token,
        timeout=args.timeout,
        poll_interval=args.poll_interval,
        max_poll_minutes=args.max_poll_minutes,
        retries=args.retries,
        backoff_seconds=args.backoff_seconds,
        formats=args.formats,
        depth=args.depth,
        limit=args.limit,
        render=args.render,
        concurrency=max(1, args.concurrency),
    )

    lock = threading.Lock()
    results: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.concurrency) as executor:
        future_map = {
            executor.submit(process_site, site, output_dir, config, lock): site
            for site in sites
        }
        for future in concurrent.futures.as_completed(future_map):
            site = future_map[future]
            try:
                result = future.result()
                with lock:
                    print(f"[done] {site} -> {result['status']}")
                results.append(result)
            except Exception as exc:
                with lock:
                    print(f"[error] {site}: {exc}")
                results.append({"site_url": site, "status": "error", "error": str(exc)})

    summary_path = output_dir / "summary.json"
    write_json(summary_path, {"finished_at": utcnow_iso(), "results": results})
    print(f"[summary] Wrote {summary_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cloudflare crawl pipeline")
    parser.add_argument("--sites-file", default="data/sites.txt", help="Path to .txt list of URLs")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--formats", nargs="+", default=["html", "markdown"], help="Cloudflare formats")
    parser.add_argument("--depth", type=int, default=10, help="Crawl depth")
    parser.add_argument("--limit", type=int, default=1000, help="Crawl page limit")
    parser.add_argument(
        "--render",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable browser rendering",
    )
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds")
    parser.add_argument("--poll-interval", type=int, default=10, help="Poll interval seconds")
    parser.add_argument("--max-poll-minutes", type=int, default=90, help="Max poll duration in minutes")
    parser.add_argument("--retries", type=int, default=3, help="Retry attempts for transient failures")
    parser.add_argument("--backoff-seconds", type=int, default=2, help="Retry backoff base seconds")
    parser.add_argument("--concurrency", type=int, default=2, help="Concurrent site workers")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_pipeline(args)


if __name__ == "__main__":
    raise SystemExit(main())
