import importlib.util
import sys
from pathlib import Path


def _load_pipeline_module():
    root = Path(__file__).resolve().parent.parent
    module_path = root / "scripts" / "cloudflare_crawl_pipeline.py"
    spec = importlib.util.spec_from_file_location("cloudflare_crawl_pipeline", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_is_terminal_status():
    mod = _load_pipeline_module()
    assert mod.is_terminal_status("completed")
    assert mod.is_terminal_status("ERRORED")
    assert mod.is_terminal_status("cancelled")
    assert not mod.is_terminal_status("running")
    assert not mod.is_terminal_status(None)


def test_merge_paginated_records_dedupes_by_url():
    mod = _load_pipeline_module()
    page_one = {
        "result": {
            "records": [
                {"url": "https://a.com", "status": "completed", "html": "<html/>"},
                {"url": "https://b.com", "status": "completed", "html": "<html/>"},
            ]
        }
    }
    page_two = {
        "result": {
            "records": [
                {"url": "https://b.com", "status": "completed", "html": "<html/>"},
                {"url": "https://c.com", "status": "completed", "html": "<html/>"},
            ]
        }
    }
    records = mod.merge_paginated_records([page_one, page_two])
    assert [r["url"] for r in records] == [
        "https://a.com",
        "https://b.com",
        "https://c.com",
    ]


def test_extract_asset_urls_normalizes_and_dedupes():
    mod = _load_pipeline_module()
    html = """
    <html>
      <img src="/images/logo.png" />
      <script src="https://cdn.example.com/app.js"></script>
      <link href="/static/site.css" rel="stylesheet" />
      <source srcset="/img/a.webp 1x, /img/a.webp 2x" />
      <a href="#top">skip</a>
    </html>
    """
    urls = mod.extract_asset_urls(html, "https://example.com/docs/page")
    assert "https://example.com/images/logo.png" in urls
    assert "https://cdn.example.com/app.js" in urls
    assert "https://example.com/static/site.css" in urls
    assert "https://example.com/img/a.webp" in urls
    assert len(urls) == len(set(urls))


def test_output_payload_shape():
    mod = _load_pipeline_module()
    payload = mod.build_output_payload(
        site_url="https://example.com",
        job_id="job-123",
        status="completed",
        submitted_at="2026-03-23T00:00:00+00:00",
        finished_at="2026-03-23T00:01:00+00:00",
        records=[{"url": "https://example.com", "status": "completed"}],
        manifest=[
            {
                "original_url": "https://example.com/image.png",
                "local_path": "output/example/assets/abc.png",
                "content_type": "image/png",
                "size_bytes": 10,
                "download_status": "downloaded",
                "error": None,
            }
        ],
        raw_result={"status": "completed"},
    )
    assert payload["site_url"] == "https://example.com"
    assert payload["job_id"] == "job-123"
    assert payload["status"] == "completed"
    assert payload["total_pages"] == 1
    assert isinstance(payload["pages"], list)
    assert isinstance(payload["assets"], list)
    assert "raw_result" in payload
