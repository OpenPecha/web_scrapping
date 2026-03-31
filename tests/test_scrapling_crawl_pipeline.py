"""Tests for the Scrapling-based crawl pipeline."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

# Load the module from scripts/ since it's not an installed package
_spec = importlib.util.spec_from_file_location(
    "scrapling_crawl_pipeline",
    Path(__file__).resolve().parent.parent / "scripts" / "scrapling_crawl_pipeline.py",
)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
sys.modules["scrapling_crawl_pipeline"] = _mod
_spec.loader.exec_module(_mod)

classify_asset = _mod.classify_asset
extract_asset_urls = _mod.extract_asset_urls
slugify_site = _mod.slugify_site
read_sites = _mod.read_sites
write_json = _mod.write_json
_get_url_extension = _mod._get_url_extension
_is_video_embed = _mod._is_video_embed
_safe_asset_filename = _mod._safe_asset_filename
_page_filename = _mod._page_filename
download_single_asset = _mod.download_single_asset
_aggregate_assets = _mod._aggregate_assets


class TestClassifyAsset:
    """Tests for the classify_asset function."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://example.com/photo.jpg", "image"),
            ("https://example.com/photo.jpeg", "image"),
            ("https://example.com/image.png", "image"),
            ("https://example.com/icon.gif", "image"),
            ("https://example.com/banner.webp", "image"),
            ("https://example.com/logo.svg", "image"),
            ("https://example.com/pic.bmp", "image"),
            ("https://example.com/fav.ico", "image"),
            ("https://example.com/photo.avif", "image"),
        ],
    )
    def test_classifies_images(self, url: str, expected: str):
        assert classify_asset(url) == expected

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://example.com/document.pdf", "pdf"),
            ("https://example.com/path/to/report.pdf", "pdf"),
            ("https://cdn.example.com/file.pdf", "pdf"),
        ],
    )
    def test_classifies_pdfs(self, url: str, expected: str):
        assert classify_asset(url) == expected

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://example.com/video.mp4", "video"),
            ("https://example.com/clip.webm", "video"),
            ("https://example.com/movie.avi", "video"),
            ("https://example.com/film.mov", "video"),
            ("https://example.com/vid.mkv", "video"),
            ("https://youtube.com/watch?v=abc123", "video"),
            ("https://www.youtube.com/embed/abc123", "video"),
            ("https://vimeo.com/12345", "video"),
            ("https://player.vimeo.com/video/12345", "video"),
        ],
    )
    def test_classifies_videos(self, url: str, expected: str):
        assert classify_asset(url) == expected

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://example.com/page.html", "other"),
            ("https://example.com/style.css", "other"),
            ("https://example.com/script.js", "other"),
            ("https://example.com/about", "other"),
        ],
    )
    def test_classifies_other(self, url: str, expected: str):
        assert classify_asset(url) == expected


class TestExtractAssetUrls:
    """Tests for extract_asset_urls function."""

    def test_empty_html_returns_empty(self):
        result = extract_asset_urls("", "https://example.com")
        assert result == {"images": [], "pdfs": [], "videos": []}

    def test_extracts_img_src(self):
        html = '<img src="/images/photo.jpg" alt="test">'
        result = extract_asset_urls(html, "https://example.com")
        assert "https://example.com/images/photo.jpg" in result["images"]

    def test_extracts_img_srcset(self):
        html = '<img srcset="/img/small.png 1x, /img/large.png 2x">'
        result = extract_asset_urls(html, "https://example.com")
        assert "https://example.com/img/small.png" in result["images"]
        assert "https://example.com/img/large.png" in result["images"]

    def test_extracts_pdf_links(self):
        html = '<a href="/docs/report.pdf">Download</a>'
        result = extract_asset_urls(html, "https://example.com")
        assert "https://example.com/docs/report.pdf" in result["pdfs"]

    def test_extracts_video_sources(self):
        html = '<video src="/media/clip.mp4"></video>'
        result = extract_asset_urls(html, "https://example.com")
        assert "https://example.com/media/clip.mp4" in result["videos"]

    def test_extracts_video_source_tags(self):
        html = """
        <video>
            <source src="/media/clip.webm" type="video/webm">
            <source src="/media/clip.mp4" type="video/mp4">
        </video>
        """
        result = extract_asset_urls(html, "https://example.com")
        assert "https://example.com/media/clip.webm" in result["videos"]
        assert "https://example.com/media/clip.mp4" in result["videos"]

    def test_extracts_video_poster_as_image(self):
        html = '<video poster="/images/poster.jpg" src="/media/vid.mp4"></video>'
        result = extract_asset_urls(html, "https://example.com")
        assert "https://example.com/images/poster.jpg" in result["images"]
        assert "https://example.com/media/vid.mp4" in result["videos"]

    def test_extracts_youtube_iframe(self):
        html = '<iframe src="https://www.youtube.com/embed/abc123"></iframe>'
        result = extract_asset_urls(html, "https://example.com")
        assert "https://www.youtube.com/embed/abc123" in result["videos"]

    def test_extracts_embed_pdf(self):
        html = '<embed src="/docs/manual.pdf" type="application/pdf">'
        result = extract_asset_urls(html, "https://example.com")
        assert "https://example.com/docs/manual.pdf" in result["pdfs"]

    def test_extracts_object_data_pdf(self):
        html = '<object data="/docs/guide.pdf" type="application/pdf"></object>'
        result = extract_asset_urls(html, "https://example.com")
        assert "https://example.com/docs/guide.pdf" in result["pdfs"]

    def test_skips_javascript_and_data_urls(self):
        html = """
        <img src="javascript:void(0)">
        <img src="data:image/png;base64,abc">
        <a href="mailto:test@example.com">Email</a>
        <a href="tel:+1234567890">Call</a>
        """
        result = extract_asset_urls(html, "https://example.com")
        assert result["images"] == []
        assert result["pdfs"] == []
        assert result["videos"] == []

    def test_resolves_relative_urls(self):
        html = '<img src="../images/photo.png">'
        result = extract_asset_urls(html, "https://example.com/pages/about.html")
        assert "https://example.com/images/photo.png" in result["images"]

    def test_deduplicates_urls(self):
        html = """
        <img src="/img/logo.png">
        <img src="/img/logo.png">
        <img src="https://example.com/img/logo.png">
        """
        result = extract_asset_urls(html, "https://example.com")
        assert len(result["images"]) == 1

    def test_extracts_picture_source(self):
        html = """
        <picture>
            <source srcset="/img/hero.webp" type="image/webp">
            <img src="/img/hero.jpg">
        </picture>
        """
        result = extract_asset_urls(html, "https://example.com")
        assert "https://example.com/img/hero.webp" in result["images"]
        assert "https://example.com/img/hero.jpg" in result["images"]

    def test_mixed_assets(self):
        html = """
        <img src="/photo.jpg">
        <a href="/doc.pdf">PDF</a>
        <video src="/clip.mp4"></video>
        <iframe src="https://youtube.com/embed/xyz"></iframe>
        """
        result = extract_asset_urls(html, "https://example.com")
        assert len(result["images"]) == 1
        assert len(result["pdfs"]) == 1
        assert len(result["videos"]) == 2


class TestSlugifySite:
    """Tests for the slugify_site function."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://example.com/", "example.com"),
            ("https://example.com", "example.com"),
            ("https://www.example.com/path/to/page", "www.example.com_path_to_page"),
            (
                "https://www.shanpafoundation-resourcecenter.net/index.php?title=Home",
                "www.shanpafoundation-resourcecenter.net_index.php",
            ),
            ("http://xz.qiongbuwang.com/view/302.html", "xz.qiongbuwang.com_view_302.html"),
        ],
    )
    def test_slugify(self, url: str, expected: str):
        assert slugify_site(url) == expected


class TestReadSites:
    """Tests for the read_sites function."""

    def test_reads_valid_file(self, tmp_path: Path):
        sites_file = tmp_path / "sites.txt"
        sites_file.write_text("https://example.com\nhttps://test.org\n")
        result = read_sites(sites_file)
        assert result == ["https://example.com", "https://test.org"]

    def test_skips_comments_and_blanks(self, tmp_path: Path):
        sites_file = tmp_path / "sites.txt"
        sites_file.write_text("# comment\nhttps://example.com\n\n# another\nhttps://test.org\n")
        result = read_sites(sites_file)
        assert result == ["https://example.com", "https://test.org"]

    def test_raises_on_missing_file(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            read_sites(tmp_path / "nonexistent.txt")

    def test_raises_on_empty_file(self, tmp_path: Path):
        sites_file = tmp_path / "sites.txt"
        sites_file.write_text("# only comments\n\n")
        with pytest.raises(ValueError):
            read_sites(sites_file)


class TestHelpers:
    """Tests for various helper functions."""

    def test_get_url_extension(self):
        assert _get_url_extension("https://example.com/file.pdf") == ".pdf"
        assert _get_url_extension("https://example.com/path/image.jpg") == ".jpg"
        assert _get_url_extension("https://example.com/page") == ""
        assert _get_url_extension("https://example.com/") == ""

    def test_is_video_embed(self):
        assert _is_video_embed("https://www.youtube.com/embed/abc") is True
        assert _is_video_embed("https://vimeo.com/12345") is True
        assert _is_video_embed("https://player.vimeo.com/video/12345") is True
        assert _is_video_embed("https://example.com/page") is False

    def test_safe_asset_filename(self):
        name = _safe_asset_filename("https://example.com/photo.jpg")
        assert name.endswith(".jpg")
        assert len(name) > 4

    def test_safe_asset_filename_uses_content_type(self):
        name = _safe_asset_filename(
            "https://example.com/image", content_type="image/png"
        )
        assert name.endswith(".png")

    def test_page_filename_ends_with_json(self):
        name = _page_filename("https://example.com/about")
        assert name.endswith(".json")

    def test_page_filename_includes_path_slug(self):
        name = _page_filename("https://example.com/docs/guide")
        assert "docs_guide" in name

    def test_page_filename_stable_for_same_url(self):
        a = _page_filename("https://example.com/page")
        b = _page_filename("https://example.com/page")
        assert a == b

    def test_page_filename_different_for_different_urls(self):
        a = _page_filename("https://example.com/page1")
        b = _page_filename("https://example.com/page2")
        assert a != b

    def test_page_filename_handles_root_url(self):
        name = _page_filename("https://example.com/")
        assert name.endswith(".json")
        assert len(name) > 5

    def test_write_json(self, tmp_path: Path):
        out = tmp_path / "sub" / "test.json"
        write_json(out, {"key": "value"})
        assert out.exists()
        data = json.loads(out.read_text())
        assert data == {"key": "value"}


class TestAggregateAssets:
    """Tests for the _aggregate_assets function."""

    def test_aggregates_and_deduplicates(self):
        items = [
            {
                "url": "https://example.com/p1",
                "assets": {
                    "images": ["https://example.com/a.jpg", "https://example.com/b.png"],
                    "pdfs": ["https://example.com/doc.pdf"],
                    "videos": ["https://example.com/vid.mp4"],
                },
            },
            {
                "url": "https://example.com/p2",
                "assets": {
                    "images": ["https://example.com/a.jpg", "https://example.com/c.gif"],
                    "pdfs": ["https://example.com/doc.pdf"],
                    "videos": [],
                },
            },
        ]
        images, pdfs, videos = _aggregate_assets(items)
        assert len(images) == 3
        assert len(pdfs) == 1
        assert len(videos) == 1
        assert "https://example.com/a.jpg" in images

    def test_handles_missing_assets_key(self):
        items = [{"url": "https://example.com/p1"}]
        images, pdfs, videos = _aggregate_assets(items)
        assert images == []
        assert pdfs == []
        assert videos == []
