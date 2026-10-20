"""
Content pages (app/pages.py): every slug renders, nothing parked in an HTML
comment or left as a placeholder reaches a page, and the head tags, nav,
robots.txt and sitemap.xml agree with content/seo-meta.md.
"""
import re
import xml.etree.ElementTree as ET

import pytest
from fastapi.testclient import TestClient

import app.main as main
import app.pages as pages


@pytest.fixture(scope="module")
def client():
    with TestClient(main.app) as c:
        yield c


CONTENT_SLUGS = [s for s in pages.slugs() if s != "/"]


@pytest.mark.parametrize("slug", CONTENT_SLUGS)
def test_page_renders_with_meta_and_chrome(client, slug):
    r = client.get(slug)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    page = r.text
    meta = pages.seo_meta()[slug]
    assert f"<title>{pages.esc(meta['title'])}</title>" in page
    assert f'<meta name="description" content="{pages.esc(meta["description"])}">' in page
    assert f'<link rel="canonical" href="https://permitodds.com{slug}">' in page
    assert 'class="site-header"' in page and 'class="site-footer"' in page
    assert '<script src="/static/analytics.js" defer></script>' in page


@pytest.mark.parametrize("slug", CONTENT_SLUGS)
def test_no_placeholders_or_comments_published(client, slug):
    page = client.get(slug).text
    assert not pages.PLACEHOLDER.search(page), pages.PLACEHOLDER.search(page).group(0)
    assert "<!--" not in page
    assert "HELD UNTIL" not in page


def test_parked_ads_text_stays_out():
    # The ads paragraphs are in the source, inside comments, and must not render.
    assert "AdSense" in (pages.CONTENT_DIR / "privacy.md").read_text(encoding="utf-8")
    assert "AdSense" not in pages.render_guide("/privacy")
    assert "affiliate links" not in pages.render_guide("/terms")


def test_internal_links_resolve(client):
    known = set(pages.slugs())
    for slug in CONTENT_SLUGS:
        for href in re.findall(r'href="(/[^"#?]*)', client.get(slug).text):
            if href.startswith("/static/"):
                continue
            assert href in known, f"{slug} links to {href}"


def test_zone_page_has_calendar_and_month_table(client):
    page = client.get("/zones/snow").text
    # 170 season days, each a coloured tile with its odds.
    assert len(re.findall(r'class="hm-day hm-(?:green|orange|red|nodata)"', page)) == 170
    assert "<th>Oct</th>" in page and "<th>2022</th>" in page
    assert 'href="/?zone=Snow"' in page
    assert "Group size:" not in page          # only the Core has a size picker


@pytest.mark.parametrize("slug", ["/zones/colchuck", "/zones/snow", "/zones/eightmile", "/zones/stuart"])
def test_non_core_zone_pages_name_no_group_size(client, slug):
    # Group size does not change the odds outside the Core, so these pages
    # must not present their figures as being for a particular party size.
    page = client.get(slug).text.lower()
    assert not re.search(r"party of|two-person|solo|for a pair", page)
    assert "group size does not affect the odds in this zone" in page


def test_core_group_size(client):
    default = client.get("/zones/core").text
    solo = client.get("/zones/core?size=1").text
    assert "party of 2" in default and "solo" in solo
    assert 'href="/?zone=Core&amp;size=1"' in solo
    # Canonical ignores the size parameter.
    assert '<link rel="canonical" href="https://permitodds.com/zones/core">' in solo
    assert client.get("/zones/core?size=9").status_code == 422


def test_head_requests(client):
    for slug in ["/", "/faq", "/zones/stuart", "/robots.txt", "/sitemap.xml"]:
        assert client.head(slug).status_code == 200, slug


def test_robots_and_sitemap(client):
    robots = client.get("/robots.txt").text
    assert "Sitemap: https://permitodds.com/sitemap.xml" in robots
    r = client.get("/sitemap.xml")
    assert r.headers["content-type"].startswith("application/xml")
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs = [e.text for e in ET.fromstring(r.text).findall("s:url/s:loc", ns)]
    assert locs == ["https://permitodds.com" + s for s in pages.slugs()]
    assert len(locs) == 13
    assert set(pages.slugs()) == set(pages.seo_meta())


def test_static_files(client):
    assert client.get("/static/site.css").status_code == 200
    assert client.get("/static/analytics.js").status_code == 200
    fav = client.get("/favicon.ico")
    assert fav.status_code == 200 and fav.headers["content-type"].startswith("image/svg+xml")


def test_calculator_links_shared_assets(client):
    page = client.get("/").text
    assert '<script src="/static/analytics.js"></script>' in page
    assert '<link rel="stylesheet" href="/static/site.css" />' in page
    assert "<title>Enchantments Lottery Odds Calculator</title>" in page
    # The identity code moved to analytics.js; the page must not define it twice.
    assert "function analyticsIds" not in page
