import os
import re
import requests
from typing import Optional, List, Tuple
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup, Tag
'''
This scraper can be used to retrieve all diamond league mens 200m race analysis PDFs
'''

# ---- Configure the year pages you want to crawl ----
YEAR_PAGES = [
    "https://www.omegatiming.com/sports-timing-live-results/2024",
    "https://www.omegatiming.com/sports-timing-live-results/2025",
    "https://www.omegatiming.com/sports-timing-live-results/2023",
    "https://www.omegatiming.com/sports-timing-live-results/2022",
    "https://www.omegatiming.com/sports-timing-live-results/2021",
     "https://www.omegatiming.com/sports-timing-live-results/2020",
    # add more year pages if desired
]

OUT_DIR = "PDF_Downloads"  # downloads go here
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; omega-scraper/1.0)"}

# On the YEAR pages: select meet anchors from rows marked as Athletics
CSS_MEET = "div.row:has(> p.sport.athletics) > h3.detail > a[href]"

# ---------- Helpers ----------

def get_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        print(f"[!] Request failed for {url}: {e}")
        return None

def make_absolute(href: str, base_url: str) -> str:
    """
    Make href absolute, with the special rule on YEAR pages:
    - '/Zurich' under a YEAR page -> append to *current* year path
      (https://.../sports-timing-live-results/YEAR/Zurich)
    Otherwise use standard resolution.
    """
    if href.startswith(("http://", "https://")):
        return href
    scheme, netloc, path, *_ = urlparse(base_url)

    if href.startswith("//"):
        return f"{scheme}:{href}"

    # '/YYYY/...': treat as site-root absolute
    if href.startswith("/") and len(href) >= 6 and href[1:5].isdigit() and href[5] == "/":
        return f"{scheme}://{netloc}{href}"

    # On YEAR pages, '/Zurich' -> append to the YEAR path
    if href.startswith("/") and "/sports-timing-live-results/" in path:
        return base_url.rstrip("/") + href

    # Normal resolution (also handles root-relative like '/File/...')
    return urljoin(base_url.rstrip("/") + "/", href)

def p_is_sport_athletics(p: Tag) -> bool:
    classes = p.get("class", [])
    return "sport" in classes and "athletics" in classes

def normalize_text(s: str) -> str:
    return " ".join(s.split()).strip().lower()

def is_200m_men(text: str) -> bool:
    t = normalize_text(text).replace("200 m", "200m")
    return t == "200m men"

def is_race_analysis(text: str) -> bool:
    # match 'Race analysis' case-insensitively (tolerant of multiple spaces/suffixes)
    return re.search(r"Race analysis$", text, flags=re.IGNORECASE) is not None

def content_disposition_filename(headers: dict) -> Optional[str]:
    cd = headers.get("content-disposition") or headers.get("Content-Disposition")
    if not cd:
        return None
    # naive parse for filename="..."
    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
    if m:
        return os.path.basename(m.group(1))
    return None

def safe_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    name = os.path.basename(path) or "race_analysis.pdf"
    # If no extension, assume PDF
    if "." not in name:
        name += ".pdf"
    # Strip any problematic characters
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    return name

def unique_path(directory: str, filename: str) -> str:
    base, ext = os.path.splitext(filename)
    candidate = os.path.join(directory, filename)
    i = 1
    while os.path.exists(candidate):
        candidate = os.path.join(directory, f"{base}_{i}{ext}")
        i += 1
    return candidate

def download_pdf(url: str, out_dir: str) -> Optional[str]:
    try:
        with requests.get(url, headers=HEADERS, timeout=40, stream=True) as r:
            r.raise_for_status()
            # Prefer server-provided filename if any
            fname = content_disposition_filename(r.headers) or safe_filename_from_url(url)
            os.makedirs(out_dir, exist_ok=True)
            out_path = unique_path(out_dir, fname)
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
        return out_path
    except requests.RequestException as e:
        print(f"[!] Download failed for {url}: {e}")
        return None

# ---------- Page parsing ----------

def find_meet_links_on_year_page(year_url: str) -> List[str]:
    """From a YEAR page, get meet links (e.g., 'Wanda Diamond League') anchored under Athletics rows."""
    html = get_html(year_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    # Try CSS with :has(); fallback if unsupported
    try:
        anchors = soup.select(CSS_MEET)
    except Exception:
        anchors = []
        for div in soup.find_all("div", class_="row"):
            if not any(p_is_sport_athletics(p) for p in div.find_all("p", recursive=False)):
                continue
            for h3 in div.find_all("h3", class_="detail", recursive=False):
                for a in h3.find_all("a", href=True, recursive=False):
                    anchors.append(a)

    # Deduplicate while preserving order
    seen = set()
    result = []
    for a in anchors:
        abs_href = make_absolute(a["href"], year_url)
        if abs_href not in seen:
            seen.add(abs_href)
            result.append(abs_href)
    return result

def find_200m_race_analysis_links_on_meet_page(meet_url: str) -> List[Tuple[str, str]]:
    """
    On a meet page, find rows where a DIRECT-CHILD <p> has text '200m Men'.
    Return only sibling anchors whose text matches 'Race analysis'.
    """
    html = get_html(meet_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")

    results: List[Tuple[str, str]] = []

    for div in soup.find_all("div", class_="row"):
        # Direct-child <p> blocks for '200m Men'
        direct_ps = [p for p in div.find_all("p", recursive=False)]
        if not any(is_200m_men(p.get_text(" ", strip=True)) for p in direct_ps):
            continue

        # Candidate anchors: direct-child <a>, or anchors nested in other direct children (e.g., <h3>)
        sibling_as = [a for a in div.find_all("a", href=True, recursive=False)]
        if not sibling_as:
            for child in div.find_all(recursive=False):
                if child.name == "p":
                    continue
                sibling_as.extend(child.find_all("a", href=True))

        # Keep only those whose visible text is 'Race analysis'
        for a in sibling_as:
            text = " ".join(a.get_text(" ", strip=True).split())
            if not is_race_analysis(text):
                continue
            href = make_absolute(a["href"], meet_url)
            results.append((text, href))

    # Deduplicate by (text, href)
    dedup, seen = [], set()
    for t, h in results:
        key = (t, h)
        if key not in seen:
            seen.add(key)
            dedup.append(key)
    return dedup

# ---------- Main ----------

def main():
    os.makedirs(OUT_DIR, exist_ok=True)  # safe even if already exists
    for year_url in YEAR_PAGES:
        meet_links = find_meet_links_on_year_page(year_url)
        print(f"\n=== YEAR PAGE: {year_url} ===")
        print(f"Found {len(meet_links)} meet links")
        for meet in meet_links:
            ra_links = find_200m_race_analysis_links_on_meet_page(meet)
            if not ra_links:
                continue
            print(f"\n-- Meet: {meet}")
            for i, (text, href) in enumerate(ra_links, start=1):
                print(f"{i:02d}. {text} -> {href}")
                out_path = download_pdf(href, OUT_DIR)
                if out_path:
                    size_kb = os.path.getsize(out_path) / 1024.0
                    print(f"    saved: {out_path} ({size_kb:.1f} KB)")
                else:
                    print("    [!] download failed")

if __name__ == "__main__":
    main()
