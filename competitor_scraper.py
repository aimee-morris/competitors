"""Competitor events scraper including Southbank Centre.

NOTE: This is a simplified illustrative scraper. You MUST verify that scraping each
site complies with its robots.txt and Terms of Use. Where disallowed, replace with
manual data entry or an approved API/feed.

Run:
    python ingestion/competitor_scraper.py
"""

import os
import re
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup

# Optional: environment variable to slow scraping further in CI
REQUEST_DELAY = float(os.getenv("COMPETITOR_SCRAPE_DELAY", "1.5"))

HEADERS = {
    "User-Agent": "ProducerAIBot/0.1 (+contact: your-email@example.com)"
}

competitors: Dict[str, str] = {
    'Live Nation': 'https://www.livenation.co.uk/events',
    'AEG Presents': 'https://www.aegpresents.co.uk/events',
    'Intelligence Squared': 'https://www.intelligencesquared.com/events/',
    'Goalhanger': 'https://goalhangerpodcasts.com/live/',
    'No Third Entertainment': 'https://nothirdentertainment.com/events',  # placeholder, validate
    'Phil McIntyre Entertainments': 'https://www.philmcintyreentertainments.com/tour-dates',
    'Southbank Centre': 'https://www.southbankcentre.co.uk/whats-on',
}

def guess_speaker(title: str) -> Optional[str]:
    """Heuristic to extract a probable speaker name from event title."""
    patterns = [
        r'An Evening with (.+)',
        r'In conversation with (.+)',
        r'In conversation (?:with )?(.+)',
        r'(.+): A Conversation',
        r'Conversation with (.+)',
    ]
    for p in patterns:
        m = re.search(p, title, re.IGNORECASE)
        if m:
            candidate = m.group(1).strip().rstrip('.')
            candidate = re.sub(r' – .*', '', candidate)
            return candidate
    return None

def fetch(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"[WARN] Failed to fetch {url}: {e}")
        return None

def parse_generic_list(soup: BeautifulSoup, competitor_name: str) -> List[Dict[str, Any]]:
    """Fallback generic parser (very naive). Replace with site-specific logic as needed."""
    events = []
    if not soup:
        return events
    links = soup.select("a")
    seen = set()
    for a in links:
        text = a.get_text(strip=True)
        if not text or len(text.split()) < 3:
            continue
        href = a.get("href") or ""
        # crude filter
        if competitor_name in ("Live Nation", "AEG Presents"):
            if "/event/" not in href and "/events/" not in href:
                continue
        key = (text, href)
        if key in seen:
            continue
        seen.add(key)
        events.append({
            "competitor_name": competitor_name,
            "title": text,
            "speaker_name": guess_speaker(text),
            "date_raw": None,
            "url": href if href.startswith("http") else None,
        })
    return events

def parse_southbank_events(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    if not soup:
        return events
    cards = soup.select('div.card--event')
    for c in cards:
        category_el = c.select_one('.card__meta-category')
        category = category_el.get_text(strip=True).lower() if category_el else ''
        if not any(kw in category for kw in ['talk', 'literature', 'ideas', 'spoken']):
            continue
        title_el = c.select_one('.card__title a')
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        url = title_el.get('href') or ''
        date_el = c.select_one('.card__date')
        date_text = date_el.get_text(strip=True) if date_el else ''
        # TODO: implement robust date parsing (multiple formats)
        full_url = url if url.startswith('http') else f"https://www.southbankcentre.co.uk{url}"
        events.append({
            "competitor_name": "Southbank Centre",
            "title": title,
            "speaker_name": guess_speaker(title),
            "date_raw": date_text,
            "url": full_url
        })
    return events

def normalize_and_print(events: List[Dict[str, Any]]):
    print(f"Collected {len(events)} raw events.")
    # Just print summary; DB insertion handled elsewhere
    by_comp = {}
    for ev in events:
        by_comp.setdefault(ev['competitor_name'], 0)
        by_comp[ev['competitor_name']] += 1
    for comp, count in sorted(by_comp.items(), key=lambda x: -x[1]):
        print(f"  {comp}: {count}")

def scrape_all() -> List[Dict[str, Any]]:
    all_events: List[Dict[str, Any]] = []
    for name, url in competitors.items():
        print(f"[INFO] Fetching {name}: {url}")
        soup = fetch(url)
        if name == "Southbank Centre":
            events = parse_southbank_events(soup)
        else:
            events = parse_generic_list(soup, name)
        all_events.extend(events)
        time.sleep(REQUEST_DELAY)
    normalize_and_print(all_events)
    return all_events

if __name__ == "__main__":
    scrape_all()
