# -*- coding: utf-8 -*-
import logging, time, random, re
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ========== Logging (console + file) ==========
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
fh = logging.FileHandler("hn_scraper.log", mode="a", encoding="utf-8"); fh.setFormatter(formatter)
ch = logging.StreamHandler(); ch.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(fh); logger.addHandler(ch)
else:
    logger.addHandler(fh); logger.addHandler(ch)

# ========== Constants ==========
BASE = "https://news.ycombinator.com/"
HEADERS = {"User-Agent":"Mozilla/5.0"}
TIMEOUT = 15
MAX_RETRIES = 3
RETRYABLE = {429, 500, 502, 503, 504}

# ========== SQLite ==========
DB_PATH = "hn_posts.db"

def db_create():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS hn_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id TEXT NOT NULL UNIQUE,
            title    TEXT,
            url      TEXT,
            points   INTEGER,
            author   TEXT,
            age_text TEXT,
            comments_link TEXT,
            scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()

def db_insert_many(rows):
    """rows: list[dict] load Keys: item_id, title, url, points, author, age_text, comments_link"""
    if not rows: 
        return
    data = [(r["item_id"], r["title"], r["url"], r["points"], r["author"], r["age_text"], r["comments_link"])
            for r in rows]
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.executemany("""
            INSERT OR IGNORE INTO hn_posts
            (item_id, title, url, points, author, age_text, comments_link)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()

def db_count():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM hn_posts")
        return cur.fetchone()[0]

# ========== HTTP ==========
def fetch_soup(url, max_retries=MAX_RETRIES, timeout=TIMEOUT):
    backoff = 1.0
    for attempt in range(1, max_retries+1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 404:
                logger.warning(f"404 Not Found: {url}")
                return None
            if r.status_code in RETRYABLE:
                raise requests.HTTPError(f"retryable {r.status_code}")
            r.raise_for_status()
            logger.info(f"OK {r.status_code} | {url}")
            return BeautifulSoup(r.text, "html.parser")
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            if attempt == max_retries:
                logger.error(f"Failed after {attempt} attempts | {url} | {e}")
                raise
            sleep_for = backoff + random.uniform(0, 0.5)
            logger.warning(f"Retry {attempt}/{max_retries} in {sleep_for:.1f}s | {url} | {e}")
            time.sleep(sleep_for)
            backoff *= 2

# ========== Parsing ==========
def parse_page(soup):
    rows = []
    for athing in soup.select("tr.athing"):
        item_id = athing.get("id", "").strip()
        if not item_id:
            continue

        a = athing.select_one("span.titleline a") or athing.select_one("a.storylink")
        if not a:
            continue
        title = a.get_text(strip=True)
        link = a.get("href", "")
        if link.startswith("item?id="):
            link = urljoin(BASE, link)

        sub = athing.find_next_sibling("tr")
        if not sub:
            continue

        # points
        points_el = sub.select_one("span.score")
        points = 0
        if points_el:
            txt = points_el.get_text(strip=True)  # e.g. "123 points"
            m = re.search(r"\d+", txt)
            points = int(m.group()) if m else 0

        # author
        author_el = sub.select_one("a.hnuser")
        author = author_el.get_text(strip=True) if author_el else ""

        # age text + comments link (includes item?id=ITEM)
        age_el = sub.select_one("span.age a")
        age_text = age_el.get_text(strip=True) if age_el else ""
        comments_link = urljoin(BASE, age_el.get("href")) if age_el else ""

        rows.append({
            "item_id": item_id,
            "title": title,
            "url": link,
            "points": points,
            "author": author,
            "age_text": age_text,
            "comments_link": comments_link
        })
    logger.info(f"Parsed {len(rows)} posts from page")
    return rows

# ========== Scraper (all pages until empty) ==========
def scrape_hn_all(max_pages=9999, sleep_between=0.7):
    all_rows = []
    page = 1
    while page <= max_pages:
        url = BASE if page == 1 else f"{BASE}news?p={page}"
        logger.info(f"Scraping page {page}: {url}")
        soup = fetch_soup(url)
        if soup is None:
            logger.info("Stop: got None soup (404 or fail).")
            break
        page_rows = parse_page(soup)
        if not page_rows:
            logger.info("Stop: empty page.")
            break
        db_insert_many(page_rows) 
        all_rows.extend(page_rows)
        page += 1
        time.sleep(sleep_between)     
    logger.info(f"TOTAL collected: {len(all_rows)} posts across {page-1} pages.")
    return all_rows


# ========== Main ==========
if __name__ == "__main__":
    try:
        db_create()
        rows = scrape_hn_all()  
        total = db_count()
        df = pd.DataFrame(rows)
        df.to_csv("hn_posts.csv", index=False, encoding="utf-8")
        df.to_excel("hn_posts.xlsx", index=False, engine="openpyxl")
        logger.info(f"Saved to SQLite. Total rows in DB: {total}")
        print(f"Scraped session: {len(rows)} | Total in DB: {total}")
        print("✅ Data Saved to DataBase and hn_posts.csv and hn_posts.xlsx")
    except Exception as e:
        logger.critical(f"Unexpected error: {e}")
        raise
