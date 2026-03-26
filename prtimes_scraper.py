# =========================
# PR TIMES 週次収集スクリプト（GitHub Actions対応版）
# =========================

import shutil
import os
import re
import time
import html
import unicodedata
from datetime import date, datetime, timedelta
from urllib.parse import quote_plus, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# =========================================
# 設定
# =========================================

KEYWORDS = [
    "脱毛器",
    "光美容器",
    "シェーバー",
]

MANUAL_RUN_DATE = None  # 自動実行時はNone

OUTPUT_DIR = "./prtimes_csv"
SLEEP_SECONDS = 1.0
MAX_SEARCH_PAGES = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# =========================================
# 日付
# =========================================

def get_run_date():
    if MANUAL_RUN_DATE:
        return datetime.strptime(MANUAL_RUN_DATE, "%Y-%m-%d").date()
    return date.today()

def get_target_period(run_date):
    days_since_prev_friday = (run_date.weekday() - 4) % 7
    target_end = run_date - timedelta(days=days_since_prev_friday)
    if target_end >= run_date:
        target_end -= timedelta(days=7)
    target_start = target_end - timedelta(days=6)
    return target_start, target_end

def get_delivery_date(run_date):
    return run_date + timedelta(days=2)

# =========================================
# 文字処理
# =========================================

def normalize_space(text):
    if text is None:
        return ""
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def parse_date(text):
    if not text:
        return None
    text = unicodedata.normalize("NFKC", text)
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
    if m:
        return date(int(m[1]), int(m[2]), int(m[3]))
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return date(int(m[1]), int(m[2]), int(m[3]))
    return None

# =========================================
# HTTP
# =========================================

session = requests.Session()
session.headers.update(HEADERS)

def fetch(url):
    r = session.get(url, timeout=30)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    time.sleep(SLEEP_SECONDS)
    return r.text

# =========================================
# URL取得
# =========================================

def build_search_url(keyword, page):
    return f"https://prtimes.jp/main/action.php?run=html&page=searchkey&search_word={quote_plus(keyword)}&search_page={page}"

def is_article(url):
    return bool(re.match(r"https://prtimes\.jp/main/html/rd/p/\d+\.\d+\.html", url))

def get_urls(keyword):
    urls = set()
    for page in range(1, MAX_SEARCH_PAGES + 1):
        url = build_search_url(keyword, page)
        print("取得:", url)

        soup = BeautifulSoup(fetch(url), "lxml")

        page_urls = {
            urljoin("https://prtimes.jp", a["href"])
            for a in soup.find_all("a", href=True)
            if is_article(urljoin("https://prtimes.jp", a["href"]))
        }

        print("件数:", len(page_urls))

        if not page_urls:
            break

        before = len(urls)
        urls |= page_urls

        if len(urls) == before:
            break

    return list(urls)

# =========================================
# 記事解析
# =========================================

def parse_article(url):
    soup = BeautifulSoup(fetch(url), "lxml")

    title = normalize_space(soup.title.text if soup.title else "")
    body = normalize_space(soup.get_text(" ", strip=True))

    date_text = soup.get_text(" ", strip=True)
    published = parse_date(date_text)

    company = ""

# ▼① 見出し直下から取得（最優先）
    meta_company = soup.select_one("a[href*='/company_id/']")
    if meta_company:
        company = normalize_space(meta_company.get_text())

# ▼② fallback（本文から抽出）
    if not company:
        m = re.search(r"([^\s]{1,30}株式会社)", body)
        if m:
            company = m.group(1)

# ▼③ 不要文字削除
    company = company.replace("のプレスリリース", "")  

    return {
        "url": url,
        "title": title,
        "body": body,
        "date": published,
        "company": company,
    }

# =========================================
# メイン処理
# =========================================

def run():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    run_date = get_run_date()
    start, end = get_target_period(run_date)
    delivery = get_delivery_date(run_date)

    csv_path = f"{OUTPUT_DIR}/{delivery.strftime('%Y-%m')}_prtimes_weekly.csv"

    print("対象期間:", start, end)

    rows = []

    for kw in KEYWORDS:
        urls = get_urls(kw)

        for u in urls:
            try:
                art = parse_article(u)

                if not art["date"]:
                    continue

                if not (start <= art["date"] <= end):
                    continue

                if kw not in (art["title"] + art["body"]):
                    continue

                rows.append([
                    delivery.strftime("%Y/%m/%d"),
                    kw,
                    u,
                    art["date"].strftime("%Y/%m/%d"),
                    art["company"],
                    art["title"],
                    art["body"],
                ])

            except Exception as e:
                print("エラー:", u, e)

    df_new = pd.DataFrame(rows, columns=[
        "配信予定日", "検索キーワード", "ページURL",
        "記事投稿日", "投稿者名", "記事タイトル", "記事全文"
    ])

    if os.path.exists(csv_path):
        df_old = pd.read_csv(csv_path, dtype=str)
    else:
        df_old = pd.DataFrame(columns=df_new.columns)

    df = pd.concat([df_old, df_new]).drop_duplicates(
        subset=["配信予定日", "検索キーワード", "ページURL"]
    )

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    
    DRIVE_DIR = "./drive_backup"
    

    os.makedirs(DRIVE_DIR, exist_ok=True)
    
    shutil.copy(csv_path, f"{DRIVE_DIR}/{os.path.basename(csv_path)}")
    print("保存完了:", csv_path)

# =========================================

if __name__ == "__main__":
    run()
