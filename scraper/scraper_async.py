import asyncio
import aiohttp
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
import random
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}
MAX_CONCURRENT_REQUESTS = 10
TIMEOUT = 10
DELAY_RANGE = (1.0, 2.5)


async def fetch(session, url, sem):
    async with sem:
        await asyncio.sleep(random.uniform(*DELAY_RANGE))
        try:
            async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as response:
                if response.status != 200:
                    return None
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                card = soup.find(
                    "ul", class_="a-unordered-list a-horizontal a-size-small"
                )
                cat = card.get_text(" > ", strip=True) if card else None

                title_tag = soup.find(
                    "span", class_="a-size-large product-title-word-break"
                )
                title = title_tag.get_text(strip=True) if title_tag else None

                top_list = []
                top_card = soup.find(
                    "ul", class_="a-unordered-list a-nostyle a-vertical"
                )
                if top_card:
                    top_soup = top_card.find_all("span", class_="a-list-item")
                    top_list = [t.get_text(" ", strip=True) for t in top_soup]

                date_pub = None
                table_soup = soup.find(
                    "table", id="productDetails_detailBullets_sections1"
                )
                if table_soup:
                    rows = table_soup.find_all("tr")
                    for row in rows:
                        th = row.find("th")
                        td = row.find("td")
                        if (
                            th
                            and "date de mise en ligne"
                            in th.get_text(strip=True).lower()
                        ):
                            date_pub = td.get_text(strip=True) if td else None
                            break

                return {
                    "url": url,
                    "category": cat,
                    "classement": top_list,
                    "published_date": date_pub,
                    "full_title": title,
                }

        except Exception:
            return None


async def scrape_async_amazon(urls):
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url, sem) for url in urls]
        results = []
        for task in tqdm(
            asyncio.as_completed(tasks), total=len(tasks), desc="Scraping Amazon Async"
        ):
            res = await task
            if res:
                results.append(res)
        return results


def run_scrape_async(urls):
    results = asyncio.run(scrape_async_amazon(urls))
    df = pd.DataFrame(results)

    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data" / "scraped"
    data_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d")
    save_path = data_dir / f"amazon_details_{timestamp}.csv"
    df.to_csv(save_path, index=False)
    print(f"✅ Détails produits sauvegardés : {save_path}")
    return save_path
