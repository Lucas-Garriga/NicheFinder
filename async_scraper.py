# -------------------- Amazon Async Scraper --------------------

import asyncio
import aiohttp
import pandas as pd
import duckdb
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
import random

# -------------------- Config --------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept-Encoding": "gzip, deflate, br"
}
MAX_CONCURRENT_REQUESTS = 10
TIMEOUT = 10
DELAY_RANGE = (1.0, 2.5)
PARQUET_INPUT = "notebooks/df_extract.parquet"
PARQUET_OUTPUT = "produits_amazon_scrapes.parquet"

# -------------------- DuckDB SQL --------------------
def sql(query):
    con = duckdb.connect()
    df = con.execute(query).df()
    con.close()
    return df

df_full = sql(f"SELECT * FROM read_parquet('{PARQUET_INPUT}')")
urls = df_full['url'].dropna().unique().tolist()

# -------------------- Async Scraping --------------------
async def fetch(session, url, sem):
    async with sem:
        await asyncio.sleep(random.uniform(*DELAY_RANGE))
        try:
            async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as response:
                if response.status != 200:
                    return None
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                # Catégorie
                card = soup.find('ul', class_='a-unordered-list a-horizontal a-size-small')
                cat = card.get_text(" > ", strip=True) if card else None

                # Titre
                title_tag = soup.find('span', class_="a-size-large product-title-word-break")
                title = title_tag.get_text(strip=True) if title_tag else None

                # Classement
                top_list = []
                top_card = soup.find('ul', class_="a-unordered-list a-nostyle a-vertical")
                if top_card:
                    top_soup = top_card.find_all('span', class_="a-list-item")
                    top_list = [t.get_text(" ", strip=True) for t in top_soup]

                # Date de mise en ligne
                date_pub = None
                table_soup = soup.find('table', id='productDetails_detailBullets_sections1')
                if table_soup:
                    rows = table_soup.find_all('tr')
                    for row in rows:
                        th = row.find('th')
                        td = row.find('td')
                        if th and "date de mise en ligne" in th.get_text(strip=True).lower():
                            date_pub = td.get_text(strip=True) if td else None
                            break

                return {
                    "url": url,
                    "titre": title,
                    "categorie": cat,
                    "classement": top_list,
                    "date_mise_en_ligne": date_pub
                }

        except Exception:
            return None

# -------------------- Main Runner --------------------
async def main():
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url, sem) for url in urls]
        results = []
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping Amazon"):
            res = await task
            if res:
                results.append(res)
        return results

if __name__ == "__main__":
    final_results = asyncio.run(main())
    df = pd.DataFrame(final_results)
    df.to_parquet(PARQUET_OUTPUT, index=False)
    print(f"\n✅ Export terminé : {PARQUET_OUTPUT} ({len(df)} lignes)")