from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import re
from pathlib import Path


def scrape_amazon(max_pages=5) -> Path:
    # 📁 Créer dossier de sauvegarde des données scrapées (avec date)
    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data" / "scraped"
    data_dir.mkdir(parents=True, exist_ok=True)

    # 🔧 Configuration de Selenium pour tourner sans ouvrir Chrome
    options = Options()
    options.add_argument("--headless")  # Pas de fenêtre visible
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)

    base_url = "https://www.amazon.fr/s?i=electronics&srs=4551203031&rh=n%3A4551203031&s=popularity-rank&fs=true&page={}"
    products = []
    page = 1

    while page <= max_pages:
        print(f"🔎 Scraping page {page}")
        driver.get(base_url.format(page))
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        items = soup.find_all("div", {"data-component-type": "s-search-result"})

        for rank, item in enumerate(items, start=1 + (page - 1) * len(items)):
            title_elem = item.h2
            title = title_elem.text.strip() if title_elem else None

            # 🔢 Récupération des différents KPIs
            price = None
            price_whole = item.select_one("span.a-price > span.a-offscreen")
            if price_whole:
                try:
                    price = float(
                        price_whole.text.strip().replace("€", "").replace(",", ".")
                    )
                except:
                    price = None

            rating = None
            rating_tag = item.select_one("span.a-icon-alt")
            if rating_tag:
                rating_match = re.search(r"(\d+,\d+)", rating_tag.text)
                if rating_match:
                    rating = float(rating_match.group(1).replace(",", "."))

            votes = None
            votes_text = item.select_one(
                "span.a-size-base.s-underline-text"
            ) or item.select_one("div.a-row.a-size-small span.a-size-base")
            if votes_text:
                cleaned = re.sub(r"[^\d]", "", votes_text.text.strip())
                if cleaned.isdigit():
                    votes = int(cleaned)

            sales = None
            sales_elem = item.find("span", class_="a-size-base a-color-secondary")
            if sales_elem and "acheté" in sales_elem.text:
                sales_match = re.search(r"(\d[\d\s]+)", sales_elem.text)
                if sales_match:
                    sales = int(
                        sales_match.group(1).replace(" ", "").replace("\u202f", "")
                    )

            image_elem = item.find("img")
            image_url = image_elem["src"] if image_elem else None

            url = None
            link_tag = item.select_one("a.a-link-normal.s-link-style.a-text-normal")
            if link_tag and link_tag.get("href"):
                url = "https://www.amazon.fr" + link_tag["href"]

            prime = bool(item.select_one("i.a-icon-prime"))

            products.append(
                {
                    "title": title,
                    "brand": title.split()[0] if title else None,
                    "price": price,
                    "rating": rating,
                    "votes": votes,
                    "sales_last_month": sales,
                    "image_url": image_url,
                    "url": url,
                    "prime": prime,
                    "category": "Objets connectés",
                    "rank": rank,
                    "scraped_at": datetime.now(),
                }
            )
        page += 1

    driver.quit()

    # 📦 Sauvegarde avec horodatage
    timestamp = datetime.now().strftime("%Y-%m-%d")
    save_path = data_dir / f"raw_data_{timestamp}.csv"
    pd.DataFrame(products).to_csv(save_path, index=False)
    print(f"✅ Données sauvegardées : {save_path}")

    return save_path, len(products), page - 1
