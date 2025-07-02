from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import re

# Configuration Selenium
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=options)

base_url = "https://www.amazon.fr/s?i=electronics&srs=4551203031&rh=n%3A4551203031&s=popularity-rank&fs=true&page={}"

products = []
page = 1
max_pages = 196  # Nombre de pages à scraper (ajuster si nécessaire)

while page <= max_pages:
    print(f"Scraping page {page}")
    driver.get(base_url.format(page))
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    items = soup.find_all("div", {"data-component-type": "s-search-result"})

    for rank, item in enumerate(items, start=1 + (page - 1) * len(items)):
        title_elem = item.h2
        title = title_elem.text.strip() if title_elem else None

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

        # Votes (avis clients)
        votes = None
        votes_text = None
        votes_elem = item.find("span", class_="a-size-base s-underline-text")
        if votes_elem:
            votes_text = votes_elem.text.strip()
        else:
            alt_votes = item.select_one("div.a-row.a-size-small span.a-size-base")
            if alt_votes:
                votes_text = alt_votes.text.strip()
        if votes_text:
            cleaned = re.sub(r"[^\d]", "", votes_text)
            if cleaned.isdigit():
                votes = int(cleaned)

        # Ventes le mois dernier
        sales = None
        sales_elem = item.find("span", class_="a-size-base a-color-secondary")
        if sales_elem and "acheté" in sales_elem.text:
            sales_match = re.search(r"(\d[\d\s]+)", sales_elem.text)
            if sales_match:
                sales = int(sales_match.group(1).replace(" ", "").replace("\u202f", ""))

        # Image
        image_elem = item.find("img")
        image_url = image_elem["src"] if image_elem else None

        # Marque
        brand = None
        if title:
            brand = title.split()[0]

        # URL produit
        url = None
        link_tag = item.select_one("a.a-link-normal.s-link-style.a-text-normal")
        if link_tag and link_tag.get("href"):
            url = "https://www.amazon.fr" + link_tag["href"]

        # Prime (booléen)
        prime = bool(item.select_one("i.a-icon-prime"))

        products.append(
            {
                "title": title,
                "brand": brand,
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

# Sauvegarde
dfv38 = pd.DataFrame(products)
dfv38.to_csv("amazon_objets_connectes_v38.csv", index=False)
print("✅ Données sauvegardées : amazon_objets_connectes_v38.csv")
