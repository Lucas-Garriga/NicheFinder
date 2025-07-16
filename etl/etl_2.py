from pathlib import Path
import sqlite3
import pandas as pd
import sys
from datetime import datetime

root = Path(__file__).resolve().parent.parent

sys.path.append(str(root / "scraper"))
sys.path.append(str(root / "cleaner"))

from scraper_2 import scrape_amazon
from scraper_async import run_scrape_async
from clean_data_2 import clean_amazon


def main():
    print("📌 Démarrage du scraping synchronisé")
    raw_file, n_products, max_pages = scrape_amazon(max_pages=200)
    print(f"📊 {n_products} produits extraits sur {max_pages} page(s)")

    print("\n📌 Démarrage du scraping asynchrone (détails produits)")
    details_file = run_scrape_async(pd.read_csv(raw_file)["url"].dropna().tolist())

    print("\n📌 Démarrage du nettoyage et fusion")
    clean_file = clean_amazon(raw_file, details_file)

    print("\n📌 Chargement dans SQLite")
    df = pd.read_csv(clean_file)
    db_dir = root / "data" / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d")
    db_path = db_dir / f"amazon_{timestamp}.db"

    with sqlite3.connect(db_path) as conn:
        df.to_sql("products", conn, if_exists="replace", index=False)

    print(f"✅ {len(df)} lignes chargées dans la base : {db_path}")


if __name__ == "__main__":
    main()
