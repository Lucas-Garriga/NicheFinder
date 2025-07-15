from pathlib import Path
import sqlite3
import pandas as pd
import sys
from datetime import datetime

# 📌 Permet d’importer les autres modules du projet
root = Path(__file__).resolve().parent.parent
sys.path.append(str(root))

from utils.io_utils import get_project_root
from scraper.scraper import scrape_amazon
from cleaner.clean_data import clean_amazon


def main():
    root = get_project_root()

    # 1️⃣ Étape de scraping
    print("\n📌 Démarrage du scraping")
    raw_file, n_scraped, pages_scraped = scrape_amazon(max_pages=5)
    print(
        f"🔎 Scraping terminé : {n_scraped} produits trouvés sur {pages_scraped} page(s)"
    )

    # 2️⃣ Étape de nettoyage
    print("\n📌 Démarrage du nettoyage")
    clean_file, n_cleaned = clean_amazon(raw_file)
    print(f"🧼 Données nettoyées : {n_cleaned} lignes conservées après nettoyage")

    # 3️⃣ Chargement dans une base SQLite datée
    print("\n📌 Chargement des données dans une base SQLite")
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
