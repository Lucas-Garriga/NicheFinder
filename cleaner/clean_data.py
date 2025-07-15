import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# 📌 Ajoute le dossier racine au chemin pour importer utils
root = Path(__file__).resolve().parent.parent
sys.path.append(str(root))

from utils.io_utils import get_project_root, make_dir


def clean_amazon(raw_path: Path) -> Path:
    """Nettoie un fichier CSV et retourne le chemin du fichier nettoyé."""
    root = get_project_root()
    data_dir = root / "data" / "cleaned"
    make_dir(data_dir)  # 📁 Crée le dossier s’il n'existe pas

    if not raw_path.exists():
        raise FileNotFoundError(f"{raw_path} introuvable.")

    df = pd.read_csv(raw_path)
    df = df.drop_duplicates().dropna(subset=["title"])

    for col in ["price", "rating", "votes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 🕒 Fichier nettoyé avec date
    timestamp = datetime.now().strftime("%Y-%m-%d")
    clean_path = data_dir / f"df_clean_{timestamp}.csv"
    df.to_csv(clean_path, index=False)
    print(f"✅ Données nettoyées : {clean_path}")

    return clean_path, len(df)
