from pathlib import Path
import pandas as pd
import re
from datetime import datetime


def extract_category_ranks(classement_list):
    # Si la liste est vide ou invalide
    if not isinstance(classement_list, list) or len(classement_list) == 0:
        return "", ""

    # Première catégorie (générale)
    principale = classement_list[0] if len(classement_list) > 0 else ""
    secondaire = classement_list[1] if len(classement_list) > 1 else ""

    # Nettoyage des deux textes
    principale = principale.strip()
    secondaire = secondaire.strip()

    return principale, secondaire


def clean_amazon(raw_path: Path, details_path: Path) -> Path:
    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data" / "cleaned"
    data_dir.mkdir(parents=True, exist_ok=True)

    df_raw = pd.read_csv(raw_path)
    df_details = pd.read_csv(details_path)

    df = df_raw.merge(df_details, on="url", how="left")

    df = df.drop_duplicates().dropna(subset=["title"])

    for col in ["price", "rating", "votes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "category" in df.columns:
        df.rename(columns={"category": "categorie"}, inplace=True)
        df["categorie"] = (
            df["categorie"]
            .astype(str)
            .str.replace(">", "", regex=False)
            .str.replace("›", ".", regex=False)
            .str.split(".")
            .apply(lambda lst: [l.strip() for l in lst])
        )

    # Extraire les classements texte : "190 en High-Tech"
    if "classement" in df.columns:
        df["classement"] = (
            df["classement"].fillna("").apply(eval)
        )  # Convertir les strings en listes
        df[["categorie_principale", "categorie_secondaire"]] = df["classement"].apply(
            lambda x: pd.Series(extract_category_ranks(x))
        )
        df = df.drop(columns=["classement"])

    else:
        df["categorie_principale"] = ""
        df["categorie_secondaire"] = ""

    timestamp = datetime.now().strftime("%Y-%m-%d")
    clean_path = data_dir / f"df_clean_{timestamp}.csv"
    df.to_csv(clean_path, index=False)

    print(
        f"✅ Données nettoyées sauvegardées : {clean_path} (lignes restantes : {len(df)})"
    )
    return clean_path
