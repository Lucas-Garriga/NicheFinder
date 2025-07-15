# 📦 Imports principaux
import streamlit as st
import pandas as pd

from pathlib import Path
from datetime import datetime


st.set_page_config(page_title="Objets Connectés - Analyse du Marché", layout="wide")


@st.cache_data
def load_data():
    data_path = Path("data\df_clean.csv")
    df = pd.read_csv(data_path)
    return df


df = load_data()


total_produits = len(df)
moyenne_prix = df["price"].mean()
moyenne_note = df["rating"].mean()
produits_avec_ventes = df["sales_last_month"].notna().sum()


st.title("📊 Analyse du Marché - Objets Connectés")
st.subheader("Bienvenue ! Voici un aperçu de vos données collectées sur Amazon.fr")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Produits analysés", total_produits)
col2.metric("Prix moyen", f"{moyenne_prix:.2f} €")
col3.metric("Note moyenne", f"{moyenne_note:.2f} ⭐")
col4.metric("Produits avec ventes", produits_avec_ventes)


st.sidebar.title("🧭 Navigation")
page = st.sidebar.radio(
    "Aller vers :", ["Accueil", "Vue d'ensemble", "Top produits", "Recherche"]
)


if page == "Accueil":
    st.markdown(
        """
    ## 🏠 Accueil du projet
    Ce projet analyse les tendances du marché des **objets connectés** sur Amazon.

    **Sources :** Scraping Amazon.fr (catégorie objets connectés)

    **But :**
    - Identifier les produits les plus populaires
    - Analyser les notes et les ventes
    - Fournir des insights aux entreprises ou aux consommateurs

    *Projet réalisé dans le cadre d’un bootcamp Data Analyst.*
    """
    )

elif page == "Vue d'ensemble":
    st.write("🚧 Cette section sera développée ensuite.")
