import streamlit as st
import pandas as pd
import os
from datetime import datetime
import plotly.express as px

# === CONFIG ===
DATA_PATH = os.path.join("data", "../data/cleaned/df_clean_2025-07-16.csv")


@st.cache_data
def load_data():
    df = pd.read_csv(DATA_PATH, parse_dates=["scraped_at"])
    df["votes"] = df["votes"].fillna(0)
    df["sales_last_month"] = df["sales_last_month"].fillna(0)
    df["price"] = df["price"].fillna(0)
    df["rating"] = df["rating"].fillna(0)
    df["brand"] = df["brand"].fillna("Inconnu")
    return df


df = load_data()

# === SIDEBAR ===
st.sidebar.title("🎛️ Filtres")
min_votes = st.sidebar.slider("Nombre minimal de votes", 0, 10000, 1000, 100)
min_rating = st.sidebar.slider("Note minimale", 0.0, 5.0, 4.0, 0.1)
max_price = st.sidebar.slider("Prix maximal (€)", 0, 1000, 500, 10)
prime_only = st.sidebar.checkbox("Afficher uniquement les produits Prime", value=False)

df_filtered = df[
    (df["votes"] >= min_votes)
    & (df["rating"] >= min_rating)
    & (df["price"] <= max_price)
]

if prime_only and "prime" in df_filtered.columns:
    df_filtered = df_filtered[df_filtered["prime"] == True]

# === TITRE ===
st.title("📊 Dashboard Amazon - Objets Connectés")
st.caption(f"🕒 Données du : {df['scraped_at'].max().strftime('%d/%m/%Y %H:%M')}")

# === KPI ===
st.subheader("🔢 Indicateurs")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Produits analysés", len(df))
col2.metric("Après filtre", len(df_filtered))
col3.metric("Prix moyen", f"{df_filtered['price'].mean():.2f} €")
col4.metric("Ventes connues", df_filtered["sales_last_month"].astype(bool).sum())

# === TOP PRODUITS ===
st.subheader("🏆 Top 10 produits fiables")
top = df_filtered.sort_values(by=["rating", "votes"], ascending=[False, False]).head(10)
st.dataframe(top[["title", "brand", "price", "rating", "votes", "sales_last_month"]])

# === PRODUITS LES PLUS VENDUS ===
st.subheader("🔥 Top 10 produits les plus vendus")
best_sellers = (
    df[df["sales_last_month"] > 0]
    .sort_values(by="sales_last_month", ascending=False)
    .head(10)
)
st.dataframe(best_sellers[["title", "brand", "price", "sales_last_month", "rating"]])

# === MARQUES PRINCIPALES ===
st.subheader("🏷️ Marques dominantes")
brand_counts = df_filtered["brand"].value_counts().head(10)
fig_brand = px.bar(
    brand_counts,
    x=brand_counts.index,
    y=brand_counts.values,
    labels={"x": "Marque", "y": "Nombre de produits"},
)
st.plotly_chart(fig_brand, use_container_width=True)

# === RÉPARTITION PRIX vs RATING ===
st.subheader("📉 Relation Note / Prix")
fig_scatter = px.scatter(
    df_filtered,
    x="price",
    y="rating",
    size="votes",
    hover_name="title",
    color="brand",
    title="Prix vs Note (taille = nombre de votes)",
)
st.plotly_chart(fig_scatter, use_container_width=True)

# === ÉVOLUTION SCRAPING ===
st.subheader("📆 Produits ajoutés par jour")
by_day = df["scraped_at"].dt.date.value_counts().sort_index()
fig_time = px.line(
    x=by_day.index, y=by_day.values, labels={"x": "Date", "y": "Produits scrapés"}
)
st.plotly_chart(fig_time, use_container_width=True)

# === DATASET ===
st.subheader("💾 Télécharger le fichier")
st.download_button(
    "📥 Télécharger CSV", df.to_csv(index=False), "amazon_data.csv", "text/csv"
)
