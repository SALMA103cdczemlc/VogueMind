"""
VogueMind — Tableau de bord (Streamlit).

Lit la base de restitution PostgreSQL :
  - predictions      : scores de réachat écrits en temps réel par streaming/predict_stream.py
  - ind_*            : indicateurs de ventes exportés par batch/export_to_postgres.py
et le rapport du modèle (results/ml_metrics_<version>.json).
La partie « Temps réel » se rafraîchit toute seule toutes les 5 secondes.
"""
import glob
import json
import os

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st

PG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "voguemind"),
    "user": os.environ.get("POSTGRES_USER", "voguemind"),
    "password": os.environ.get("POSTGRES_PASSWORD", "voguemind"),
}
RESULTS_DIR = os.environ.get("VOGUEMIND_RESULTS", "/app/results")
REFRESH = os.environ.get("DASHBOARD_REFRESH", "5s")
ACCENT = "#b5174f"

st.set_page_config(page_title="VogueMind", layout="wide")


def query(sql):
    """Renvoie un DataFrame, ou None si la table n'existe pas encore."""
    try:
        with psycopg2.connect(**PG, connect_timeout=3) as conn:
            conn.set_client_encoding("UTF8")
            return pd.read_sql_query(sql, conn)
    except Exception as exc:  # table absente, base pas encore prête…
        st.session_state["last_error"] = str(exc).splitlines()[0]
        return None


def fmt(n, digits=0):
    return f"{n:,.{digits}f}".replace(",", " ") if n is not None else "—"


st.title("VogueMind")
st.caption("Pipeline Big Data H&M · Kafka → Spark → HDFS → Spark ML → PostgreSQL")

tab_live, tab_sales, tab_model = st.tabs(["Temps réel", "Ventes", "Modèle de réachat"])


# --------------------------------------------------------------------------- temps réel
with tab_live:
    @st.fragment(run_every=REFRESH)
    def live():
        stats = query("""
            SELECT COUNT(*) AS n, COALESCE(SUM(prediction), 0) AS n_pos,
                   AVG(proba_rachat) AS p_moy,
                   AVG(EXTRACT(EPOCH FROM scored_at - ingestion_ts)) AS latence,
                   MAX(scored_at) AS derniere, MAX(model_version) AS version
            FROM predictions""")
        if stats is None or not int(stats["n"][0]):
            st.info("Aucune prédiction pour l'instant. Lancer streaming/predict_stream.py "
                    "puis envoyer des transactions avec ingestion/producer.py.")
            return
        s = stats.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Transactions scorées", fmt(int(s["n"])))
        c2.metric("Réachat prédit (7 jours)", f"{100 * s['n_pos'] / s['n']:.1f} %")
        c3.metric("Probabilité moyenne", f"{s['p_moy']:.2f}")
        c4.metric("Délai producer → base", f"{s['latence']:.1f} s" if pd.notna(s["latence"]) else "—")
        st.caption(f"Dernière prédiction : {pd.to_datetime(s['derniere']):%d/%m/%Y %H:%M:%S} UTC "
                   f"· modèle {s['version']}")

        left, right = st.columns([3, 2])
        per_min = query("""
            SELECT date_trunc('minute', scored_at) AS minute, COUNT(*) AS transactions
            FROM predictions GROUP BY 1 ORDER BY 1 DESC LIMIT 60""")
        with left:
            st.markdown("**Transactions scorées par minute**")
            st.altair_chart(alt.Chart(per_min).mark_bar(color=ACCENT).encode(
                x=alt.X("minute:T", title=None), y=alt.Y("transactions:Q", title=None),
                tooltip=["minute:T", "transactions:Q"]).properties(height=240), use_container_width=True)
        hist = query("SELECT proba_rachat FROM predictions ORDER BY scored_at DESC LIMIT 5000")
        with right:
            st.markdown("**Répartition des probabilités de réachat**")
            st.altair_chart(alt.Chart(hist).mark_bar(color="#6b5b95").encode(
                x=alt.X("proba_rachat:Q", bin=alt.Bin(step=0.1), title="probabilité"),
                y=alt.Y("count():Q", title=None)).properties(height=240), use_container_width=True)

        st.markdown("**Dernières transactions scorées**")
        last = query("""
            SELECT scored_at, transaction_id, LEFT(customer_id, 12) AS client, article_id, t_dat,
                   ROUND(proba_rachat::numeric, 3) AS proba, prediction
            FROM predictions ORDER BY scored_at DESC, transaction_id LIMIT 15""")
        if last is not None:
            last["client"] = last["client"] + "…"
            last["prediction"] = last["prediction"].map({1: "oui", 0: "non"})
            last = last.rename(columns={"scored_at": "scoré à", "t_dat": "date d'achat",
                                        "proba": "proba réachat", "prediction": "réachat prédit"})
            st.dataframe(last, hide_index=True, use_container_width=True)

    live()


# --------------------------------------------------------------------------- ventes
with tab_sales:
    kpi = query("SELECT * FROM ind_kpi_global")
    if kpi is None or kpi.empty:
        st.info("Indicateurs non disponibles : lancer batch/indicators.py puis batch/export_to_postgres.py.")
    else:
        k = kpi.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Transactions", fmt(int(k["nb_transactions"])))
        c2.metric("Clients", fmt(int(k["nb_clients"])))
        c3.metric("Articles", fmt(int(k["nb_articles"])))
        c4.metric("Panier moyen", f"{k['panier_moyen']:.4f}")
        c5.metric("Articles / panier", f"{k['articles_par_panier']:.2f}")
        st.caption(f"Période : {k['premiere_date']} → {k['derniere_date']} · prix normalisés du dataset H&M")

        days = query("SELECT t_dat, nb_ventes, nb_clients FROM ind_ventes_par_jour ORDER BY t_dat")
        if days is not None:
            st.markdown("**Ventes par jour**")
            st.altair_chart(alt.Chart(days).mark_line(color=ACCENT, point=False).encode(
                x=alt.X("t_dat:T", title=None), y=alt.Y("nb_ventes:Q", title="ventes"),
                tooltip=["t_dat:T", "nb_ventes:Q", "nb_clients:Q"]).properties(height=260),
                use_container_width=True)

        left, right = st.columns(2)
        cat = query("SELECT * FROM ind_ventes_par_categorie ORDER BY nb_ventes DESC LIMIT 12")
        if cat is not None:
            with left:
                st.markdown("**Ventes par groupe de produits**")
                st.altair_chart(alt.Chart(cat).mark_bar(color=ACCENT).encode(
                    y=alt.Y("product_group_name:N", sort="-x", title=None),
                    x=alt.X("nb_ventes:Q", title=None),
                    tooltip=["product_group_name", "nb_ventes", "part_ca_pct"]).properties(height=320),
                    use_container_width=True)
        rayon = query("SELECT * FROM ind_ventes_par_rayon ORDER BY nb_ventes DESC")
        if rayon is not None:
            with right:
                st.markdown("**Part du chiffre d'affaires par rayon**")
                st.altair_chart(alt.Chart(rayon).mark_arc(innerRadius=60).encode(
                    theta="part_ca_pct:Q", color=alt.Color("index_group_name:N", title=None),
                    tooltip=["index_group_name", "part_ca_pct"]).properties(height=320),
                    use_container_width=True)

        top = query("SELECT article_id, prod_name, product_type_name, colour_group_name, nb_ventes "
                    "FROM ind_top_articles ORDER BY nb_ventes DESC")
        if top is not None:
            st.markdown("**Top 20 des articles**")
            st.dataframe(top, hide_index=True, use_container_width=True)


# --------------------------------------------------------------------------- modèle
with tab_model:
    files = sorted(glob.glob(os.path.join(RESULTS_DIR, "ml_metrics_*.json")))
    if not files:
        st.info("Aucun rapport de modèle : lancer batch/train_model.py.")
    else:
        rep = json.load(open(files[-1]))
        d = rep["data"]
        st.markdown(f"**Question :** ce client va-t-il racheter dans les {d['horizon_days']} prochains jours ? "
                    f"(variables calculées sur les {d['lookback_days']} jours précédents)")
        st.caption(f"Historique {d['history_start']} → {d['history_end']} · "
                   f"entraînement {d['train_rows']} lignes · test {d['test_rows']} lignes "
                   f"(coupure {d['test_cutoff']}) · taux de réachat {d['train_pos_rate']:.1%}")

        def table(rows):
            return pd.DataFrame([{"modèle": n, "précision": m["precision"], "rappel": m["recall"],
                                  "F1": m["f1"], "AUC": m.get("auc")} for n, m in rows])

        st.markdown("**Comparaison des trois techniques (validation)**")
        st.dataframe(table([("référence : achat < 7 j", rep["validation"]["reference"])]
                           + list(rep["validation"]["techniques"].items())),
                     hide_index=True, use_container_width=True)
        t = rep["tuning"]
        st.markdown(f"**Technique réglée :** {t['technique']} · meilleurs paramètres `{t['best_params']}`")
        m = rep["test"]["model"]
        st.markdown("**Évaluation finale (test)**")
        st.dataframe(table([("référence : achat < 7 j", rep["test"]["reference"]),
                            (f"{t['technique']} réglé", m)]), hide_index=True, use_container_width=True)
        cm = pd.DataFrame({"prédit : réachat": [m["tp"], m["fp"]], "prédit : pas de réachat": [m["fn"], m["tn"]]},
                          index=["réel : réachat", "réel : pas de réachat"])
        st.markdown("**Matrice de confusion (test)**")
        st.dataframe(cm, use_container_width=False)
