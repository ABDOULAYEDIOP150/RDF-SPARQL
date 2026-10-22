"""
Metabolic KG Explorer : collection de réseaux métaboliques -> graphe de connaissances RDF.
Lancer en local : streamlit run app.py
"""

import io
import itertools

import pandas as pd
import streamlit as st

from kg import DEMO_QUERIES, KG_QUERIES, build_rdf, parse_sbml, quality_check, run_query, store, to_dataframes
from kg.graph import build_graph, currency_metabolites, path_to_dot, shortest_path

REPO = "https://github.com/ABDOULAYEDIOP150/RDF-SPARQL"
DEMO_PATH = store.RAW / "demo_glycolysis.xml"

st.set_page_config(page_title="Metabolic KG Explorer", page_icon="🧬", layout="wide")


# ---------------------------------------------------------------------------
# Chargement des sorties du job Spark (mises en cache)
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner="Chargement des tables Parquet…")
def tables():
    return store.load_tables()


@st.cache_data
def quality_report():
    return store.load_quality()


@st.cache_data
def shared_chebi():
    df = store.load_shared()
    df["networks"] = df["networks"].apply(lambda x: ", ".join(x))
    return df


@st.cache_data
def top_hubs():
    return store.load_top_hubs()


@st.cache_resource(show_spinner="Chargement du graphe de connaissances (≈135 000 triplets)…")
def knowledge_graph():
    return store.load_kg()


@st.cache_resource(show_spinner="Construction du graphe NetworkX…")
def network_graph(network_id):
    met, rea, par = store.load_network(network_id, tables())
    return build_graph(met, rea, par), met, rea, par


report = quality_report()
networks = report["networks"]
valid_networks = [n for n, r in networks.items() if r["status"] == "validé"]

# ---------------------------------------------------------------------------
# En-tête
# ---------------------------------------------------------------------------
st.title("🧬 Metabolic KG Explorer")
st.markdown(
    "Une **collection de réseaux métaboliques** (SBML) traitée par un **pipeline PySpark**, "
    "contrôlée, convertie en **graphe de connaissances RDF** relié à **ChEBI** et **MetaNetX**, "
    "puis interrogée en **SPARQL**, analysée par la **théorie des graphes** et exposée par une **API**."
)
met_all, rea_all, par_all = tables()
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Réseaux validés", f"{len(valid_networks)}/{len(networks)}")
c2.metric("Métabolites", f"{len(met_all):,}".replace(",", " "))
c3.metric("Réactions", f"{len(rea_all):,}".replace(",", " "))
c4.metric("Liens réaction-métabolite", f"{len(par_all):,}".replace(",", " "))
c5.metric("Triplets RDF", f"{report['triples']:,}".replace(",", " "))

tabs = st.tabs(["🏠 Architecture", "🗂️ Data lake & qualité", "🕸️ Théorie des graphes",
                "🔗 Graphe fusionné", "❓ SPARQL", "🔬 Mini-projet pas à pas", "🔌 API & CI/CD"])

# ---------------------------------------------------------------------------
# 0. Architecture
# ---------------------------------------------------------------------------
with tabs[0]:
    st.subheader("Architecture du projet")
    st.graphviz_chart("""
digraph A {
  rankdir=LR; node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, color="#3B6FC4", fillcolor="#E3ECFA"];
  raw [label="Data lake brut\\nfichiers SBML\\n(data/raw)", fillcolor="#FFF4D6", color="#C99A2E"];
  spark [label="Job PySpark\\nlecture parallèle\\nqualité + quarantaine", fillcolor="#FCE3D6", color="#D2691E"];
  pq [label="Tables Parquet\\npartitionnées par réseau", fillcolor="#DDF1E4", color="#2E8B57"];
  nt [label="Graphe RDF\\nN-Triples\\n(ChEBI, MetaNetX)", fillcolor="#EDE3FA", color="#7B4FC4"];
  gf [label="Sommets / arêtes\\nGraphFrames", fillcolor="#DDF1E4", color="#2E8B57"];
  api [label="API FastAPI\\n/sparql /path /hubs"];
  app [label="App Streamlit"];
  ci [label="CI/CD GitHub Actions\\ntests · Spark · Docker", shape=note, fillcolor="#F2F2F2", color="#777777"];
  raw -> spark -> {pq nt gf};
  {pq nt} -> api; {pq nt gf} -> app;
  ci -> spark [style=dashed]; ci -> api [style=dashed];
}""", width="stretch")
    st.markdown("**Ce que demande l'offre, et où c'est dans le projet**")
    st.dataframe(pd.DataFrame([
        ["Générer des graphes de connaissances à partir de collections de réseaux métaboliques",
         "Job PySpark : 6 réseaux → 1 graphe RDF fusionné de 135 000 triplets"],
        ["Big Data, data lake", "Lecture parallèle du data lake, Parquet partitionné par réseau, quarantaine"],
        ["RDF / SPARQL, Web sémantique", "Liens vers les URI officielles ChEBI et MetaNetX, onglet SPARQL"],
        ["Apache Spark, idéalement GraphFrames",
         "pipeline_spark.py + graphframes_analysis.py (degrés, motifs, composantes connexes)"],
        ["Interroger efficacement les bases de connaissances",
         "Requêtes inter-réseaux (molécules communes à tous les organismes…)"],
        ["Développer une API pour des applications externes", "API FastAPI documentée (/docs), image Docker"],
        ["Théorie des graphes", "Hubs, plus court chemin, métabolites « monnaie »"],
        ["Collaboration avec les biologistes",
         "Avertissements qualité remontés au lieu d'être corrigés automatiquement"],
    ], columns=["Point de l'offre", "Réponse dans le projet"]), hide_index=True, width="stretch")
    st.markdown(f"Code source : [{REPO}]({REPO})")

# ---------------------------------------------------------------------------
# 1. Data lake & qualité
# ---------------------------------------------------------------------------
with tabs[1]:
    st.subheader("Le data lake et le contrôle qualité réseau par réseau")
    st.markdown(
        "Le job Spark lit tous les fichiers en parallèle. Une **erreur** (lien cassé, stœchiométrie "
        "négative…) met le réseau en **quarantaine** : il est exclu, les autres continuent. "
        "Un **avertissement** est remonté aux biologistes sans bloquer."
    )
    rows = []
    for nid, r in networks.items():
        rows.append({
            "Réseau": nid, "Statut": {"validé": "✅ validé", "quarantaine": "⛔ quarantaine"}.get(
                r["status"], "❌ " + r["status"]),
            "Métabolites": r["metabolites"], "Réactions": r["reactions"],
            "Couverture ChEBI": f"{100 * r['with_chebi'] / max(r['metabolites'], 1):.0f} %",
            "Couverture MetaNetX": f"{100 * r['with_metanetx'] / max(r['metabolites'], 1):.0f} %",
            "Erreurs": len(r["errors"]), "Avertissements": len(r["warnings"]),
        })
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    st.caption("Sources : e_coli_core, iJO1366 (E. coli) et iYS1720 (Salmonella) issus de BiGG via cobrapy, "
               "yeast_GEM (levure, SysBioChalmers). Les deux réseaux « demo » sont des jeux de test.")
    for nid, r in networks.items():
        if r["errors"] or r["warnings"]:
            with st.expander(f"{nid} : {len(r['errors'])} erreur(s), {len(r['warnings'])} avertissement(s)"):
                for e in r["errors"]:
                    st.error(e)
                for w in r["warnings"]:
                    st.warning(w)
    st.info("Exemple réel détecté dans iJO1366 : le D-xylulose et le L-xylulose portent le même "
            "identifiant MetaNetX alors que ce sont deux molécules différentes (stéréoisomères).")

# ---------------------------------------------------------------------------
# 2. Théorie des graphes
# ---------------------------------------------------------------------------
with tabs[2]:
    st.subheader("Le réseau vu comme un graphe")
    st.markdown("Graphe biparti orienté : **métabolite → réaction → métabolite** "
                "(une réaction réversible ajoute les arcs inverses).")
    net = st.selectbox("Réseau", valid_networks, index=valid_networks.index("e_coli_core")
                       if "e_coli_core" in valid_networks else 0)
    g, met, rea, par = network_graph(net)
    a, b, c = st.columns(3)
    a.metric("Sommets", g.number_of_nodes())
    b.metric("Arcs", g.number_of_edges())
    c.metric("Densité", f"{g.number_of_edges() / max(g.number_of_nodes() ** 2, 1):.5f}")

    st.markdown("**Les hubs** (calculés par Spark avec une fonction de fenêtre) : les métabolites "
                "qui participent au plus grand nombre de réactions.")
    h = top_hubs()
    h = h[h["network_id"] == net].sort_values("rang").head(15)
    st.bar_chart(h.set_index("name")["nb_reactions"], horizontal=True)

    st.markdown("**Plus court chemin entre deux métabolites**")
    labels = (met["name"] + " · " + met["id"]).tolist()
    ids = met["id"].tolist()
    def default(candidates):
        for cnd in candidates:
            if cnd in ids:
                return ids.index(cnd)
        return 0
    col1, col2 = st.columns(2)
    src = col1.selectbox("Départ", range(len(ids)), format_func=lambda i: labels[i],
                         index=default(["M_glc__D_e", "glc_c"]))
    dst = col2.selectbox("Arrivée", range(len(ids)), format_func=lambda i: labels[i],
                         index=default(["M_succ_e", "lac_c"]))
    excl = st.toggle("Exclure les métabolites « monnaie » (H₂O, ATP, NAD(P)H, H⁺… repérés par ChEBI)",
                     value=True)
    excluded = currency_metabolites(met) if excl else set()
    res = shortest_path(g, ids[src], ids[dst], excluded)
    if res is None:
        st.warning("Aucun chemin entre ces deux métabolites.")
    else:
        st.caption(f"{(len(res) - 1) // 2} réaction(s). Sans exclusion, le chemin passe souvent par H⁺ "
                   "ou H₂O : un raccourci sans sens biologique, c'est tout l'intérêt de l'option.")
        st.graphviz_chart(path_to_dot(res), width="stretch")
        st.dataframe(res, hide_index=True, width="stretch")

# ---------------------------------------------------------------------------
# 3. Graphe fusionné
# ---------------------------------------------------------------------------
with tabs[3]:
    st.subheader("Relier les organismes grâce aux identifiants ChEBI")
    st.markdown(
        "Chaque modèle a ses propres identifiants (`M_pyr_c`, `s_1399`…). Grâce aux **URI ChEBI**, "
        "le graphe sait que ce sont **la même molécule** : c'est ce qui permet d'interroger des "
        "milliers de réseaux d'un coup."
    )
    shared = shared_chebi()
    n_min = st.slider("Présente dans au moins … réseaux", 2, int(shared["nb_networks"].max()), 4)
    search = st.text_input("Filtrer par nom de molécule", "")
    view = shared[shared["nb_networks"] >= n_min]
    if search:
        view = view[view["example_name"].str.contains(search, case=False, regex=False)]
    st.caption(f"{len(view)} molécule(s)")
    st.dataframe(view.rename(columns={"example_name": "nom", "nb_networks": "nb réseaux",
                                      "networks": "réseaux"}),
                 hide_index=True, width="stretch", height=300)

    st.markdown("**Molécules partagées entre deux réseaux** (nombre d'identifiants ChEBI communs)")
    chebi_sets = (met_all[met_all["network_id"].isin(valid_networks)]
                  .explode("chebi_ids").dropna(subset=["chebi_ids"])
                  .groupby("network_id")["chebi_ids"].apply(set))
    nets = [n for n in valid_networks if n in chebi_sets.index]
    mat = pd.DataFrame(0, index=nets, columns=nets)
    for x, y in itertools.product(nets, nets):
        mat.loc[x, y] = len(chebi_sets[x] & chebi_sets[y])
    st.dataframe(mat, width="stretch")

# ---------------------------------------------------------------------------
# 4. SPARQL
# ---------------------------------------------------------------------------
with tabs[4]:
    st.subheader("Interroger le graphe de connaissances fusionné")
    st.markdown("Préfixes déjà déclarés : `ex:` `bio:` `chebi:` `rdfs:`. Vocabulaire : "
                "`bio:Metabolite`, `bio:Reaction`, `bio:inNetwork`, `bio:hasReactant`, `bio:hasProduct`, "
                "`bio:hasChEBI`, `bio:hasMetaNetX`, `bio:isReversible`, `bio:inCompartment`.")
    choix = st.selectbox("Requête prête à l'emploi", list(KG_QUERIES) + ["Requête libre"])
    default_q = KG_QUERIES.get(choix, "SELECT ?s ?p ?o\nWHERE { ?s ?p ?o }\nLIMIT 20")
    query = st.text_area("Requête SPARQL", value=default_q, height=230, key=f"kg_{choix}")
    if st.button("Exécuter", type="primary", key="run_kg"):
        try:
            out = run_query(knowledge_graph(), query, limit=2000)
            st.caption(f"{len(out)} ligne(s) (affichage limité à 2 000)")
            st.dataframe(out, hide_index=True, width="stretch")
        except Exception as exc:
            st.error(f"Erreur dans la requête : {exc}")
    with st.expander("Aide-mémoire SPARQL"):
        st.markdown("- `.` = « et » · `;` = même sujet · `UNION` = « ou »\n"
                    "- `FILTER NOT EXISTS { … }` = « qui n'a pas »\n"
                    "- même variable dans deux triplets = **jointure** (comme un JOIN SQL)\n"
                    "- `GROUP BY` + `COUNT`, `HAVING` pour filtrer après agrégation")

# ---------------------------------------------------------------------------
# 5. Mini-projet pas à pas
# ---------------------------------------------------------------------------
with tabs[5]:
    st.subheader("Le mini-projet : un seul fichier SBML, de bout en bout")
    up = st.file_uploader("Importer un fichier SBML (.xml, .sbml ou .xml.gz), sinon l'exemple est utilisé",
                          type=["xml", "sbml", "gz"])
    content = up.getvalue() if up is not None else DEMO_PATH.read_bytes()
    try:
        nid, mets, reas, pars = parse_sbml(io.BytesIO(content))
        dm, dr, dp = to_dataframes(mets, reas, pars)
        errs, warns, cov = quality_check(dm, dr, dp)
    except Exception as exc:
        st.error(f"Impossible de lire ce fichier : {exc}")
        nid, dm, dr, dp, errs, warns, cov = None, None, None, None, ["illisible"], [], {}
    g1 = build_rdf(dm, dr, dp) if not errs else None
    if dm is not None:
        st.markdown(f"Réseau **`{nid}`** : {len(dm)} métabolites, {len(dr)} réactions, "
                    f"{len(dp)} participations, {len(g1) if g1 is not None else 0} triplets.")
        s1, s2, s3, s4, s5 = st.tabs(["1 · SBML", "2 · Tableaux", "3 · Qualité", "4 · RDF", "5 · SPARQL"])
        with s1:
            if up is None:
                st.warning("Deux pièges volontaires : `MNXM under_review` (mal formé) pour le pyruvate, "
                           "et `MNXM3` partagé par le glucose et l'ATP.")
            text = content.decode("utf-8", errors="replace") if content[:2] != b"\x1f\x8b" else "(fichier compressé)"
            st.code(text[:15000], language="xml")
        with s2:
            st.dataframe(dm.drop(columns="network_id"), hide_index=True, width="stretch")
            x, y = st.columns(2)
            x.dataframe(dr.drop(columns="network_id"), hide_index=True, width="stretch")
            y.dataframe(dp.drop(columns="network_id"), hide_index=True, width="stretch")
        with s3:
            if not errs:
                st.success("Aucune erreur bloquante.")
            for e in errs:
                st.error(e)
            for w in warns[:200]:
                st.warning(w)
            for name, (n, total) in cov.items():
                st.progress(n / total if total else 0.0, text=f"Couverture {name} : {n}/{total}")
        with s4:
            if g1 is None:
                st.error("Graphe non construit : erreurs bloquantes.")
            else:
                fmt = st.radio("Syntaxe", ["turtle", "xml", "nt"], horizontal=True,
                               format_func={"turtle": "Turtle", "xml": "RDF/XML", "nt": "N-Triples"}.get)
                txt = g1.serialize(format=fmt)
                ext = {"turtle": "ttl", "xml": "rdf"}.get(fmt, "nt")
                st.download_button("Télécharger", txt, file_name=f"{nid}.{ext}")
                st.code(txt[:30000], language="turtle" if fmt == "turtle" else "xml" if fmt == "xml" else "text")
        with s5:
            if g1 is not None:
                q = st.selectbox("Requête", list(DEMO_QUERIES))
                st.code(DEMO_QUERIES[q], language="sparql")
                st.dataframe(run_query(g1, DEMO_QUERIES[q]), hide_index=True, width="stretch")

# ---------------------------------------------------------------------------
# 6. API & CI/CD
# ---------------------------------------------------------------------------
with tabs[6]:
    st.subheader("API REST pour les applications externes")
    st.markdown("L'API FastAPI expose le graphe de connaissances. Documentation interactive sur `/docs`.")
    st.dataframe(pd.DataFrame([
        ["GET", "/networks", "Réseaux du data lake et statut qualité"],
        ["GET", "/networks/{id}/metabolites?search=", "Recherche de métabolites"],
        ["GET", "/networks/{id}/hubs?top=10", "Métabolites les plus connectés"],
        ["GET", "/networks/{id}/path?source=&target=", "Plus court chemin métabolique"],
        ["GET", "/shared-metabolites?min_networks=2", "Molécules communes à plusieurs organismes"],
        ["GET", "/sparql/examples", "Requêtes SPARQL prêtes à l'emploi"],
        ["POST", "/sparql", "Exécuter une requête SPARQL SELECT"],
    ], columns=["Méthode", "Route", "Rôle"]), hide_index=True, width="stretch")
    st.code("""# Lancer l'API en local
uvicorn api.main:app --reload            # puis http://localhost:8000/docs

# ou avec Docker (image publiée par la CI sur GitHub Container Registry)
docker run -p 8000:8000 ghcr.io/abdoulayediop150/rdf-sparql-api:latest

# Exemple d'appel
curl "http://localhost:8000/networks/e_coli_core/path?source=M_glc__D_e&target=M_succ_e"
""", language="bash")
    st.subheader("CI/CD avec GitHub Actions")
    st.markdown(
        "À chaque push :\n"
        "1. **Qualité du code** : `ruff` puis les tests `pytest` (lecture SBML, qualité, RDF, graphes, API).\n"
        "2. **Pipeline Spark** : Java 17 + PySpark, le job tourne sur tout le data lake et ses sorties "
        "sont vérifiées, puis l'analyse **GraphFrames** est lancée.\n"
        "3. **Livraison** : l'image Docker de l'API est construite et publiée sur GitHub Container Registry "
        "(branche `main`) ; Streamlit Cloud redéploie l'application automatiquement."
    )
    st.markdown(f"![CI]({REPO}/actions/workflows/ci.yml/badge.svg)")

st.divider()
st.caption(f"Projet réalisé par Abdoulaye Diop · [Code source]({REPO})")
