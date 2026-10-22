"""
Application Streamlit : du fichier SBML au graphe de connaissances RDF, interrogé en SPARQL.
Lancer en local : streamlit run app.py
"""

from pathlib import Path

import streamlit as st

from pipeline import (QUERIES, build_rdf, parse_sbml, quality_check,
                      run_query, to_dataframes)

DEMO_PATH = Path(__file__).parent / "data" / "modele_demo.xml"

st.set_page_config(page_title="Metabolic KG Explorer", page_icon="🧬", layout="wide")

# ---------------------------------------------------------------------------
# Barre latérale : choix du réseau
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("🧬 Réseau métabolique")
    source = st.radio("Source du fichier SBML", ["Exemple de démonstration", "Importer mon fichier"])
    uploaded = None
    if source == "Importer mon fichier":
        uploaded = st.file_uploader("Fichier SBML (.xml ou .sbml)", type=["xml", "sbml"])
        st.caption("Exemples réels : modèles BiGG (bigg.ucsd.edu) ou BioModels.")
    st.divider()
    st.markdown(
        "**Pipeline**\n\n"
        "1. Lecture du SBML\n"
        "2. Tableaux pandas\n"
        "3. Contrôle qualité\n"
        "4. Graphe RDF (ChEBI, MetaNetX)\n"
        "5. Requêtes SPARQL"
    )
    st.caption("Python · xml.etree · pandas · rdflib · Streamlit")

if source == "Importer mon fichier" and uploaded is None:
    st.title("Du fichier SBML au graphe de connaissances")
    st.info("Importe un fichier SBML dans la barre latérale, ou repasse sur l'exemple de démonstration.")
    st.stop()


@st.cache_data(show_spinner=False)
def run_pipeline(content: bytes):
    """Étapes 1 à 4, mises en cache pour ne pas tout recalculer à chaque clic."""
    import io
    network_id, mets, reas, pars = parse_sbml(io.BytesIO(content))
    df_met, df_rea, df_par = to_dataframes(mets, reas, pars)
    errors, warnings, coverage = quality_check(df_met, df_rea, df_par)
    turtle = build_rdf(df_met, df_rea, df_par).serialize(format="turtle") if not errors else ""
    return network_id, df_met, df_rea, df_par, errors, warnings, coverage, turtle


content = uploaded.getvalue() if uploaded is not None else DEMO_PATH.read_bytes()

try:
    network_id, df_met, df_rea, df_par, errors, warnings, coverage, turtle = run_pipeline(content)
except Exception as exc:  # fichier illisible ou non SBML
    st.error(f"Impossible de lire ce fichier : {exc}")
    st.stop()

# Le graphe rdflib n'est pas mis en cache directement : on le recharge depuis le Turtle
from rdflib import Graph  # noqa: E402

g = Graph()
if turtle:
    g.parse(data=turtle, format="turtle")

# ---------------------------------------------------------------------------
# En-tête et indicateurs
# ---------------------------------------------------------------------------
st.title("Du fichier SBML au graphe de connaissances")
st.markdown(
    f"Réseau **`{network_id}`** : lecture du fichier, rangement en tableaux, contrôle qualité, "
    "conversion en triplets RDF reliés à **ChEBI** et **MetaNetX**, puis interrogation en **SPARQL**."
)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Métabolites", len(df_met))
c2.metric("Réactions", len(df_rea))
c3.metric("Participations", len(df_par))
c4.metric("Avertissements", len(warnings))
c5.metric("Triplets RDF", len(g))

tabs = st.tabs(["1 · Fichier SBML", "2 · Tableaux", "3 · Qualité", "4 · Graphe", "5 · RDF", "6 · SPARQL"])

# ---------------------------------------------------------------------------
# 1. Fichier SBML
# ---------------------------------------------------------------------------
with tabs[0]:
    st.subheader("Le fichier d'entrée")
    st.markdown(
        "Un fichier **SBML** est un XML qui décrit un réseau métabolique. On y lit quatre choses : "
        "le réseau (`<model>`), les métabolites (`<species>` avec leurs identifiants officiels), "
        "les réactions (`<reaction>`) et, pour chacune, ce qu'elle consomme et produit."
    )
    if uploaded is None:
        st.warning(
            "L'exemple contient **deux pièges volontaires** : le pyruvate a un identifiant MetaNetX "
            "mal formé (`MNXM under_review`), et le glucose et l'ATP partagent le même `MNXM3`. "
            "Le contrôle qualité (onglet 3) doit les détecter tout seul."
        )
    text = content.decode("utf-8", errors="replace")
    st.code(text[:20000] + ("\n…" if len(text) > 20000 else ""), language="xml")

# ---------------------------------------------------------------------------
# 2. Tableaux
# ---------------------------------------------------------------------------
with tabs[1]:
    st.subheader("Un modèle relationnel en trois tables")
    st.markdown(
        "**Métabolites** et **réactions** sont les nœuds, **participants** est la table de liens "
        "(relation plusieurs-à-plusieurs) : chaque ligne deviendra une arête du graphe."
    )
    st.markdown("**Métabolites**")
    st.dataframe(df_met.drop(columns="network_id"), hide_index=True, width="stretch")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Réactions**")
        st.dataframe(df_rea.drop(columns="network_id"), hide_index=True, width="stretch")
    with col_b:
        st.markdown("**Participants**")
        st.dataframe(df_par, hide_index=True, width="stretch")

# ---------------------------------------------------------------------------
# 3. Qualité
# ---------------------------------------------------------------------------
with tabs[2]:
    st.subheader("Contrôle qualité : bloquer ou signaler")
    st.markdown(
        "Une **erreur** rend les données inutilisables : le pipeline s'arrête. "
        "Un **avertissement** signale un doute sans tout arrêter : il est remonté aux biologistes."
    )
    if errors:
        for e in errors:
            st.error(e)
        st.error("Données invalides : le graphe RDF n'est pas construit.")
    else:
        st.success("Aucune erreur bloquante : la structure du réseau est cohérente.")
    if warnings:
        for w in warnings:
            st.warning(w)
    else:
        st.info("Aucun avertissement.")
    st.markdown("**Couverture des annotations**")
    cols = st.columns(len(coverage))
    for col, (nom, (n, total)) in zip(cols, coverage.items()):
        col.metric(nom, f"{n}/{total}")
        col.progress(n / total if total else 0.0)

# ---------------------------------------------------------------------------
# 4. Graphe
# ---------------------------------------------------------------------------
with tabs[3]:
    st.subheader("Le réseau dessiné comme un graphe")
    st.markdown("Ronds = métabolites, rectangles = réactions. Double flèche = réaction réversible.")
    max_nodes = 150
    if len(df_rea) > max_nodes:
        st.info(f"Réseau volumineux : affichage des {max_nodes} premières réactions.")
    shown = df_rea.head(max_nodes)
    shown_par = df_par[df_par["reaction_id"].isin(shown["id"])]
    names = dict(zip(df_met["id"], df_met["name"]))

    def q(s):
        return '"' + str(s).replace('"', "'") + '"'

    lines = ["digraph G {", "rankdir=LR;", 'node [fontname="Helvetica", fontsize=11];']
    for mid in shown_par["metabolite_id"].unique():
        lines.append(f'{q("m_" + mid)} [label={q(names.get(mid, mid))}, shape=ellipse, '
                     'style=filled, fillcolor="#DDF1E4", color="#2E8B57"];')
    for _, r in shown.iterrows():
        lines.append(f'{q("r_" + r["id"])} [label={q(r["id"])}, shape=box, style="rounded,filled", '
                     'fillcolor="#E3ECFA", color="#3B6FC4"];')
    rev = dict(zip(df_rea["id"], df_rea["reversible"]))
    for _, p in shown_par.iterrows():
        label = f'×{p["stoichiometry"]:g}'
        extra = ", dir=both" if rev.get(p["reaction_id"]) else ""
        if p["role"] == "reactant":
            lines.append(f'{q("m_" + p["metabolite_id"])} -> {q("r_" + p["reaction_id"])} '
                         f'[label={q(label)}{extra}];')
        else:
            lines.append(f'{q("r_" + p["reaction_id"])} -> {q("m_" + p["metabolite_id"])} '
                         f'[label={q(label)}{extra}];')
    lines.append("}")
    st.graphviz_chart("\n".join(lines), width="stretch")

# ---------------------------------------------------------------------------
# 5. RDF
# ---------------------------------------------------------------------------
with tabs[4]:
    st.subheader("Le graphe de connaissances RDF")
    st.markdown(
        "Chaque ligne de tableau devient un ou plusieurs **triplets** *sujet – prédicat – objet*. "
        "Les molécules pointent vers les **URI officielles ChEBI et MetaNetX**, ce qui rend le graphe "
        "interopérable. Un identifiant mal formé n'est jamais écrit."
    )
    if turtle:
        formats = {
            "Turtle (.ttl)": ("turtle", "ttl", "text/turtle", "turtle"),
            "RDF/XML (.rdf)": ("xml", "rdf", "application/rdf+xml", "xml"),
            "N-Triples (.nt)": ("nt", "nt", "application/n-triples", "text"),
        }
        choix_fmt = st.radio("Syntaxe", list(formats.keys()), horizontal=True)
        fmt, ext, mime, lang = formats[choix_fmt]
        st.caption(
            "Les trois syntaxes décrivent exactement les mêmes triplets. Turtle est la plus lisible, "
            "RDF/XML la plus ancienne (balises `rdf:Description rdf:about=…`), "
            "N-Triples écrit un triplet par ligne : idéale pour les gros volumes."
        )
        serialized = turtle if fmt == "turtle" else g.serialize(format=fmt)
        st.download_button(f"Télécharger le graphe ({choix_fmt})", serialized,
                           file_name=f"{network_id}.{ext}", mime=mime)
        st.code(serialized[:30000] + ("\n…" if len(serialized) > 30000 else ""), language=lang)
    else:
        st.error("Graphe non construit : voir les erreurs dans l'onglet Qualité.")

# ---------------------------------------------------------------------------
# 6. SPARQL
# ---------------------------------------------------------------------------
with tabs[5]:
    st.subheader("Interroger le graphe en SPARQL")
    st.markdown(
        "Une requête décrit un **motif de triplets** avec des variables (`?r`, `?m`). "
        "Les préfixes `ex:`, `bio:` et `rdfs:` sont ajoutés automatiquement."
    )
    if not turtle:
        st.error("Graphe non construit : impossible de lancer des requêtes.")
    else:
        choix = st.selectbox("Requête prête à l'emploi", list(QUERIES.keys()) + ["Requête libre"])
        default = QUERIES.get(choix, "SELECT ?s ?p ?o\nWHERE { ?s ?p ?o }\nLIMIT 20").strip()
        query = st.text_area("Requête SPARQL", value=default, height=220, key=f"q_{choix}")
        if st.button("Exécuter", type="primary"):
            try:
                result = run_query(g, query)
                st.caption(f"{len(result)} ligne(s)")
                st.dataframe(result, hide_index=True, width="stretch")
            except Exception as exc:
                st.error(f"Erreur dans la requête : {exc}")
        with st.expander("Aide-mémoire SPARQL"):
            st.markdown(
                "- `.` = « et » · `;` = même sujet\n"
                "- `UNION` = « ou »\n"
                "- `FILTER NOT EXISTS { … }` = « qui n'a pas »\n"
                "- La même variable dans deux triplets fait la **jointure** (comme un JOIN en SQL)\n"
                "- `GROUP BY` + `COUNT` pour agréger"
            )

st.divider()
st.caption("Mini-projet réalisé par Abdoulaye Diop · Préparation au poste d'ingénieur informatique MetExplore (INRAE).")
