"""
Pipeline SBML -> tableaux -> contrôle qualité -> graphe RDF -> SPARQL.

Version 100 % Python (xml.etree, pandas, rdflib), sans Spark.
Chaque étape est une fonction simple, réutilisée par l'app Streamlit (app.py)
et par la ligne de commande (python pipeline.py data/modele_demo.xml).
"""

import re
import sys
import xml.etree.ElementTree as ET

import pandas as pd
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD

RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDF_RESOURCE = "{" + RDF_NS + "}resource"

# Espaces de noms du graphe RDF
EX = Namespace("https://example.org/metabolisme/")               # nos ressources
BIO = Namespace("https://example.org/metabolisme/vocabulaire/")  # notre vocabulaire
CHEBI = Namespace("http://purl.obolibrary.org/obo/CHEBI_")       # URI officielles ChEBI
MNX = Namespace("https://identifiers.org/metanetx.chemical/")    # URI MetaNetX

PREFIXES = """
PREFIX ex:   <https://example.org/metabolisme/>
PREFIX bio:  <https://example.org/metabolisme/vocabulaire/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""


# ---------------------------------------------------------------------------
# Étape 1 : lire le XML et extraire les informations
# ---------------------------------------------------------------------------
def parse_sbml(source):
    """Lit un fichier SBML (chemin ou objet fichier) et renvoie 3 listes de dictionnaires."""
    root = ET.parse(source).getroot()
    # L'espace de noms SBML dépend du niveau/version : on le lit dans la balise racine
    sbml_ns = root.tag.split("}")[0].strip("{")
    ns = {"sbml": sbml_ns, "rdf": RDF_NS}

    model = root.find("sbml:model", ns)
    if model is None:
        raise ValueError("Aucune balise <model> trouvée : ce n'est pas un fichier SBML valide.")
    network_id = model.get("id", "reseau")

    # 1a. Métabolites et identifiants externes
    metabolites = []
    for sp in model.findall(".//sbml:species", ns):
        chebi, mnx = [], []
        for li in sp.findall(".//rdf:li", ns):
            uri = li.get(RDF_RESOURCE, "")
            if "identifiers.org/chebi/" in uri:
                chebi.append(uri.rsplit("/", 1)[-1])        # garde "CHEBI:17234"
            elif "identifiers.org/metanetx.chemical/" in uri:
                mnx.append(uri.rsplit("/", 1)[-1])          # garde "MNXM3"
        metabolites.append({
            "id": sp.get("id"),
            "name": sp.get("name") or sp.get("id"),
            "compartment": sp.get("compartment"),
            "chebi_ids": chebi,
            "metanetx_ids": mnx,
            "network_id": network_id,
        })

    # 1b. Réactions et participants (réactifs et produits)
    reactions, participants = [], []
    for r in model.findall(".//sbml:reaction", ns):
        reactions.append({
            "id": r.get("id"),
            "name": r.get("name") or r.get("id"),
            "reversible": r.get("reversible") == "true",
            "network_id": network_id,
        })
        for role, liste in [("reactant", "listOfReactants"), ("product", "listOfProducts")]:
            for ref in r.findall(f"sbml:{liste}/sbml:speciesReference", ns):
                participants.append({
                    "reaction_id": r.get("id"),
                    "metabolite_id": ref.get("species"),
                    "role": role,
                    "stoichiometry": float(ref.get("stoichiometry", 1)),
                })

    return network_id, metabolites, reactions, participants


# ---------------------------------------------------------------------------
# Étape 2 : ranger les données dans des tableaux
# ---------------------------------------------------------------------------
def to_dataframes(metabolites, reactions, participants):
    df_met = pd.DataFrame(metabolites,
                          columns=["id", "name", "compartment", "chebi_ids", "metanetx_ids", "network_id"])
    df_rea = pd.DataFrame(reactions, columns=["id", "name", "reversible", "network_id"])
    df_par = pd.DataFrame(participants,
                          columns=["reaction_id", "metabolite_id", "role", "stoichiometry"])
    return df_met, df_rea, df_par


# ---------------------------------------------------------------------------
# Étape 3 : contrôler la qualité
# ---------------------------------------------------------------------------
def quality_check(df_met, df_rea, df_par):
    """Renvoie (erreurs bloquantes, avertissements, couverture des annotations)."""
    errors, warnings = [], []

    if df_met["id"].duplicated().any():
        errors.append("Identifiant de métabolite en double")
    if df_rea["id"].duplicated().any():
        errors.append("Identifiant de réaction en double")
    if not df_par["metabolite_id"].isin(df_met["id"]).all():
        errors.append("Un participant pointe vers un métabolite inconnu")
    if not df_par["reaction_id"].isin(df_rea["id"]).all():
        errors.append("Un participant pointe vers une réaction inconnue")
    if (df_par["stoichiometry"] <= 0).any():
        errors.append("Stœchiométrie nulle ou négative")

    # Identifiants MetaNetX mal formés (ex. : un espace dans l'identifiant)
    annot = df_met.explode("metanetx_ids").dropna(subset=["metanetx_ids"])
    mal_formes = annot[~annot["metanetx_ids"].str.fullmatch(r"MNXM\d+")]
    for _, row in mal_formes.iterrows():
        warnings.append(f"MetaNetX mal formé pour {row['id']} : '{row['metanetx_ids']}'")

    # Un même identifiant externe porté par plusieurs métabolites
    partages = annot.groupby("metanetx_ids")["id"].apply(list)
    for ident, ids in partages.items():
        if len(ids) > 1:
            warnings.append(f"{ident} est porté par plusieurs métabolites : {ids}")

    coverage = {}
    for col, nom in [("chebi_ids", "ChEBI"), ("metanetx_ids", "MetaNetX")]:
        coverage[nom] = (int(df_met[col].apply(len).gt(0).sum()), len(df_met))

    return errors, warnings, coverage


def is_valid_mnx(ident):
    return bool(re.fullmatch(r"MNXM\d+", ident))


# ---------------------------------------------------------------------------
# Étape 4 : construire le graphe RDF
# ---------------------------------------------------------------------------
def build_rdf(df_met, df_rea, df_par):
    g = Graph()
    for prefix, ns in [("ex", EX), ("bio", BIO), ("chebi", CHEBI), ("mnx", MNX)]:
        g.bind(prefix, ns)

    # 4a. Métabolites
    for _, row in df_met.iterrows():
        m = EX[f"metabolite/{row['id']}"]
        g.add((m, RDF.type, BIO.Metabolite))
        g.add((m, RDFS.label, Literal(row["name"], lang="en")))
        g.add((m, BIO.inCompartment, EX[f"compartment/{row['compartment']}"]))
        for c in row["chebi_ids"]:
            g.add((m, BIO.hasChEBI, CHEBI[c.replace("CHEBI:", "")]))
        for x in row["metanetx_ids"]:
            if is_valid_mnx(x):          # on n'écrit jamais un identifiant mal formé
                g.add((m, BIO.hasMetaNetX, MNX[x]))

    # 4b. Réactions
    for _, row in df_rea.iterrows():
        r = EX[f"reaction/{row['id']}"]
        g.add((r, RDF.type, BIO.Reaction))
        g.add((r, RDFS.label, Literal(row["name"], lang="fr")))
        g.add((r, BIO.isReversible, Literal(bool(row["reversible"]), datatype=XSD.boolean)))

    # 4c. Liens réaction -> métabolite
    for _, row in df_par.iterrows():
        r = EX[f"reaction/{row['reaction_id']}"]
        m = EX[f"metabolite/{row['metabolite_id']}"]
        g.add((r, BIO.hasReactant if row["role"] == "reactant" else BIO.hasProduct, m))

    return g


# ---------------------------------------------------------------------------
# Étape 5 : interroger le graphe en SPARQL
# ---------------------------------------------------------------------------
QUERIES = {
    "Q1 · Que consomme et que produit chaque réaction ?": """
SELECT ?reaction ?role ?metabolite
WHERE {
  ?r a bio:Reaction ; rdfs:label ?reaction .
  { ?r bio:hasReactant ?m . BIND("consomme" AS ?role) }
  UNION
  { ?r bio:hasProduct  ?m . BIND("produit"  AS ?role) }
  ?m rdfs:label ?metabolite .
}
ORDER BY ?reaction ?role ?metabolite""",
    "Q2 · Quels métabolites ont un identifiant ChEBI ?": """
SELECT ?nom ?chebi
WHERE { ?m a bio:Metabolite ; rdfs:label ?nom ; bio:hasChEBI ?chebi . }
ORDER BY ?nom""",
    "Q3 · Quelles réactions produisent le pyruvate ?": """
SELECT ?reaction
WHERE { ?r bio:hasProduct <https://example.org/metabolisme/metabolite/pyr_c> ;
           rdfs:label ?reaction . }""",
    "Q4 · Quels métabolites n'ont aucun identifiant MetaNetX valide ?": """
SELECT ?nom
WHERE {
  ?m a bio:Metabolite ; rdfs:label ?nom .
  FILTER NOT EXISTS { ?m bio:hasMetaNetX ?x }
}
ORDER BY ?nom""",
    "Q5 · Combien de réactions touchent chaque métabolite ?": """
SELECT ?nom (COUNT(DISTINCT ?r) AS ?nb_reactions)
WHERE {
  ?m a bio:Metabolite ; rdfs:label ?nom .
  ?r ?lien ?m .
  FILTER (?lien IN (bio:hasReactant, bio:hasProduct))
}
GROUP BY ?nom
ORDER BY DESC(?nb_reactions) ?nom""",
}


def run_query(g, query):
    """Exécute une requête SPARQL (les préfixes sont ajoutés automatiquement) -> DataFrame."""
    res = g.query(PREFIXES + query)
    cols = [str(v) for v in res.vars]
    rows = [[str(v) if v is not None else "" for v in row] for row in res]
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# Lancement en ligne de commande
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "data/modele_demo.xml"
    network_id, mets, reas, pars = parse_sbml(path)
    df_met, df_rea, df_par = to_dataframes(mets, reas, pars)
    print(f"Réseau : {network_id} | {len(df_met)} métabolites, {len(df_rea)} réactions, {len(df_par)} participations")

    errors, warnings, coverage = quality_check(df_met, df_rea, df_par)
    print("Erreurs bloquantes :", errors or "aucune")
    for w in warnings:
        print("  -", w)
    for nom, (n, total) in coverage.items():
        print(f"Couverture {nom} : {n}/{total}")
    if errors:
        raise SystemExit("Données invalides : arrêt du pipeline")

    g = build_rdf(df_met, df_rea, df_par)
    print("Nombre de triplets :", len(g))
    for titre, q in QUERIES.items():
        print("\n" + titre)
        print(run_query(g, q).to_string(index=False))
