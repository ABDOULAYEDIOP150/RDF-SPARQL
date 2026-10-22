"""Étapes 4 et 5 : construire le graphe RDF et l'interroger en SPARQL."""

import re

import pandas as pd
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD

from kg.quality import MNX_PATTERN

EX = Namespace("https://example.org/metabolisme/")               # nos ressources
BIO = Namespace("https://example.org/metabolisme/vocabulaire/")  # notre vocabulaire
CHEBI = Namespace("http://purl.obolibrary.org/obo/CHEBI_")       # URI officielles ChEBI
MNX = Namespace("https://identifiers.org/metanetx.chemical/")    # URI MetaNetX

PREFIXES = """PREFIX ex:    <https://example.org/metabolisme/>
PREFIX bio:   <https://example.org/metabolisme/vocabulaire/>
PREFIX chebi: <http://purl.obolibrary.org/obo/CHEBI_>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
"""


def build_rdf(df_met, df_rea, df_par):
    """Graphe RDF d'un seul réseau (version 1 du mini-projet : 29 triplets pour la démo)."""
    g = Graph()
    for prefix, ns in [("ex", EX), ("bio", BIO), ("chebi", CHEBI), ("mnx", MNX)]:
        g.bind(prefix, ns)

    for _, row in df_met.iterrows():
        m = EX[f"metabolite/{row['id']}"]
        g.add((m, RDF.type, BIO.Metabolite))
        g.add((m, RDFS.label, Literal(row["name"], lang="en")))
        g.add((m, BIO.inCompartment, EX[f"compartment/{row['compartment']}"]))
        for c in row["chebi_ids"]:
            g.add((m, BIO.hasChEBI, CHEBI[c.replace("CHEBI:", "")]))
        for x in row["metanetx_ids"]:
            if re.fullmatch(MNX_PATTERN, x):      # on n'écrit jamais un identifiant mal formé
                g.add((m, BIO.hasMetaNetX, MNX[x]))

    for _, row in df_rea.iterrows():
        r = EX[f"reaction/{row['id']}"]
        g.add((r, RDF.type, BIO.Reaction))
        g.add((r, RDFS.label, Literal(row["name"], lang="fr")))
        g.add((r, BIO.isReversible, Literal(bool(row["reversible"]), datatype=XSD.boolean)))

    for _, row in df_par.iterrows():
        r = EX[f"reaction/{row['reaction_id']}"]
        m = EX[f"metabolite/{row['metabolite_id']}"]
        g.add((r, BIO.hasReactant if row["role"] == "reactant" else BIO.hasProduct, m))

    return g


SHORT = [("https://example.org/metabolisme/vocabulaire/", "bio:"),
         ("https://example.org/metabolisme/", "ex:"),
         ("http://purl.obolibrary.org/obo/CHEBI_", "chebi:"),
         ("https://identifiers.org/metanetx.chemical/", "mnx:")]


def shorten(value):
    """Affiche une URI sous sa forme courte (ex:network/iJO1366 au lieu de l'adresse complète)."""
    for long, short in SHORT:
        if value.startswith(long):
            return short + value[len(long):]
    return value


def run_query(g, query, limit=None, short=True):
    """Exécute une requête SPARQL (préfixes ajoutés automatiquement) et renvoie un DataFrame."""
    res = g.query(PREFIXES + query)
    cols = [str(v) for v in res.vars]
    rows = []
    for i, row in enumerate(res):
        if limit is not None and i >= limit:
            break
        values = [str(v) if v is not None else "" for v in row]
        rows.append([shorten(v) for v in values] if short else values)
    return pd.DataFrame(rows, columns=cols)


# Requêtes du mini-projet (un seul réseau, URI ex:metabolite/<id>)
DEMO_QUERIES = {
    "Q1 · Que consomme et que produit chaque réaction ?": """SELECT ?reaction ?role ?metabolite
WHERE {
  ?r a bio:Reaction ; rdfs:label ?reaction .
  { ?r bio:hasReactant ?m . BIND("consomme" AS ?role) }
  UNION
  { ?r bio:hasProduct  ?m . BIND("produit"  AS ?role) }
  ?m rdfs:label ?metabolite .
}
ORDER BY ?reaction ?role ?metabolite""",
    "Q2 · Quels métabolites ont un identifiant ChEBI ?": """SELECT ?nom ?chebi
WHERE { ?m a bio:Metabolite ; rdfs:label ?nom ; bio:hasChEBI ?chebi . }
ORDER BY ?nom""",
    "Q3 · Quelles réactions produisent le pyruvate ?": """SELECT ?reaction
WHERE { ?r bio:hasProduct <https://example.org/metabolisme/metabolite/pyr_c> ;
           rdfs:label ?reaction . }""",
    "Q4 · Quels métabolites n'ont aucun identifiant MetaNetX valide ?": """SELECT ?nom
WHERE {
  ?m a bio:Metabolite ; rdfs:label ?nom .
  FILTER NOT EXISTS { ?m bio:hasMetaNetX ?x }
}
ORDER BY ?nom""",
}

# Requêtes sur le graphe de connaissances fusionné (tous les réseaux, produit par le job Spark)
KG_QUERIES = {
    "Taille de chaque réseau": """SELECT ?reseau (COUNT(?r) AS ?reactions)
WHERE { ?r a bio:Reaction ; bio:inNetwork ?reseau . }
GROUP BY ?reseau
ORDER BY DESC(?reactions)""",
    "Dans quels réseaux trouve-t-on le pyruvate (CHEBI:15361) ?": """SELECT ?reseau (COUNT(DISTINCT ?r) AS ?reactions_qui_le_produisent)
WHERE {
  ?m bio:hasChEBI chebi:15361 ; bio:inNetwork ?reseau .
  ?r bio:hasProduct ?m .
}
GROUP BY ?reseau
ORDER BY ?reseau""",
    "Molécules communes à tous les organismes (via ChEBI)": """SELECT ?chebi (SAMPLE(?nom) AS ?exemple_de_nom) (COUNT(DISTINCT ?reseau) AS ?nb_reseaux)
WHERE {
  ?m bio:hasChEBI ?chebi ; bio:inNetwork ?reseau ; rdfs:label ?nom .
  FILTER (?reseau != <https://example.org/metabolisme/network/demo_glycolysis>)
}
GROUP BY ?chebi
HAVING (COUNT(DISTINCT ?reseau) >= 4)
ORDER BY ?exemple_de_nom
LIMIT 50""",
    "Couverture ChEBI : métabolites sans annotation, par réseau": """SELECT ?reseau (COUNT(?m) AS ?sans_chebi)
WHERE {
  ?m a bio:Metabolite ; bio:inNetwork ?reseau .
  FILTER NOT EXISTS { ?m bio:hasChEBI ?c }
}
GROUP BY ?reseau
ORDER BY DESC(?sans_chebi)""",
    "Réactions réversibles qui consomment de l'ATP (CHEBI:30616), par réseau": """SELECT ?reseau (COUNT(DISTINCT ?r) AS ?nb)
WHERE {
  ?m bio:hasChEBI chebi:30616 ; bio:inNetwork ?reseau .
  ?r bio:hasReactant ?m ; bio:isReversible true .
}
GROUP BY ?reseau
ORDER BY DESC(?nb)""",
}
