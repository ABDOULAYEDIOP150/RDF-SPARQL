"""
API REST qui expose le graphe de connaissances à des applications externes.

    uvicorn api.main:app --reload        ->  documentation interactive sur /docs
"""

import json
from functools import lru_cache

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from kg import store
from kg.graph import build_graph, currency_metabolites, hubs, shortest_path
from kg.rdf import KG_QUERIES, run_query

app = FastAPI(
    title="Metabolic KG API",
    version="1.0.0",
    description="Réseaux métaboliques (SBML) convertis en graphe de connaissances RDF, "
                "reliés à ChEBI et MetaNetX : statistiques, hubs, chemins métaboliques et SPARQL.",
)


def records(df):
    """DataFrame -> liste de dictionnaires JSON (convertit aussi les types numpy)."""
    return json.loads(df.to_json(orient="records", force_ascii=False))


@lru_cache
def tables():
    return store.load_tables()


@lru_cache
def kg():
    return store.load_kg()


@lru_cache
def network_graph(network_id):
    met, rea, par = store.load_network(network_id, tables())
    return build_graph(met, rea, par), met, par


def check_network(network_id):
    if network_id not in set(tables()[0]["network_id"]):
        raise HTTPException(404, f"Réseau inconnu : {network_id}")


@app.get("/health", tags=["Service"])
def health():
    return {"status": "ok"}


@app.get("/networks", tags=["Réseaux"])
def list_networks():
    """Tous les réseaux du data lake, avec leur statut qualité."""
    report = store.load_quality()["networks"]
    return [{"network_id": nid, **{k: v for k, v in r.items() if k != "warnings"},
             "nb_warnings": len(r["warnings"])} for nid, r in report.items()]


@app.get("/networks/{network_id}/metabolites", tags=["Réseaux"])
def metabolites(network_id: str, search: str = "", limit: int = Query(50, le=500)):
    """Métabolites d'un réseau, filtrables par nom ou identifiant."""
    check_network(network_id)
    met = tables()[0]
    met = met[met["network_id"] == network_id]
    if search:
        mask = (met["name"].str.contains(search, case=False, regex=False)
                | met["id"].str.contains(search, case=False, regex=False))
        met = met[mask]
    return records(met.head(limit))


@app.get("/networks/{network_id}/hubs", tags=["Graphe"])
def network_hubs(network_id: str, top: int = Query(10, ge=1, le=100)):
    """Métabolites les plus connectés (degré = nombre de réactions)."""
    check_network(network_id)
    _, met, par = network_graph(network_id)
    return records(hubs(met, par, top))


@app.get("/networks/{network_id}/path", tags=["Graphe"])
def path(network_id: str, source: str, target: str, exclude_currency: bool = True):
    """Plus court chemin métabolique entre deux métabolites (identifiants SBML)."""
    check_network(network_id)
    g, met, _ = network_graph(network_id)
    excluded = currency_metabolites(met) if exclude_currency else set()
    res = shortest_path(g, source, target, excluded)
    if res is None:
        raise HTTPException(404, "Aucun chemin trouvé (ou identifiant inconnu)")
    return {"length": int((len(res) - 1) / 2), "steps": records(res)}


@app.get("/shared-metabolites", tags=["Graphe de connaissances"])
def shared(min_networks: int = Query(2, ge=2), limit: int = Query(50, le=1000)):
    """Molécules (identifiant ChEBI) présentes dans plusieurs organismes."""
    df = store.load_shared()
    df = df[df["nb_networks"] >= min_networks].head(limit).copy()
    df["networks"] = df["networks"].apply(list)
    return records(df)


class SparqlRequest(BaseModel):
    query: str = Field(..., examples=[KG_QUERIES["Taille de chaque réseau"]])
    limit: int = Field(200, le=5000)


@app.get("/sparql/examples", tags=["Graphe de connaissances"])
def sparql_examples():
    """Requêtes SPARQL prêtes à l'emploi."""
    return KG_QUERIES


@app.post("/sparql", tags=["Graphe de connaissances"])
def sparql(req: SparqlRequest):
    """Exécute une requête SPARQL SELECT (préfixes ex:, bio:, chebi:, rdfs: déjà déclarés)."""
    if not req.query.lstrip().upper().startswith(("SELECT", "PREFIX")):
        raise HTTPException(400, "Seules les requêtes SELECT sont acceptées")
    try:
        df = run_query(kg(), req.query, limit=req.limit)
    except Exception as exc:
        raise HTTPException(400, f"Requête invalide : {exc}") from exc
    return {"columns": list(df.columns), "rows": records(df)}
