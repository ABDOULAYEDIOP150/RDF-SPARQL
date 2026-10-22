"""Accès aux sorties du job Spark (data/processed), partagé par l'app Streamlit et l'API."""

import gzip
import json
from pathlib import Path

import pandas as pd
from rdflib import Graph

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
RAW = ROOT / "data" / "raw"


def _read(name, processed=PROCESSED):
    df = pd.read_parquet(processed / name)
    if "network_id" in df.columns:                  # colonne de partition -> texte simple
        df["network_id"] = df["network_id"].astype(str)
    return df


def load_tables(processed=PROCESSED):
    """Les trois tables de tous les réseaux validés (Parquet partitionné par réseau)."""
    met = _read("tables/metabolites", processed)
    met["chebi_ids"] = met["chebi_ids"].apply(list)
    met["metanetx_ids"] = met["metanetx_ids"].apply(list)
    return met, _read("tables/reactions", processed), _read("tables/participants", processed)


def load_network(network_id, tables):
    met, rea, par = tables
    return (met[met["network_id"] == network_id].reset_index(drop=True),
            rea[rea["network_id"] == network_id].reset_index(drop=True),
            par[par["network_id"] == network_id].reset_index(drop=True))


def load_quality(processed=PROCESSED):
    return json.loads((processed / "quality" / "report.json").read_text(encoding="utf-8"))


def load_shared(processed=PROCESSED):
    return _read("graph/shared_chebi", processed)


def load_top_hubs(processed=PROCESSED):
    return _read("graph/top_hubs", processed)


def load_kg(processed=PROCESSED):
    """Le graphe de connaissances fusionné (N-Triples compressé) chargé dans rdflib."""
    g = Graph()
    with gzip.open(processed / "rdf" / "knowledge_graph.nt.gz", "rb") as f:
        g.parse(f, format="nt")
    return g
