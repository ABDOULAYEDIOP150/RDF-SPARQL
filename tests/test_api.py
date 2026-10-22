"""Tests de l'API sur les sorties du job Spark (data/processed)."""

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_networks_et_quarantaine(client):
    statuts = {n["network_id"]: n["status"] for n in client.get("/networks").json()}
    assert statuts["demo_broken"] == "quarantaine"
    assert statuts["iJO1366"] == "validé"


def test_hubs(client):
    top = client.get("/networks/e_coli_core/hubs", params={"top": 1}).json()
    assert top[0]["name"] == "H+"


def test_path(client):
    r = client.get("/networks/e_coli_core/path", params={"source": "M_glc__D_e", "target": "M_succ_e"})
    assert r.status_code == 200
    steps = r.json()["steps"]
    assert steps[0]["identifiant"] == "M_glc__D_e" and steps[-1]["identifiant"] == "M_succ_e"


def test_reseau_inconnu(client):
    assert client.get("/networks/inconnu/hubs").status_code == 404


def test_shared(client):
    shared = client.get("/shared-metabolites", params={"min_networks": 5}).json()
    assert any(s["chebi"] == "CHEBI:15361" for s in shared)   # le pyruvate est partout


def test_sparql(client):
    q = "SELECT ?reseau (COUNT(?r) AS ?n) WHERE { ?r a bio:Reaction ; bio:inNetwork ?reseau } GROUP BY ?reseau"
    rows = client.post("/sparql", json={"query": q}).json()["rows"]
    tailles = {r["reseau"]: int(r["n"]) for r in rows}
    assert tailles["ex:network/iJO1366"] == 2583


def test_sparql_refuse_les_modifications(client):
    assert client.post("/sparql", json={"query": "DELETE WHERE { ?s ?p ?o }"}).status_code == 400
