"""Tests de la bibliothèque kg sur le réseau de démonstration (résultats du mini-projet)."""

import pytest

from kg import DEMO_QUERIES, build_rdf, parse_sbml, quality_check, run_query, to_dataframes
from kg.graph import build_graph, currency_metabolites, hubs, shortest_path
from kg.store import RAW


@pytest.fixture(scope="module")
def demo():
    _, mets, reas, pars = parse_sbml(RAW / "demo_glycolysis.xml")
    return to_dataframes(mets, reas, pars)


def test_lecture(demo):
    met, rea, par = demo
    assert (len(met), len(rea), len(par)) == (4, 2, 5)
    glc = met.set_index("id").loc["glc_c"]
    assert glc["chebi_ids"] == ["CHEBI:17234"]
    assert rea.set_index("id").loc["R_LDH", "reversible"]


def test_lecture_gzip():
    network_id, mets, reas, _ = parse_sbml(RAW / "e_coli_core.xml.gz", "e_coli_core")
    assert network_id == "e_coli_core"
    assert (len(mets), len(reas)) == (72, 95)


def test_qualite_detecte_les_deux_pieges(demo):
    errors, warnings, coverage = quality_check(*demo)
    assert errors == []
    assert any("MNXM under_review" in w for w in warnings)
    assert any("MNXM3" in w and "glc_c" in w and "atp_c" in w for w in warnings)
    assert coverage == {"ChEBI": (4, 4), "MetaNetX": (3, 4)}


def test_qualite_reseau_casse():
    _, mets, reas, pars = parse_sbml(RAW / "demo_broken.xml")
    errors, _, _ = quality_check(*to_dataframes(mets, reas, pars))
    assert any("métabolite inconnu" in e for e in errors)
    assert any("Stœchiométrie" in e for e in errors)


def test_rdf_29_triplets(demo):
    g = build_rdf(*demo)
    assert len(g) == 29


def test_sparql(demo):
    g = build_rdf(*demo)
    q3 = run_query(g, DEMO_QUERIES["Q3 · Quelles réactions produisent le pyruvate ?"])
    assert q3["reaction"].tolist() == ["Glucose vers pyruvate (simplifié)"]
    q4 = run_query(g, DEMO_QUERIES["Q4 · Quels métabolites n'ont aucun identifiant MetaNetX valide ?"])
    assert q4["nom"].tolist() == ["Lactate", "Pyruvate"]


def test_graphe(demo):
    met, rea, par = demo
    g = build_graph(met, rea, par)
    assert g.number_of_nodes() == 6
    assert hubs(met, par, 1)["metabolite_id"].tolist() == ["pyr_c"]
    path = shortest_path(g, "glc_c", "lac_c")
    assert path["identifiant"].tolist() == ["glc_c", "R_HEX1", "pyr_c", "R_LDH", "lac_c"]


def test_metabolites_monnaie_exclus():
    _, mets, reas, pars = parse_sbml(RAW / "e_coli_core.xml.gz")
    met, rea, par = to_dataframes(mets, reas, pars)
    currency = currency_metabolites(met)
    assert {"M_h_c", "M_h2o_c", "M_atp_c"} <= currency
    path = shortest_path(build_graph(met, rea, par), "M_glc__D_e", "M_succ_e", currency)
    assert not set(path["identifiant"]) & currency
