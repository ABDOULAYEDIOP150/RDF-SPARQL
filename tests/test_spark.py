"""Test du job PySpark sur un mini data lake (ignoré si PySpark ou Java est absent)."""

import shutil

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java est requis pour Spark", allow_module_level=True)

from kg import store  # noqa: E402


@pytest.fixture(scope="module")
def summary(tmp_path_factory):
    import sys
    sys.path.insert(0, str(store.ROOT / "spark"))
    from pipeline_spark import run

    raw = tmp_path_factory.mktemp("raw")
    for name in ["demo_glycolysis.xml", "demo_broken.xml", "e_coli_core.xml.gz"]:
        shutil.copy(store.RAW / name, raw / name)
    out = tmp_path_factory.mktemp("processed")
    return run(raw, out), out


def test_quarantaine(summary):
    report, _ = summary
    assert report["quarantined"] == ["demo_broken"]
    assert report["networks"]["e_coli_core"]["status"] == "validé"


def test_avertissements_identiques_a_pandas(summary):
    report, _ = summary
    warnings = report["networks"]["demo_glycolysis"]["warnings"]
    assert len(warnings) == 2
    assert any("MNXM under_review" in w for w in warnings)


def test_sorties(summary):
    report, out = summary
    met, rea, par = store.load_tables(out)
    assert set(met["network_id"]) == {"demo_glycolysis", "e_coli_core"}
    assert len(rea) == 2 + 95
    g = store.load_kg(out)
    assert len(g) == report["triples"] > 0
