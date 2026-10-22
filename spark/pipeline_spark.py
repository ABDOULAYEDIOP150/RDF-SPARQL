"""
Job PySpark : data lake de fichiers SBML -> tables Parquet + graphe de connaissances RDF.

    spark-submit spark/pipeline_spark.py --raw data/raw --out data/processed
    (ou simplement : python spark/pipeline_spark.py)

Étapes
  1. Lecture en parallèle de tous les fichiers SBML du data lake (un fichier = un réseau).
  2. Trois DataFrames Spark : métabolites, réactions, participants.
  3. Contrôle qualité calculé par Spark, réseau par réseau. Un réseau avec une erreur
     bloquante est mis en QUARANTAINE : il est exclu, les autres continuent.
  4. Tables Parquet partitionnées par réseau (format du data lake).
  5. Graphe de connaissances fusionné en N-Triples, généré en parallèle (un triplet par ligne).
  6. Analyses de graphe : sommets / arêtes au format GraphFrames, hubs par réseau,
     molécules partagées entre organismes (via ChEBI).
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, StringType, StructField, StructType  # noqa: E402

from kg.quality import MNX_PATTERN  # noqa: E402
from kg.sbml import parse_file_to_rows  # noqa: E402

EX = "https://example.org/metabolisme/"
BIO = "https://example.org/metabolisme/vocabulaire/"
RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
RDFS_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>"
XSD_BOOL = "<http://www.w3.org/2001/XMLSchema#boolean>"

MET_SCHEMA = StructType([
    StructField("network_id", StringType()), StructField("id", StringType()),
    StructField("name", StringType()), StructField("compartment", StringType()),
    StructField("chebi_ids", ArrayType(StringType())),
    StructField("metanetx_ids", ArrayType(StringType())),
])
REA_SCHEMA = StructType([
    StructField("network_id", StringType()), StructField("id", StringType()),
    StructField("name", StringType()), StructField("reversible", BooleanType()),
])
PAR_SCHEMA = StructType([
    StructField("network_id", StringType()), StructField("reaction_id", StringType()),
    StructField("metabolite_id", StringType()), StructField("role", StringType()),
    StructField("stoichiometry", DoubleType()),
])


# ---------------------------------------------------------------------------
# Étape 1 : lecture parallèle (exécutée sur les workers)
# ---------------------------------------------------------------------------
# La fonction de lecture vit dans le package kg (kg.sbml.parse_file_to_rows) : elle est ainsi
# importable sur chaque worker grâce au zip envoyé par sc.addPyFile.


# ---------------------------------------------------------------------------
# Étape 3 : contrôle qualité en Spark
# ---------------------------------------------------------------------------
def quality(met, rea, par):
    """Renvoie un DataFrame (network_id, level, message) : level = error | warning."""
    def by_net(df, level, message):
        return df.groupBy("network_id").count().select(
            "network_id", F.lit(level).alias("level"),
            F.concat(F.lit(message + " ("), F.col("count").cast("string"), F.lit(")")).alias("message"))

    dup_met = met.groupBy("network_id", "id").count().filter("count > 1")
    dup_rea = rea.groupBy("network_id", "id").count().filter("count > 1")
    m_ids = met.select("network_id", F.col("id").alias("metabolite_id"))
    r_ids = rea.select("network_id", F.col("id").alias("reaction_id"))
    orphan_met = par.join(m_ids, ["network_id", "metabolite_id"], "left_anti")
    orphan_rea = par.join(r_ids, ["network_id", "reaction_id"], "left_anti")
    bad_stoich = par.filter(F.col("stoichiometry") <= 0)

    mnx = met.select("network_id", "id", "compartment", F.explode("metanetx_ids").alias("mnx"))
    valid = F.col("mnx").rlike(f"^({MNX_PATTERN})$")
    malformed = mnx.filter(~valid).select(
        "network_id", F.lit("warning").alias("level"),
        F.concat(F.lit("MetaNetX mal formé pour "), F.col("id"), F.lit(" : '"), F.col("mnx"),
                 F.lit("'")).alias("message"))
    shared = (mnx.filter(valid).groupBy("network_id", "compartment", "mnx")
              .agg(F.sort_array(F.collect_list("id")).alias("ids"))
              .filter(F.size("ids") > 1)
              .select("network_id", F.lit("warning").alias("level"),
                      F.concat(F.col("mnx"), F.lit(" est porté par plusieurs métabolites : "),
                               F.concat_ws(", ", "ids")).alias("message")))

    return (by_net(dup_met, "error", "Identifiants de métabolites en double")
            .unionByName(by_net(dup_rea, "error", "Identifiants de réactions en double"))
            .unionByName(by_net(orphan_met, "error", "Participants vers un métabolite inconnu"))
            .unionByName(by_net(orphan_rea, "error", "Participants vers une réaction inconnue"))
            .unionByName(by_net(bad_stoich, "error", "Stœchiométries nulles ou négatives"))
            .unionByName(malformed).unionByName(shared))


# ---------------------------------------------------------------------------
# Étape 5 : N-Triples générés par des expressions Spark (aucune boucle Python)
# ---------------------------------------------------------------------------
def safe(c):
    """Partie d'URI sûre : tout caractère hors [A-Za-z0-9_.-] devient '_'."""
    return F.regexp_replace(c, r"[^A-Za-z0-9_.\-]", "_")


def iri(*parts):
    return F.concat(F.lit("<" + EX), *parts, F.lit(">"))


def literal(c, lang="en"):
    escaped = F.regexp_replace(F.regexp_replace(c, r"\\", r"\\\\"), '"', r'\\"')
    escaped = F.regexp_replace(escaped, r"[\r\n]+", " ")
    return F.concat(F.lit('"'), escaped, F.lit(f'"@{lang}'))


def triple(s, p, o):
    return F.concat_ws(" ", s, p if not isinstance(p, str) else F.lit(p), o, F.lit("."))


def ntriples(met, rea, par):
    net = F.col("network_id")
    m_iri = iri(safe(net), F.lit("/metabolite/"), safe(F.col("id")))
    r_iri = iri(safe(net), F.lit("/reaction/"), safe(F.col("id")))
    n_iri = iri(F.lit("network/"), safe(net))

    lines = []
    networks = met.select("network_id").distinct()
    lines.append(networks.select(triple(n_iri, RDF_TYPE, F.lit(f"<{BIO}Network>"))))
    lines.append(networks.select(triple(n_iri, RDFS_LABEL, literal(net))))

    lines.append(met.select(triple(m_iri, RDF_TYPE, F.lit(f"<{BIO}Metabolite>"))))
    lines.append(met.select(triple(m_iri, RDFS_LABEL, literal(F.col("name")))))
    lines.append(met.select(triple(m_iri, f"<{BIO}inNetwork>", n_iri)))
    lines.append(met.select(triple(m_iri, f"<{BIO}inCompartment>",
                                   iri(safe(net), F.lit("/compartment/"), safe(F.col("compartment"))))))
    chebi = (met.select("network_id", "id", F.explode("chebi_ids").alias("c"))
             .withColumn("num", F.regexp_extract("c", r"(\d+)$", 1)).filter("num != ''"))
    lines.append(chebi.select(triple(m_iri, f"<{BIO}hasChEBI>",
                                     F.concat(F.lit("<http://purl.obolibrary.org/obo/CHEBI_"),
                                              F.col("num"), F.lit(">")))))
    mnx = (met.select("network_id", "id", F.explode("metanetx_ids").alias("x"))
           .filter(F.col("x").rlike(f"^({MNX_PATTERN})$")))      # jamais d'identifiant mal formé
    lines.append(mnx.select(triple(m_iri, f"<{BIO}hasMetaNetX>",
                                   F.concat(F.lit("<https://identifiers.org/metanetx.chemical/"),
                                            F.col("x"), F.lit(">")))))

    lines.append(rea.select(triple(r_iri, RDF_TYPE, F.lit(f"<{BIO}Reaction>"))))
    lines.append(rea.select(triple(r_iri, RDFS_LABEL, literal(F.col("name")))))
    lines.append(rea.select(triple(r_iri, f"<{BIO}inNetwork>", n_iri)))
    lines.append(rea.select(triple(
        r_iri, f"<{BIO}isReversible>",
        F.concat(F.lit('"'), F.lower(F.col("reversible").cast("string")), F.lit(f'"^^{XSD_BOOL}')))))

    links = par.select(
        "network_id", F.col("reaction_id").alias("id"),
        F.when(F.col("role") == "reactant", F.lit(f"<{BIO}hasReactant>"))
         .otherwise(F.lit(f"<{BIO}hasProduct>")).alias("pred"),
        iri(safe(net), F.lit("/metabolite/"), safe(F.col("metabolite_id"))).alias("obj"))
    lines.append(links.select(triple(r_iri, F.col("pred"), F.col("obj"))))

    out = lines[0]
    for df in lines[1:]:
        out = out.unionAll(df)
    return out.toDF("value").distinct()


# ---------------------------------------------------------------------------
# Étape 6 : analyses de graphe
# ---------------------------------------------------------------------------
def graph_tables(met, rea, par):
    """Sommets et arêtes au format attendu par GraphFrames (colonnes id / src, dst)."""
    v_met = met.select(F.concat_ws(":", "network_id", F.lit("M"), "id").alias("id"),
                       "network_id", F.lit("metabolite").alias("kind"), "name")
    v_rea = rea.select(F.concat_ws(":", "network_id", F.lit("R"), "id").alias("id"),
                       "network_id", F.lit("reaction").alias("kind"), "name")
    m_node = F.concat_ws(":", "network_id", F.lit("M"), "metabolite_id")
    r_node = F.concat_ws(":", "network_id", F.lit("R"), "reaction_id")
    edges = par.select(
        F.when(F.col("role") == "reactant", m_node).otherwise(r_node).alias("src"),
        F.when(F.col("role") == "reactant", r_node).otherwise(m_node).alias("dst"),
        "network_id", F.col("role").alias("relationship"), "stoichiometry")
    return v_met.unionByName(v_rea), edges


def top_hubs(met, par, top=20):
    """Les N métabolites les plus connectés de chaque réseau (fonction de fenêtre)."""
    deg = (par.groupBy("network_id", "metabolite_id")
           .agg(F.countDistinct("reaction_id").alias("nb_reactions")))
    deg = deg.join(met.select("network_id", F.col("id").alias("metabolite_id"), "name", "compartment"),
                   ["network_id", "metabolite_id"])
    w = Window.partitionBy("network_id").orderBy(F.desc("nb_reactions"), "metabolite_id")
    return deg.withColumn("rang", F.row_number().over(w)).filter(F.col("rang") <= top)


def shared_chebi(met):
    """Molécules (ChEBI) présentes dans plusieurs réseaux : le lien entre les organismes."""
    ex = met.select("network_id", "name", F.explode("chebi_ids").alias("chebi"))
    return (ex.groupBy("chebi")
            .agg(F.countDistinct("network_id").alias("nb_networks"),
                 F.sort_array(F.collect_set("network_id")).alias("networks"),
                 F.min("name").alias("example_name"))
            .filter("nb_networks >= 2"))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def write_parquet(df, path, partition=True, sort_cols=()):
    df = df.repartition("network_id") if partition and "network_id" in df.columns else df.coalesce(1)
    if sort_cols:
        df = df.sortWithinPartitions(*sort_cols)
    writer = df.write.mode("overwrite")
    if partition and "network_id" in df.columns:
        writer = writer.partitionBy("network_id")
    writer.parquet(str(path))


def run(raw, out, spark=None):
    raw, out = Path(raw).resolve(), Path(out).resolve()
    own_spark = spark is None
    if own_spark:
        spark = (SparkSession.builder.appName("metabolic-kg")
                 .master(os.environ.get("SPARK_MASTER", "local[*]"))
                 .config("spark.sql.shuffle.partitions", "8")
                 .config("spark.ui.enabled", "false")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # Le package kg est envoyé aux workers (indispensable sur un vrai cluster)
    zip_path = Path(tempfile.mkdtemp()) / "kg.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        for f in (ROOT / "kg").glob("*.py"):
            z.write(f, f"kg/{f.name}")
    sc.addPyFile(str(zip_path))

    # 1. Lecture parallèle : un fichier = une tâche
    files = sc.binaryFiles(str(raw / "*"), minPartitions=8)
    parsed = files.flatMap(parse_file_to_rows).cache()
    unreadable = parsed.filter(lambda t: t[1]).map(lambda t: (t[0], t[1])).collect()

    # 2. DataFrames Spark
    met = spark.createDataFrame(parsed.flatMap(lambda t: t[2]), MET_SCHEMA).cache()
    rea = spark.createDataFrame(parsed.flatMap(lambda t: t[3]), REA_SCHEMA).cache()
    par = spark.createDataFrame(parsed.flatMap(lambda t: t[4]), PAR_SCHEMA).cache()

    # 3. Qualité et quarantaine
    issues = quality(met, rea, par).cache()
    quarantined = {r.network_id for r in issues.filter("level = 'error'").select("network_id")
                   .distinct().collect()}
    stats = {r.network_id: r.asDict() for r in met.groupBy("network_id").agg(
        F.count("*").alias("metabolites"),
        F.sum(F.when(F.size("chebi_ids") > 0, 1).otherwise(0)).alias("with_chebi"),
        F.sum(F.when(F.size("metanetx_ids") > 0, 1).otherwise(0)).alias("with_metanetx")).collect()}
    n_rea = dict(rea.groupBy("network_id").count().collect())
    n_par = dict(par.groupBy("network_id").count().collect())
    all_issues = issues.orderBy("network_id", "level", "message").collect()

    report = {}
    for nid in sorted(set(stats) | {n for n, _ in unreadable}):
        net_issues = [r for r in all_issues if r.network_id == nid]
        s = stats.get(nid, {})
        report[nid] = {
            "status": "illisible" if nid in dict(unreadable)
            else ("quarantaine" if nid in quarantined else "validé"),
            "metabolites": int(s.get("metabolites", 0)),
            "reactions": int(n_rea.get(nid, 0)),
            "participants": int(n_par.get(nid, 0)),
            "with_chebi": int(s.get("with_chebi") or 0),
            "with_metanetx": int(s.get("with_metanetx") or 0),
            "errors": [r.message for r in net_issues if r.level == "error"]
                      + ([dict(unreadable)[nid]] if nid in dict(unreadable) else []),
            "warnings": [r.message for r in net_issues if r.level == "warning"],
        }

    keep = F.col("network_id").isin(list(quarantined)) == F.lit(False)
    met_ok, rea_ok, par_ok = met.filter(keep), rea.filter(keep), par.filter(keep)

    # 4. Tables Parquet du data lake
    if out.exists():
        shutil.rmtree(out)
    write_parquet(met_ok, out / "tables" / "metabolites", sort_cols=["id"])
    write_parquet(rea_ok, out / "tables" / "reactions", sort_cols=["id"])
    write_parquet(par_ok, out / "tables" / "participants", sort_cols=["reaction_id", "metabolite_id"])

    # 5. Graphe de connaissances N-Triples (un seul fichier compressé pour l'app et l'API)
    nt = ntriples(met_ok, rea_ok, par_ok)
    tmp_nt = out / "rdf" / "_tmp"
    nt.orderBy("value").coalesce(1).write.mode("overwrite").text(str(tmp_nt), compression="gzip")
    part = next(tmp_nt.glob("part-*"))
    shutil.move(str(part), out / "rdf" / "knowledge_graph.nt.gz")
    shutil.rmtree(tmp_nt)
    n_triples = nt.count()

    # 6. Analyses de graphe
    vertices, edges = graph_tables(met_ok, rea_ok, par_ok)
    write_parquet(vertices, out / "graph" / "vertices", sort_cols=["id"])
    write_parquet(edges, out / "graph" / "edges", sort_cols=["src", "dst"])
    write_parquet(top_hubs(met_ok, par_ok), out / "graph" / "top_hubs", partition=False,
                  sort_cols=["network_id", "rang"])
    write_parquet(shared_chebi(met_ok).orderBy(F.desc("nb_networks"), "chebi"),
                  out / "graph" / "shared_chebi", partition=False)

    summary = {"networks": report, "triples": n_triples,
               "quarantined": sorted(quarantined), "unreadable": [n for n, _ in unreadable]}
    (out / "quality").mkdir(parents=True, exist_ok=True)
    (out / "quality" / "report.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2),
                                                  encoding="utf-8")
    for crc in out.rglob("*.crc"):          # fichiers techniques de Hadoop, inutiles ici
        crc.unlink()
    for ok in out.rglob("_SUCCESS"):
        ok.unlink()

    if own_spark:
        spark.stop()
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--raw", default=str(ROOT / "data" / "raw"))
    parser.add_argument("--out", default=str(ROOT / "data" / "processed"))
    args = parser.parse_args()
    summary = run(args.raw, args.out)
    print(f"\n{'Réseau':18s} {'Statut':12s} {'Métab.':>7s} {'Réac.':>7s} {'Avert.':>7s}")
    for nid, r in summary["networks"].items():
        print(f"{nid:18s} {r['status']:12s} {r['metabolites']:7d} {r['reactions']:7d} "
              f"{len(r['warnings']):7d}")
        for e in r["errors"]:
            print(f"{'':18s} ERREUR : {e}")
    print(f"\nGraphe de connaissances : {summary['triples']} triplets -> {args.out}/rdf/knowledge_graph.nt.gz")


if __name__ == "__main__":
    main()
