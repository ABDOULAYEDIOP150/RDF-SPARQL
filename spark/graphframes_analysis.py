"""
Analyses de graphe distribuées avec GraphFrames, sur les sommets / arêtes produits par
pipeline_spark.py (data/processed/graph).

GraphFrames est une bibliothèque JVM : on la charge avec --packages.
    spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.10.0 \
        spark/graphframes_analysis.py
(exécuté automatiquement par la CI GitHub Actions)

1. Degrés entrants / sortants : les métabolites « hubs » de tous les réseaux en un seul calcul.
2. Recherche de motifs : depuis le pyruvate, quels métabolites obtient-on en UNE réaction ?
   Le motif (a)-[]->(r); (r)-[]->(b) se lit comme un mini SPARQL sur le graphe.
3. Composantes connexes : les parties du réseau isolées du reste (souvent des trous
   d'annotation ou des voies incomplètes).
"""

import sys
import tempfile
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent.parent
GRAPH = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "data" / "processed" / "graph"

spark = SparkSession.builder.appName("metabolic-kg-graphframes").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir(tempfile.mkdtemp())

vertices = spark.read.parquet(str(GRAPH / "vertices"))
edges = spark.read.parquet(str(GRAPH / "edges"))
g = GraphFrame(vertices, edges)
print(f"Graphe : {g.vertices.count()} sommets, {g.edges.count()} arêtes")

# 1. Degrés : nombre de réactions qui consomment (outDegree) ou produisent (inDegree) un métabolite
deg = (g.degrees.join(g.vertices, "id")
       .filter(F.col("kind") == "metabolite")
       .orderBy(F.desc("degree")))
print("\n1. Métabolites les plus connectés (tous réseaux) :")
deg.select("network_id", "name", "degree").show(10, truncate=False)

# 2. Motifs : pyruvate -> réaction -> produit, dans E. coli core
motifs = (g.find("(a)-[e1]->(r); (r)-[e2]->(b)")
          .filter(F.col("a.id") == "e_coli_core:M:M_pyr_c")
          .filter(F.col("r.kind") == "reaction")
          .select(F.col("r.name").alias("reaction"), F.col("b.name").alias("produit"))
          .distinct().orderBy("reaction", "produit"))
print("2. Depuis le pyruvate (E. coli core), en une réaction on obtient :")
motifs.show(30, truncate=False)

# 3. Composantes connexes, par réseau
cc = g.connectedComponents()
sizes = (cc.groupBy("network_id", "component").count()
         .groupBy("network_id")
         .agg(F.count("*").alias("nb_composantes"), F.max("count").alias("plus_grande"))
         .orderBy("network_id"))
print("3. Composantes connexes par réseau :")
sizes.show(truncate=False)

spark.stop()
