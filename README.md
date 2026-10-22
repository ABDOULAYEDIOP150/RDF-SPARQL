# 🧬 Metabolic KG Explorer

[![CI/CD](https://github.com/ABDOULAYEDIOP150/RDF-SPARQL/actions/workflows/ci.yml/badge.svg)](https://github.com/ABDOULAYEDIOP150/RDF-SPARQL/actions/workflows/ci.yml)
[![Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://rdf-sparql-iuayqfdpcceqkxwfd88rnm.streamlit.app/)

**Une collection de réseaux métaboliques (SBML) transformée en graphe de connaissances RDF,
interrogeable en SPARQL, analysée par la théorie des graphes et exposée par une API.**

👉 **Application en ligne : https://rdf-sparql-iuayqfdpcceqkxwfd88rnm.streamlit.app/**

```
 data/raw (data lake SBML)
        │  PySpark : lecture parallèle, contrôle qualité, quarantaine
        ▼
 data/processed
   ├── tables/       Parquet partitionné par réseau (métabolites, réactions, participants)
   ├── rdf/          graphe de connaissances fusionné, N-Triples (135 000 triplets)
   ├── graph/        sommets / arêtes au format GraphFrames, hubs, molécules partagées
   └── quality/      rapport qualité par réseau
        │
        ├──► API FastAPI   (/sparql, /path, /hubs, /shared-metabolites…)  → image Docker
        └──► App Streamlit (exploration, SPARQL, chemins métaboliques)
```

## Les données

| Réseau | Organisme | Métabolites | Réactions | Source |
|---|---|---:|---:|---|
| `e_coli_core` | *E. coli* (cœur du métabolisme) | 72 | 95 | BiGG (via cobrapy) |
| `iJO1366` | *E. coli* K-12 | 1 805 | 2 583 | BiGG (via cobrapy) |
| `iYS1720` | *Salmonella* Typhimurium | 2 436 | 3 357 | BiGG (via cobrapy) |
| `yeast_GEM` | *S. cerevisiae* (levure) | 2 748 | 4 105 | [yeast-GEM](https://github.com/SysBioChalmers/yeast-GEM) |
| `demo_glycolysis` | exemple pédagogique, 2 pièges volontaires | 4 | 2 | — |
| `demo_broken` | réseau cassé : doit partir en quarantaine | 2 | 2 | — |

## Ce que fait chaque partie

| Fichier | Rôle |
|---|---|
| `kg/sbml.py` | Lecture d'un SBML (même compressé) → 3 tableaux : métabolites, réactions, participants |
| `kg/quality.py` | Erreurs bloquantes (doublons, liens cassés, stœchiométrie) et avertissements (identifiants mal formés ou partagés) |
| `kg/rdf.py` | Triplets RDF reliés aux URI ChEBI et MetaNetX, requêtes SPARQL |
| `kg/graph.py` | Graphe biparti NetworkX : hubs, plus court chemin, exclusion des métabolites « monnaie » via ChEBI |
| `spark/pipeline_spark.py` | Job PySpark sur tout le data lake : qualité calculée par Spark, quarantaine, Parquet, N-Triples générés en parallèle, fonctions de fenêtre |
| `spark/graphframes_analysis.py` | GraphFrames : degrés, recherche de motifs, composantes connexes |
| `api/main.py` | API REST FastAPI (documentation sur `/docs`) |
| `app.py` | Application Streamlit |
| `.github/workflows/ci.yml` | CI/CD : lint, tests, job Spark, GraphFrames, image Docker publiée sur ghcr.io |

## Lancer le projet

```bash
pip install -r requirements-dev.txt          # Java 17+ requis pour Spark

python spark/pipeline_spark.py               # 1. job Spark : data/raw -> data/processed
streamlit run app.py                         # 2. application
uvicorn api.main:app --reload                # 3. API -> http://localhost:8000/docs
pytest                                       # tests
```

API en Docker :

```bash
docker run -p 8000:8000 ghcr.io/abdoulayediop150/rdf-sparql-api:latest
curl "http://localhost:8000/networks/e_coli_core/path?source=M_glc__D_e&target=M_succ_e"
```

## CI/CD

À chaque push, GitHub Actions :
1. vérifie le code (`ruff`) et lance les tests (`pytest`, y compris le job Spark sur un mini data lake) ;
2. exécute le job Spark complet et **vérifie que les données publiées sont à jour**, puis lance l'analyse GraphFrames ;
3. construit l'image Docker de l'API, la teste, puis la publie sur GitHub Container Registry (branche `main`).

Streamlit Community Cloud redéploie l'application automatiquement à chaque push sur `main`.

## Exemples de résultats

- **Qualité** : dans *iJO1366*, le D-xylulose et le L-xylulose portent le même identifiant MetaNetX
  alors que ce sont deux stéréoisomères différents ; le réseau cassé est mis en quarantaine sans bloquer les autres.
- **Graphe fusionné** : grâce à ChEBI, la requête « molécules communes à tous les organismes » relie
  des modèles qui n'utilisent pas les mêmes identifiants (`M_pyr_c` chez *E. coli*, `s_1399` chez la levure).
- **Théorie des graphes** : sans précaution, le plus court chemin glucose → succinate passe par H⁺ ;
  en excluant les métabolites « monnaie » repérés par ChEBI, on retrouve un vrai chemin métabolique.

## Pistes pour aller plus loin

- Charger les N-Triples dans un triplestore (Apache Jena Fuseki) et interroger le point d'accès SPARQL en HTTP.
- Ajouter une ontologie (SBO, GO) pour typer les réactions, et la stœchiométrie dans le graphe (nœud de participation).
- Réécrire le job en Scala pour les traitements les plus lourds.

---
Abdoulaye Diop · Data Engineer
