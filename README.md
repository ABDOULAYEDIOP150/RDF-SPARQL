# Metabolic KG Explorer 🧬

**Du fichier SBML au graphe de connaissances RDF, interrogé en SPARQL.**

Une application Streamlit qui fait traverser à un réseau métabolique toute la chaîne d'un pipeline de graphe de connaissances :

| Étape | Ce qui est fait | Outils |
|---|---|---|
| 1 · Lecture | Extraction des métabolites, réactions et identifiants officiels depuis le XML SBML | `xml.etree` |
| 2 · Tableaux | Modèle relationnel en 3 tables : métabolites, réactions, participants | `pandas` |
| 3 · Qualité | Erreurs bloquantes (doublons, liens cassés, stœchiométrie) et avertissements (identifiants mal formés ou partagés), couverture des annotations | `pandas` |
| 4 · RDF | Triplets reliés aux URI officielles **ChEBI** et **MetaNetX**, export Turtle / RDF-XML / N-Triples | `rdflib` |
| 5 · SPARQL | Requêtes prêtes à l'emploi (UNION, FILTER NOT EXISTS, GROUP BY) et éditeur libre | `rdflib` |
| 6 · Visualisation | Le réseau dessiné comme un graphe métabolites ↔ réactions | Graphviz |

Le fichier de démonstration (glycolyse ultra-simplifiée : 4 métabolites, 2 réactions) contient **deux pièges volontaires** que le contrôle qualité détecte seul : un identifiant MetaNetX mal formé et un identifiant partagé par deux molécules. On peut aussi importer son propre fichier SBML (modèles BiGG, BioModels…).

## Lancer en local

```bash
pip install -r requirements.txt
streamlit run app.py          # l'application
python pipeline.py            # le même pipeline en ligne de commande
```

## Structure

```
app.py               interface Streamlit
pipeline.py          les étapes du pipeline (fonctions réutilisables)
data/modele_demo.xml réseau SBML de démonstration
```

## Pour aller plus loin

- Passage à l'échelle : traiter des milliers de réseaux d'un data lake en parallèle (Spark), écrire en N-Triples et charger dans un triplestore (Apache Jena Fuseki) interrogeable par HTTP.
- Exposer les requêtes via une API (FastAPI) pour des applications externes.
- Fusionner plusieurs réseaux dans un même graphe grâce aux identifiants ChEBI / MetaNetX communs.

---
Abdoulaye Diop · Data Engineer
