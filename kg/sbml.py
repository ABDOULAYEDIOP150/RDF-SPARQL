"""Étapes 1 et 2 : lire un fichier SBML et ranger son contenu en trois tableaux."""

import gzip
import io
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDF_RESOURCE = "{" + RDF_NS + "}resource"

MET_COLS = ["network_id", "id", "name", "compartment", "chebi_ids", "metanetx_ids"]
REA_COLS = ["network_id", "id", "name", "reversible"]
PAR_COLS = ["network_id", "reaction_id", "metabolite_id", "role", "stoichiometry"]


def _to_bytes(source):
    """Accepte un chemin, des octets ou un objet fichier ; décompresse le .gz si besoin."""
    if isinstance(source, (str, Path)):
        data = Path(source).read_bytes()
    elif isinstance(source, (bytes, bytearray)):
        data = bytes(source)
    else:
        data = source.read()
    if data[:2] == b"\x1f\x8b":          # signature gzip
        data = gzip.decompress(data)
    return data


def parse_sbml(source, network_id=None):
    """Lit un fichier SBML et renvoie (network_id, métabolites, réactions, participants).

    Chaque élément est une liste de dictionnaires : un dictionnaire = une future ligne de tableau.
    `network_id` force l'identifiant du réseau (le job Spark utilise le nom du fichier).
    Fonction en pur Python : elle est réutilisée telle quelle par le job Spark.
    """
    root = ET.parse(io.BytesIO(_to_bytes(source))).getroot()
    sbml_ns = root.tag.split("}")[0].strip("{")       # dépend du niveau/version SBML
    ns = {"sbml": sbml_ns, "rdf": RDF_NS}

    model = root.find("sbml:model", ns)
    if model is None:
        raise ValueError("Aucune balise <model> : ce n'est pas un fichier SBML valide.")
    network_id = network_id or model.get("id") or "reseau"

    metabolites = []
    for sp in model.findall(".//sbml:species", ns):
        chebi, mnx = [], []
        for li in sp.findall(".//rdf:li", ns):
            uri = li.get(RDF_RESOURCE, "")
            if "identifiers.org/chebi/" in uri:
                chebi.append(uri.rsplit("/", 1)[-1])     # "CHEBI:17234"
            elif "identifiers.org/metanetx.chemical/" in uri:
                mnx.append(uri.rsplit("/", 1)[-1])       # "MNXM3"
        metabolites.append({
            "network_id": network_id,
            "id": sp.get("id"),
            "name": sp.get("name") or sp.get("id"),
            "compartment": sp.get("compartment") or "",
            "chebi_ids": sorted(set(chebi)),
            "metanetx_ids": sorted(set(mnx)),
        })

    reactions, participants = [], []
    for r in model.findall(".//sbml:reaction", ns):
        reactions.append({
            "network_id": network_id,
            "id": r.get("id"),
            "name": r.get("name") or r.get("id"),
            "reversible": r.get("reversible") == "true",
        })
        for role, liste in [("reactant", "listOfReactants"), ("product", "listOfProducts")]:
            for ref in r.findall(f"sbml:{liste}/sbml:speciesReference", ns):
                participants.append({
                    "network_id": network_id,
                    "reaction_id": r.get("id"),
                    "metabolite_id": ref.get("species"),
                    "role": role,
                    "stoichiometry": float(ref.get("stoichiometry", 1)),
                })

    return network_id, metabolites, reactions, participants


def to_dataframes(metabolites, reactions, participants):
    """Étape 2 : trois tableaux, comme trois tables d'une base relationnelle."""
    return (pd.DataFrame(metabolites, columns=MET_COLS),
            pd.DataFrame(reactions, columns=REA_COLS),
            pd.DataFrame(participants, columns=PAR_COLS))


def parse_file_to_rows(path_and_bytes):
    """Version « Spark » : (chemin, octets) -> [(réseau, erreur, métabolites, réactions, participants)].

    Les dictionnaires deviennent des tuples dans l'ordre des colonnes, prêts pour createDataFrame.
    Le réseau est identifié par le nom du fichier. Un fichier illisible est signalé, pas ignoré.
    """
    path, content = path_and_bytes
    network_id = Path(path).name.split(".")[0]
    try:
        _, mets, reas, pars = parse_sbml(content, network_id)
    except Exception as exc:
        return [(network_id, f"illisible : {exc}", [], [], [])]

    def rows(dicts, cols):
        return [tuple(d[c] for c in cols) for d in dicts]

    return [(network_id, "", rows(mets, MET_COLS), rows(reas, REA_COLS), rows(pars, PAR_COLS))]
