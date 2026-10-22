"""Théorie des graphes sur un réseau métabolique (NetworkX).

Le réseau est un graphe biparti orienté : métabolite -> réaction -> métabolite.
Une réaction réversible ajoute aussi les arcs dans l'autre sens.
"""

import networkx as nx
import pandas as pd


def build_graph(df_met, df_rea, df_par):
    g = nx.DiGraph()
    names = dict(zip(df_met["id"], df_met["name"]))
    for mid, name in names.items():
        g.add_node(f"M:{mid}", kind="metabolite", label=name, id=mid)
    rev = dict(zip(df_rea["id"], df_rea["reversible"]))
    for rid, name in zip(df_rea["id"], df_rea["name"]):
        g.add_node(f"R:{rid}", kind="reaction", label=name, id=rid)
    for row in df_par.itertuples(index=False):
        m, r = f"M:{row.metabolite_id}", f"R:{row.reaction_id}"
        a, b = (m, r) if row.role == "reactant" else (r, m)
        g.add_edge(a, b)
        if rev.get(row.reaction_id):
            g.add_edge(b, a)
    return g


def hubs(df_met, df_par, top=15):
    """Métabolites qui participent au plus grand nombre de réactions (degré dans le graphe)."""
    deg = (df_par.groupby("metabolite_id")["reaction_id"].nunique()
           .rename("nb_reactions").reset_index())
    deg = deg.merge(df_met[["id", "name", "compartment"]], left_on="metabolite_id", right_on="id")
    return (deg.sort_values(["nb_reactions", "metabolite_id"], ascending=[False, True])
            .head(top)[["metabolite_id", "name", "compartment", "nb_reactions"]]
            .reset_index(drop=True))


# Métabolites « monnaie » (cofacteurs) repérés par leur identifiant ChEBI, donc valables
# quel que soit le nom donné dans chaque modèle : H+, H2O, ATP, ADP, AMP, NAD(P)(H), phosphate,
# pyrophosphate, CO2, coenzyme A, ammonium, O2, H2O2.
CURRENCY_CHEBI = {
    "CHEBI:15378", "CHEBI:24636", "CHEBI:15377", "CHEBI:30616", "CHEBI:15422", "CHEBI:456216",
    "CHEBI:16761", "CHEBI:456215", "CHEBI:16027", "CHEBI:57540", "CHEBI:15846", "CHEBI:57945",
    "CHEBI:16908", "CHEBI:58349", "CHEBI:18009", "CHEBI:57783", "CHEBI:16474", "CHEBI:43474",
    "CHEBI:18367", "CHEBI:33019", "CHEBI:29888", "CHEBI:16526", "CHEBI:57287", "CHEBI:15346",
    "CHEBI:28938", "CHEBI:16134", "CHEBI:15379", "CHEBI:16240",
}


def currency_metabolites(df_met):
    """Identifiants des métabolites « monnaie » d'un réseau (via leurs annotations ChEBI)."""
    mask = df_met["chebi_ids"].apply(lambda ids: bool(CURRENCY_CHEBI.intersection(ids)))
    return set(df_met.loc[mask, "id"])


def shortest_path(g, source, target, excluded_metabolites=(), skip_biomass=True):
    """Plus court chemin métabolique entre deux métabolites.

    Les métabolites « monnaie » (H2O, ATP, H+…) relient tout à tout et donnent des chemins
    sans sens biologique : on peut les exclure via `excluded_metabolites`.
    La réaction « biomasse » (pseudo-réaction qui consomme presque tout) est aussi ignorée.
    Renvoie un DataFrame (étape, type, identifiant, nom) ou None si aucun chemin.
    """
    src, dst = f"M:{source}", f"M:{target}"
    excluded = {f"M:{m}" for m in excluded_metabolites} - {src, dst}
    if skip_biomass:
        excluded |= {n for n, d in g.nodes(data=True)
                     if d["kind"] == "reaction" and "biomass" in str(d["id"]).lower()}
    view = nx.subgraph_view(g, filter_node=lambda n: n not in excluded)
    try:
        path = nx.shortest_path(view, src, dst)
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return None
    return pd.DataFrame([{"étape": i, "type": g.nodes[n]["kind"], "identifiant": g.nodes[n]["id"],
                          "nom": g.nodes[n]["label"]} for i, n in enumerate(path)])


def path_to_dot(path_df):
    """Dessine un chemin au format Graphviz (ronds = métabolites, rectangles = réactions)."""
    lines = ["digraph P {", "rankdir=LR;", 'node [fontname="Helvetica", fontsize=11];']
    prev = None
    for i, row in path_df.iterrows():
        label = str(row["nom"]).replace('"', "'")
        if row["type"] == "metabolite":
            style = 'shape=ellipse, style=filled, fillcolor="#DDF1E4", color="#2E8B57"'
        else:
            style = 'shape=box, style="rounded,filled", fillcolor="#E3ECFA", color="#3B6FC4"'
        lines.append(f'n{i} [label="{label}", {style}];')
        if prev is not None:
            lines.append(f"n{prev} -> n{i};")
        prev = i
    lines.append("}")
    return "\n".join(lines)
