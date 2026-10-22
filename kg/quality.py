"""Étape 3 : contrôle qualité. Erreur = on bloque ; avertissement = on signale."""

# Format d'un identifiant MetaNetX chimique valide (WATER et BIOMASS sont des identifiants spéciaux)
MNX_PATTERN = r"MNXM\d+|WATER|BIOMASS"


def quality_check(df_met, df_rea, df_par):
    """Renvoie (erreurs bloquantes, avertissements, couverture des annotations)."""
    errors, warnings = [], []

    if df_met["id"].duplicated().any():
        errors.append("Identifiant de métabolite en double")
    if df_rea["id"].duplicated().any():
        errors.append("Identifiant de réaction en double")
    orphelins = df_par.loc[~df_par["metabolite_id"].isin(df_met["id"]), "metabolite_id"].unique()
    if len(orphelins):
        errors.append(f"Participant vers un métabolite inconnu : {list(orphelins)[:5]}")
    if not df_par["reaction_id"].isin(df_rea["id"]).all():
        errors.append("Participant vers une réaction inconnue")
    if (df_par["stoichiometry"] <= 0).any():
        errors.append("Stœchiométrie nulle ou négative")

    # Identifiants MetaNetX mal formés (ex. : un espace dans l'identifiant)
    annot = df_met.explode("metanetx_ids").dropna(subset=["metanetx_ids"])
    mal_formes = annot[~annot["metanetx_ids"].str.fullmatch(MNX_PATTERN)]
    for _, row in mal_formes.iterrows():
        warnings.append(f"MetaNetX mal formé pour {row['id']} : '{row['metanetx_ids']}'")

    # Un même identifiant MetaNetX porté par deux métabolites différents du même compartiment
    # (le glucose du cytosol et celui de l'extérieur partagent légitimement leur identifiant)
    valides = annot[annot["metanetx_ids"].str.fullmatch(MNX_PATTERN)]
    partages = valides.groupby(["compartment", "metanetx_ids"])["id"].apply(list)
    for (_, ident), ids in partages.items():
        if len(ids) > 1:
            warnings.append(f"{ident} est porté par plusieurs métabolites : {ids}")

    coverage = {}
    for col, nom in [("chebi_ids", "ChEBI"), ("metanetx_ids", "MetaNetX")]:
        coverage[nom] = (int(df_met[col].apply(len).gt(0).sum()), len(df_met))

    return errors, warnings, coverage
