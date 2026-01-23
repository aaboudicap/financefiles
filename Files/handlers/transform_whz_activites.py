import logging
import pandas as pd
from utils.excel_utils import enlever_accents,normaliser_colonnes_activité, split_talent, extraire_semaine_annee_mois,harmoniser_etablissement
from io import BytesIO


def nettoyer_fichier_activites(blob_client):
    logging.info("Fonction de nettoyage appelée pour le fichier whoz activités")
    # Tu peux aussi tester avec un logging si tu préfères :
    # logging.info("Fonction de nettoyage appelée")
    
    stream = BytesIO()
    blob_data = blob_client.download_blob()
    blob_data.readinto(stream)
    stream.seek(0)

    df_activites = pd.read_excel(stream)

    df_activites.columns = normaliser_colonnes_activité(df_activites.columns)

    df_activites['ggid'] = df_activites['ggid'].astype(pd.Int64Dtype()).astype(str).str.zfill(8)

    df_activites.rename(columns={
        "resource_practice":"entite_niveau_2",
        "resource_sub_practic": "entite_niveau_3",
        "resource_practice_su" : "entite_niveau_4",
        "production_unit" : "pu_code",
        "mentor":"manager",
        "talent_city":"etablissement",
        "work-study":"work_study",
        "therapeutic_half-time": "therapeutic_half_time"
    }, inplace=True)


    #Dictionnaire de correspondance pour entite_niveau_2
    mapping_entite_niveau_2 = {
        "MANAGED SERVICES": "MS",
        "PNC": "P&C",
        "P&C": "P&C",
        "CORE": "CORE"
    }

    # Fonction d'harmonisation
    def harmoniser_entite_niveau_2(val):
        if isinstance(val, str):
            val = val.upper().strip()
            return mapping_entite_niveau_2.get(val, ''.join(m[0].upper() for m in val.split()))
        return ''

    # Application de la fonction
    df_activites['entite_niveau_2'] = df_activites['entite_niveau_2'].apply(harmoniser_entite_niveau_2)


    df_activites['etablissement']=df_activites['etablissement'].apply(harmoniser_etablissement)

    if 'talent' in df_activites.columns: 
        idx = df_activites.columns.get_loc("talent")
        nouvelles_colonnes = df_activites['talent'].apply(split_talent)
        nouvelles_colonnes.columns = ['nom', 'prenom']
        df_activites.drop(columns=['talent'], inplace=True)
        df_activites.insert(idx, 'nom', nouvelles_colonnes['nom'])
        df_activites.insert(idx + 1, 'prenom', nouvelles_colonnes['prenom'])

    
    def nettoyer_nom_mentor(nom):
      if isinstance(nom, str):
        nom_sans_accents = enlever_accents(nom)
        return ' '.join(mot.capitalize() for mot in nom_sans_accents.strip().split())
      return nom

    df_activites['manager'] = df_activites['manager'].apply(nettoyer_nom_mentor)
    df_activites = df_activites[~df_activites.apply(lambda row: row.astype(str).str.contains("Filtres appliqués", case=False, na=False)).any(axis=1)]
    df_activites = df_activites[~df_activites.apply(lambda row: row.astype(str).str.contains("Total", case=False, na=False)).any(axis=1)]

    df_activites = df_activites[df_activites['ggid'].notna()]
    df_activites = df_activites[df_activites['ggid'].astype(str).str.isdigit()]
    df_activites.reset_index(drop=True, inplace=True)
    df_activites = df_activites[df_activites['mois'].notna()]
    df_activites['source'] = blob_client.blob_name
    
    semaine, annee, mois_file = extraire_semaine_annee_mois(df_activites['source'].iloc[0])
    df_activites['semaine'] = semaine
    df_activites['annee'] = annee
    df_activites['mois_file'] = mois_file

    df_activites['entite_niveau_1']='CIS'
    logging.info(f"✅ Fichier nettoyé, nombre de lignes : {len(df_activites)}")

    return df_activites