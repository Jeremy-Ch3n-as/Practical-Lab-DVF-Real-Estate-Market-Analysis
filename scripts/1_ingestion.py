import pandas as pd
import os
import numpy as np

BASE_DIR = os.getcwd()
RAW_DIR = os.path.join(BASE_DIR, "data_lake", "raw")
STAGING_DIR = os.path.join(BASE_DIR, "data_lake", "staging")

def clean_column_names(df):
    """Nettoie les noms : minuscule, sans accents, sans espaces"""
    df.columns = [
        c.strip().lower()
        .replace(' ', '_')
        .replace('-', '_')
        .replace('é', 'e')
        .replace('è', 'e')
        .replace("'", "")
        for c in df.columns
    ]
    return df

def run_staging():
    print("--- Démarrage de l'étape STAGING ---")
    
    if not os.path.exists(STAGING_DIR):
        os.makedirs(STAGING_DIR)

    print("\nTraitement de dvf_monthly.csv...")
    file_path = os.path.join(RAW_DIR, "dvf_monthly.csv")
    
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, sep=',', low_memory=False)
            df = clean_column_names(df)

            # --- FILTRAGE DÉPARTEMENT ---
            if 'echelle_geo' in df.columns:
                df = df[df['echelle_geo'] == 'departement'].copy()

            # --- RENOMMAGE ---
            rename_dict = {
                'code_geo': 'code_departement', 
                'moy_prix_m2_maison': 'prix_m2_moyen_maison',
                'moy_prix_m2_appartement': 'prix_m2_moyen_appartement',
                'moy_prix_m2_apt_maison': 'valeur_fonciere_moyenne',
                'annee_mois': 'date_mutation' # Alignement temporel
            }
            df = df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns})
            if 'code_departement' in df.columns:
                df['code_departement'] = df['code_departement'].astype(str).str.zfill(2)
            df = df.fillna(0)

            df.to_csv(os.path.join(STAGING_DIR, "stg_dvf_monthly.csv"), index=False, sep=',', encoding='utf-8-sig')
            print(f"-> SUCCÈS Monthly : {len(df)} lignes départements conservées.")

        except Exception as e:
            print(f"ERREUR Monthly : {e}")
    print("\nTraitement de dvf_aggregated.csv...")
    file_path_agg = os.path.join(RAW_DIR, "dvf_aggregated.csv")
    
    if os.path.exists(file_path_agg):
        try:
            # On lit le CSV 
            df_agg = pd.read_csv(file_path_agg, sep=',', low_memory=False)
            df_agg = clean_column_names(df_agg)
            if 'echelle_geo' in df_agg.columns:
                print("  -> Filtrage Aggregated : Conservation uniquement des lignes 'departement'...")
                df_agg = df_agg[df_agg['echelle_geo'] == 'departement'].copy()

            rename_dict_agg = {
                'code_geo': 'code_departement', 
                'nb_ventes_whole_appartement': 'nb_ventes_appartement',
                'nb_ventes_whole_maison': 'nb_ventes_maison',
                'moy_prix_m2_whole_appartement': 'prix_m2_moyen_appartement',
                'moy_prix_m2_whole_maison': 'prix_m2_moyen_maison',
            }
            df_agg = df_agg.rename(columns={k: v for k, v in rename_dict_agg.items() if k in df_agg.columns})
            if 'code_departement' in df_agg.columns:
                df_agg['code_departement'] = df_agg['code_departement'].astype(str).str.zfill(2)

            df_agg = df_agg.fillna(0)

            df_agg.to_csv(os.path.join(STAGING_DIR, "stg_dvf_aggregated.csv"), index=False, sep=',', encoding='utf-8-sig')
            print(f"-> SUCCÈS Aggregated : {len(df_agg)} lignes départements conservées.")
        
        except Exception as e:
            print(f"ERREUR Aggregated : {e}")

if __name__ == "__main__":
    run_staging()