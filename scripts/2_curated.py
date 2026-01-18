import pandas as pd
import os
import numpy as np

BASE_DIR = os.getcwd()
STAGING_DIR = os.path.join(BASE_DIR, "data_lake", "staging")
CURATED_DIR = os.path.join(BASE_DIR, "data_lake", "curated")

def run_curated():
    print("--- Démarrage de l'étape CURATED ---")
    
    if not os.path.exists(CURATED_DIR):
        os.makedirs(CURATED_DIR)

    # 1. Chargement du fichier propre (Staging)
    csv_path = os.path.join(STAGING_DIR, "stg_dvf_monthly.csv")
    if not os.path.exists(csv_path):
        print("ERREUR : stg_dvf_monthly.csv introuvable.")
        return
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"Données chargées : {len(df)} lignes.")
    print(f"Colonnes disponibles : {list(df.columns)}")
    if 'nb_mutations' not in df.columns:
        print("-> Info : Colonne 'nb_mutations' absente. Calcul automatique (Maison + Appart)...")
        
        # On remplace les vides par 0 pour pouvoir additionner
        col_maison = df['nb_ventes_maison'].fillna(0) if 'nb_ventes_maison' in df.columns else 0
        col_appart = df['nb_ventes_appartement'].fillna(0) if 'nb_ventes_appartement' in df.columns else 0
        
        df['nb_mutations'] = col_maison + col_appart
    print("Génération de cur_france_trends.csv...")
    if 'date_mutation' in df.columns:
        aggs = {}
        aggs['nb_mutations'] = 'sum'
        if 'nb_ventes_appartement' in df.columns: aggs['nb_ventes_appartement'] = 'sum'
        if 'nb_ventes_maison' in df.columns: aggs['nb_ventes_maison'] = 'sum'
        if 'valeur_fonciere_moyenne' in df.columns: aggs['valeur_fonciere_moyenne'] = 'mean'
        
        # GroupBy Date
        try:
            df_trends = df.groupby('date_mutation').agg(aggs).reset_index()
            # Sauvegarde
            df_trends.to_csv(os.path.join(CURATED_DIR, "cur_france_trends.csv"), index=False, sep=',', encoding='utf-8-sig')
            print("-> OK : Tendances temporelles générées.")
        except Exception as e:
            print(f"ERREUR lors du groupby Trends : {e}")
    else:
        print("-> ERREUR : Colonne 'date_mutation' manquante.")
    print("Génération de cur_top_departments.csv...")
    if 'code_departement' in df.columns:
        # On prépare l'agrégation
        agg_dict_dept = {
            'nb_mutations': 'sum', 
        }
        if 'nb_ventes_appartement' in df.columns: agg_dict_dept['nb_ventes_appartement'] = 'sum'
        if 'nb_ventes_maison' in df.columns: agg_dict_dept['nb_ventes_maison'] = 'sum'
        if 'libelle_geo' in df.columns: agg_dict_dept['libelle_geo'] = 'first'

        try:
            df_dept = df.groupby('code_departement').agg(agg_dict_dept).reset_index()
            
            # Tri par volume total
            df_dept = df_dept.sort_values(by='nb_mutations', ascending=False)
            # Sauvegarde
            df_dept.to_csv(os.path.join(CURATED_DIR, "cur_top_departments.csv"), index=False, sep=',', encoding='utf-8-sig')
            print("-> OK : Top Départements généré.")
        except Exception as e:
             print(f"ERREUR lors du groupby Départements : {e}")
    else:
        print("-> ERREUR : Colonne 'code_departement' manquante.")

if __name__ == "__main__":
    run_curated()