import pandas as pd
import os

# --- CONFIGURATION ---
BASE_DIR = os.getcwd()
RAW_DIR = os.path.join(BASE_DIR, "data_lake", "raw")
STAGING_DIR = os.path.join(BASE_DIR, "data_lake", "staging")

def clean_column_names(df):
    """Fonction pour standardiser les noms de colonnes"""
    df.columns = [c.strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]
    return df

def run_staging():
    print("--- Démarrage de l'étape STAGING ---")
    
    if not os.path.exists(STAGING_DIR):
        os.makedirs(STAGING_DIR)

    print("\nTraitement de dvf_monthly.csv...")
    file_path = os.path.join(RAW_DIR, "dvf_monthly.csv")
    
    if os.path.exists(file_path):
        try:
            try:
                df_month = pd.read_csv(file_path, sep=',')
                if len(df_month.columns) < 2: 
                    df_month = pd.read_csv(file_path, sep=';')
            except:
                df_month = pd.read_csv(file_path, sep=';')

            # Nettoyage des noms de colonnes
            df_month = clean_column_names(df_month)
            print(f"Colonnes trouvées : {list(df_month.columns)}")
            # 1. Identifier la date
            col_date = next((c for c in df_month.columns if 'mois' in c or 'date' in c), None)
            
            # 2. Identifier le prix (Moyen ou Médian ou Valeur)
            col_prix = next((c for c in df_month.columns if 'moyen' in c or 'median' in c or 'valeur' in c), None)
            
            # 3. Identifier le nombre de ventes
            col_nb = next((c for c in df_month.columns if 'nb' in c or 'mutations' in c), 'nb_mutations')

            print(f"Mapping détecté -> Date: {col_date} | Prix: {col_prix} | Vol: {col_nb}")

            # Renommage pour standardiser la suite du lab
            rename_dict = {}
            if col_date: rename_dict[col_date] = 'date_mutation'
            if col_prix: rename_dict[col_prix] = 'valeur_fonciere' 
            if col_nb: rename_dict[col_nb] = 'nb_mutations'
            
            df_month = df_month.rename(columns=rename_dict)

            # Remplissage des trous
            if 'valeur_fonciere' in df_month.columns:
                df_month['valeur_fonciere'] = df_month['valeur_fonciere'].fillna(0)
            
            # Sauvegarde
            df_month.to_csv(os.path.join(STAGING_DIR, "stg_dvf_monthly.csv"), index=False)
            print("-> stg_dvf_monthly.csv créé avec succès.")

        except Exception as e:
            print(f"ERREUR CRITIQUE sur dvf_monthly : {e}")
    else:
        print("Fichier dvf_monthly.csv introuvable dans RAW !")

    print("\nTraitement de dvf_aggregated.csv...")
    file_path_agg = os.path.join(RAW_DIR, "dvf_aggregated.csv")
    
    if os.path.exists(file_path_agg):
        try:
            df_agg = pd.read_csv(file_path_agg, sep=None, engine='python') # Auto-détection
            df_agg = df_agg.fillna(0)
            df_agg = clean_column_names(df_agg)
            
            df_agg.to_csv(os.path.join(STAGING_DIR, "stg_dvf_aggregated.csv"), index=False)
            print("-> stg_dvf_aggregated.csv créé avec succès.")
        except Exception as e:
            print(f"Erreur sur le fichier agrégé : {e}")

if __name__ == "__main__":
    run_staging()