import pandas as pd
import os

# Configuration des chemins
BASE_DIR = os.getcwd()
STAGING_DIR = os.path.join(BASE_DIR, "data_lake", "staging")
CURATED_DIR = os.path.join(BASE_DIR, "data_lake", "curated")

def run_curated():
    print("--- Démarrage de l'étape CURATED (Version Blindée) ---")
    
    if not os.path.exists(CURATED_DIR):
        os.makedirs(CURATED_DIR)

    # 1. Chargement du Staging
    csv_path = os.path.join(STAGING_DIR, "stg_dvf_monthly.csv")
    if not os.path.exists(csv_path):
        print("ERREUR FATALE: Le fichier stg_dvf_monthly.csv n'existe pas !")
        return

    try:
        # On lit juste les premières lignes pour vérifier les colonnes d'abord, puis tout le fichier
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"Données chargées : {len(df)} lignes.")
        print(f"Colonnes disponibles : {list(df.columns)}")
    except Exception as e:
        print(f"Erreur lecture CSV : {e}")
        return

    # --- INTELLIGENCE DES COLONNES ---
    # On cherche la colonne date (peut s'appeler 'date_mutation', 'mois', 'date', etc.)
    col_date = next((c for c in df.columns if 'date' in c or 'mois' in c), None)
    # On cherche la colonne valeur (peut s'appeler 'valeur_fonciere', 'prix', etc.)
    col_valeur = next((c for c in df.columns if 'valeur' in c or 'prix' in c or 'montant' in c), None)
    # On cherche la colonne nombre (peut s'appeler 'nb_mutations', 'nombre', etc.)
    col_nb = next((c for c in df.columns if 'nb' in c or 'mutations' in c), None)
    # On cherche la colonne departement
    col_dept = next((c for c in df.columns if 'dep' in c or 'cod' in c), None)

    print(f"Mapping utilisé -> Date: {col_date} | Valeur: {col_valeur} | Nb: {col_nb} | Dept: {col_dept}")

    # --- Dataset 1 : Évolution Nationale (France Trends) ---
    print("Génération de cur_france_trends.csv...")
    if col_date:
        # On groupe par date. Si 'col_valeur' existe on la somme, sinon on compte juste les lignes.
        aggs = {}
        if col_nb: aggs[col_nb] = 'sum'
        if col_valeur: aggs[col_valeur] = 'mean' # Moyenne des prix
        
        if not aggs: # Si aucune colonne de mesure, on compte juste les lignes
            df['count'] = 1
            aggs['count'] = 'sum'

        try:
            df_france = df.groupby(col_date).agg(aggs).reset_index()
            # On renomme pour standardiser
            df_france.columns = ['date_mutation', 'nb_mutations', 'valeur_fonciere'] if len(df_france.columns) == 3 else ['date_mutation', 'valeur']
            
            output_path = os.path.join(CURATED_DIR, "cur_france_trends.csv")
            df_france.to_csv(output_path, index=False)
            print(f"-> SUCCÈS : {output_path} créé.")
        except Exception as e:
            print(f"Erreur pendant le calcul France Trends : {e}")
    else:
        print("ERREUR : Impossible de trouver une colonne 'Date' pour créer les tendances.")

    # --- Dataset 2 : Top Départements ---
    print("Génération de cur_top_departments.csv...")
    if col_dept:
        try:
            measure = col_nb if col_nb else col_valeur
            if not measure:
                df['count'] = 1
                measure = 'count'
            
            df_dept = df.groupby(col_dept)[[measure]].sum().reset_index()
            df_dept = df_dept.sort_values(by=measure, ascending=False)
            
            output_path = os.path.join(CURATED_DIR, "cur_top_departments.csv")
            df_dept.to_csv(output_path, index=False)
            print(f"-> SUCCÈS : {output_path} créé.")
        except Exception as e:
            print(f"Erreur pendant le calcul Top Dept : {e}")
    else:
        print("ERREUR : Impossible de trouver une colonne 'Département'.")

if __name__ == "__main__":
    run_curated()