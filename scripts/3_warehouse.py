import duckdb
import os

BASE_DIR = os.getcwd()
STAGING_DIR = os.path.join(BASE_DIR, "data_lake", "staging")
CURATED_DIR = os.path.join(BASE_DIR, "data_lake", "curated")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")

def build_warehouse():
    print("\n--- Construction du DATA WAREHOUSE ---")
    
    if not os.path.exists(WAREHOUSE_DIR):
        os.makedirs(WAREHOUSE_DIR)
    
    db_path = os.path.join(WAREHOUSE_DIR, "real_estate.duckdb")
    con = duckdb.connect(db_path)
    print(f"Base connectée : {db_path}")
    # Fonction pour charger une table proprement
    def load_table(table_name, csv_path):
        if os.path.exists(csv_path):
            print(f"Chargement de {table_name}...")
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv('{csv_path}', auto_detect=True, normalize_names=True)")
            
            # Vérifie
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            cols = [c[0] for c in con.execute(f"DESCRIBE {table_name}").fetchall()]
            print(f"  -> OK : {count} lignes chargées.")
        else:
            print(f"  -> ERREUR : Fichier {csv_path} introuvable.")
    load_table("detail_monthly", os.path.join(STAGING_DIR, "stg_dvf_monthly.csv"))
    load_table("bi_trends", os.path.join(CURATED_DIR, "cur_france_trends.csv"))
    load_table("bi_departments", os.path.join(CURATED_DIR, "cur_top_departments.csv"))

    con.close()
    print("Warehouse terminé.")

if __name__ == "__main__":
    build_warehouse()