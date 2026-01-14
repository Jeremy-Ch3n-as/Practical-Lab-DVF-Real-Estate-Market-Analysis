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
    
    print(f"Base de données connectée : {db_path}")
    
    print("Chargement de la table 'detail_monthly'...")
    csv_path_monthly = os.path.join(STAGING_DIR, 'stg_dvf_monthly.csv')
    
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE detail_monthly AS 
            SELECT * FROM read_csv('{csv_path_monthly}', header=True, ignore_errors=True)
        """)
        print("-> Table 'detail_monthly' chargée (lignes corrompues ignorées).")
    except Exception as e:
        print(f"ERREUR CRITIQUE detail_monthly: {e}")

    print("Chargement de la table 'bi_trends'...")
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE bi_trends AS 
            SELECT * FROM read_csv('{os.path.join(CURATED_DIR, 'cur_france_trends.csv')}', header=True, ignore_errors=True)
        """)
    except Exception as e:
        print(f"Erreur bi_trends: {e}")
    
    print("Chargement de la table 'bi_departments'...")
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE bi_departments AS 
            SELECT * FROM read_csv('{os.path.join(CURATED_DIR, 'cur_top_departments.csv')}', header=True, ignore_errors=True)
        """)
    except Exception as e:
        print(f"Erreur bi_departments: {e}")

    print("\n--- Validation ---")
    tables = con.execute("SHOW TABLES").fetchall()
    print(f"Tables créées avec succès : {tables}")
    
    # Vérification du nombre de lignes
    try:
        count = con.execute("SELECT COUNT(*) FROM detail_monthly").fetchone()[0]
        print(f"Nombre total de lignes chargées dans detail_monthly : {count}")
    except:
        print("Impossible de compter les lignes.")
    
    con.close()

if __name__ == "__main__":
    build_warehouse()