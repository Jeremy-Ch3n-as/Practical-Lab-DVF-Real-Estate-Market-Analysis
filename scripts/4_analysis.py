import duckdb
import os
import pandas as pd

# Configuration affichage
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

db_path = os.path.join("warehouse", "real_estate.duckdb")
con = duckdb.connect(db_path)

def query(title, sql):
    print(f"\n{'='*60}\n {title}\n{'='*60}")
    try:
        print(con.execute(sql).df())
    except Exception as e:
        print(f"Info: La requête a dû être adaptée ({e}).")

print("--- ANALYSE IMMOBILIÈRE (ADAPTÉE AUX DONNÉES STATISTIQUES) ---")

# 1. Période (Ça marchait déjà)
query("1. Période disponible", 
      "SELECT MIN(date_mutation) as debut, MAX(date_mutation) as fin FROM detail_monthly")

# 3. Volume par type de bien (Au lieu du prix médian impossible à calculer sans raw data)
# On utilise les colonnes 'nb_ventes_appartement' et 'nb_ventes_maison' si elles existent
print("\n>>> Tentative d'analyse par Type de Bien (Maison/Appart)")

# On vérifie d'abord les colonnes disponibles
cols = [c[0] for c in con.execute("DESCRIBE detail_monthly").fetchall()]

sql_type = ""
if 'nb_ventes_appartement' in cols and 'nb_ventes_maison' in cols:
    sql_type = """
        SELECT 
            SUM(nb_ventes_appartement) as total_ventes_appart,
            SUM(nb_ventes_maison) as total_ventes_maison
        FROM detail_monthly
    """
    query("3. Volume global par type (Appart vs Maison)", sql_type)
    
    # Si on a aussi les prix moyens au m2 (souvent 'prix_m2_moyen_...' dans ces fichiers)
    if 'prix_moyen_m2_appartement' in cols:
         query("3b. Prix Moyens m2 (Estimés sur moyennes mensuelles)", """
            SELECT 
                AVG(prix_moyen_m2_appartement) as prix_moyen_appart_ref,
                AVG(prix_moyen_m2_maison) as prix_moyen_maison_ref
            FROM detail_monthly
         """)
else:
    print("Pas de distinction Maison/Appartement trouvée. On analyse le global.")

# 4. Évolution temporelle (Basé sur le volume global)
# On utilise 'nb_ventes_local' qui est apparu dans ton erreur
col_vol = 'nb_ventes_local' if 'nb_ventes_local' in cols else 'nb_mutations'

query("4. Évolution du Volume des Ventes (Derniers mois)", f"""
    SELECT 
        date_mutation, 
        SUM({col_vol}) as volume_ventes
    FROM detail_monthly
    GROUP BY date_mutation
    ORDER BY date_mutation DESC
    LIMIT 6
""")

# 5. Top Départements (Activité)
# On utilise 'code_geo' ou 'code_parent' comme identifiant géo
col_geo = 'code_departement'
if 'code_departement' not in cols:
    # Dans les stats, c'est souvent 'code_geo' ou 'code_parent'
    if 'code_geo' in cols: col_geo = 'code_geo'
    elif 'code_parent' in cols: col_geo = 'code_parent'

query("5. Top 10 Zones les plus actives (Volume)", f"""
    SELECT 
        {col_geo} as code_zone, 
        SUM({col_vol}) as total_ventes
    FROM detail_monthly
    GROUP BY {col_geo}
    ORDER BY total_ventes DESC
    LIMIT 10
""")

con.close()