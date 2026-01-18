import duckdb
import pandas as pd
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

print("--- ANALYSE BUSINESS INTELLIGENCE (STEP 6) ---")

db_path = os.path.join("warehouse", "real_estate.duckdb")
con = duckdb.connect(db_path)

def run_query(titre, sql):
    print(f"\n>>> {titre}")
    try:
        # On récupère le résultat en DataFrame pandas pour l'affichage
        df = con.execute(sql).df()
        print(df)
        print("-" * 60)
    except Exception as e:
        print(f"Erreur SQL : {e}")

# ==============================================================================
# Q1 & Q2 : Disponibilité des données (Janvier 2026 ?)
# ==============================================================================
# On regarde simplement la date min et max présentes dans la base.
run_query("1 & 2. Période de données disponibles", """
    SELECT 
        MIN(date_mutation) as date_debut, 
        MAX(date_mutation) as date_fin 
    FROM detail_monthly
""")

# ==============================================================================
# Q3 : Prix au m2 par type (Appartements vs Maisons)
# ==============================================================================
run_query("3. Prix moyen au m2 (Appartements vs Maisons)", """
    SELECT 
        ROUND(AVG(prix_m2_moyen_appartement), 0) as prix_m2_appart,
        ROUND(AVG(prix_m2_moyen_maison), 0) as prix_m2_maison
    FROM detail_monthly
    WHERE prix_m2_moyen_appartement > 0 AND prix_m2_moyen_maison > 0
""")

# ==============================================================================
# Q4 : Évolution des prix (Comparaison Annuelle / Year-over-Year)
# ==============================================================================
# J'utilise une CTE (Common Table Expression) pour préparer les données par mois,
# puis la fonction LAG() pour aller chercher la valeur d'il y a 12 mois.
run_query("4. Évolution des prix (Comparaison mois N vs N-12)", """
    WITH stats_globales AS (
        SELECT 
            date_mutation,
            -- On fait une moyenne globale approximative pour voir la tendance
            AVG(valeur_fonciere_moyenne) as prix_moyen_m2_global
        FROM detail_monthly
        GROUP BY date_mutation
    )
    SELECT 
        date_mutation,
        ROUND(prix_moyen_m2_global, 0) as prix_actuel,
        -- Récupération du prix 12 lignes (mois) avant
        ROUND(LAG(prix_moyen_m2_global, 12) OVER (ORDER BY date_mutation), 0) as prix_annee_precedente,
        -- Calcul du % d'évolution
        ROUND((prix_moyen_m2_global - LAG(prix_moyen_m2_global, 12) OVER (ORDER BY date_mutation)) 
              / LAG(prix_moyen_m2_global, 12) OVER (ORDER BY date_mutation) * 100, 2) as evolution_pct
    FROM stats_globales
    ORDER BY date_mutation DESC
    LIMIT 12
""")

# ==============================================================================
# Q5 : Top 10 Départements (Transactions & Prix)
# ==============================================================================
run_query("5a. Top 10 Départements par Volume de Transactions", """
    SELECT 
        code_departement,
        SUM(nb_mutations) as total_transactions
    FROM detail_monthly
    GROUP BY code_departement
    ORDER BY total_transactions DESC
    LIMIT 10
""")

run_query("5b. Top 10 Départements par Prix au m2 (le plus cher)", """
    SELECT 
        code_departement,
        ROUND(AVG(valeur_fonciere_moyenne), 0) as prix_moyen_m2
    FROM detail_monthly
    GROUP BY code_departement
    ORDER BY prix_moyen_m2 DESC
    LIMIT 10
""")

con.close()