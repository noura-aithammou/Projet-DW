#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script d'import des données CSV brutes dans PostgreSQL
Projet: morocco_banks_reviews (dbt project)
Chemin: DATA/scripts/import_raw_data.py
Base: bank_maroc
Table: public.raw_reviews
Utilisateur: airflow1
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
import sys
import os

# Configuration des chemins relatifs au projet dbt
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))  # Remonte à morocco_banks_reviews/
DATA_RAW_DIR = os.path.join(SCRIPT_DIR, "..", "raw")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")

# Créer le dossier logs s'il n'existe pas
os.makedirs(LOGS_DIR, exist_ok=True)

# Configuration de logging sans emojis pour Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'import_raw_data.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class RawDataImporter:
    def __init__(self):
        """Initialiser l'importeur avec la configuration de la base"""
        self.db_config = {
            'host': 'localhost',
            'database': 'bank_maroc',
            'user': 'airflow1',
            'password': 'airflow',
            'port': 5432
        }
        self.connection = None
        
    def test_connection(self):
        """Tester la connexion à la base de données"""
        try:
            logger.info("[CONNEXION] Test de connexion à la base de données bank_maroc...")
            self.connection = psycopg2.connect(**self.db_config)
            cursor = self.connection.cursor()
            
            # Tester avec une requête simple
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            logger.info(f"[SUCCÈS] Connexion réussie - PostgreSQL Version: {version}")
            
            # Vérifier la base de données
            cursor.execute("SELECT current_database();")
            db_name = cursor.fetchone()[0]
            logger.info(f"[DATABASE] Base de données connectée: {db_name}")
            
            # Vérifier que la table existe
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'raw_reviews'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                logger.info("[TABLE] Table public.raw_reviews trouvée")
            else:
                logger.error("[ERREUR] Table public.raw_reviews non trouvée")
                logger.error("[SOLUTION] Exécutez d'abord les scripts de création de table")
                return False
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"[ERREUR] Erreur de connexion: {e}")
            logger.error("[SOLUTION] Vérifiez que PostgreSQL est démarré et que les credentials sont corrects")
            return False
    
    def get_csv_file_path(self):
        """Obtenir le chemin vers le fichier CSV"""
        csv_filename = "donnees_agences_avis.csv"
        csv_path = os.path.join(DATA_RAW_DIR, csv_filename)
        
        logger.info(f"[CHEMIN] Répertoire du script: {SCRIPT_DIR}")
        logger.info(f"[CHEMIN] Répertoire DATA/raw: {DATA_RAW_DIR}")
        logger.info(f"[CHEMIN] Chemin CSV attendu: {csv_path}")
        
        return csv_path
    
    def insert_raw_data(self, df):
        """Insérer les données brutes SANS AUCUN nettoyage dans public.raw_reviews"""
        try:
            cursor = self.connection.cursor()
            
            # Vider la table avant insertion
            logger.info("[NETTOYAGE] Suppression des données existantes...")
            cursor.execute("TRUNCATE TABLE public.raw_reviews RESTART IDENTITY;")
            
            # Vérifier les colonnes du DataFrame
            expected_columns = ['Banque', 'Ville', 'Nom Agence', 'Localisation', 'Note', 'Avis', 'Date Avis']
            missing_columns = [col for col in expected_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"[ERREUR] Colonnes manquantes dans le CSV: {missing_columns}")
                logger.info(f"[INFO] Colonnes trouvées: {list(df.columns)}")
                return False
            
            # Préparer les données exactement comme elles sont
            data_tuples = []
            skipped_rows = 0
            
            logger.info("[PREPARATION] Préparation des données pour l'insertion...")
            for index, row in df.iterrows():
                try:
                    # Convertir en string et gérer les valeurs NaN
                    banque = str(row['Banque']) if pd.notna(row['Banque']) else None
                    ville = str(row['Ville']) if pd.notna(row['Ville']) else None
                    nom_agence = str(row['Nom Agence']) if pd.notna(row['Nom Agence']) else None
                    localisation = str(row['Localisation']) if pd.notna(row['Localisation']) else None
                    note = str(row['Note']) if pd.notna(row['Note']) else None
                    avis = str(row['Avis']) if pd.notna(row['Avis']) else None
                    date_avis = str(row['Date Avis']) if pd.notna(row['Date Avis']) else None
                    
                    data_tuples.append((banque, ville, nom_agence, localisation, note, avis, date_avis))
                    
                except Exception as e:
                    logger.warning(f"[AVERTISSEMENT] Erreur ligne {index}: {e}")
                    skipped_rows += 1
                    continue
            
            if skipped_rows > 0:
                logger.warning(f"[AVERTISSEMENT] {skipped_rows} lignes ignorées à cause d'erreurs")
            
            # Requête d'insertion
            insert_query = """
            INSERT INTO public.raw_reviews 
            (banque, ville, nom_agence, localisation, note, avis, date_avis) 
            VALUES %s
            """
            
            # Insertion par batch
            logger.info(f"[INSERTION] Insertion de {len(data_tuples)} lignes brutes dans public.raw_reviews...")
            
            if len(data_tuples) > 0:
                execute_values(
                    cursor, 
                    insert_query, 
                    data_tuples,
                    template=None, 
                    page_size=1000
                )
                
                self.connection.commit()
                logger.info("[SUCCÈS] Insertion des données brutes réussie!")
            else:
                logger.error("[ERREUR] Aucune donnée à insérer")
                return False
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"[ERREUR] Erreur lors de l'insertion: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def verify_insertion(self):
        """Vérifier l'insertion avec des statistiques basiques"""
        try:
            cursor = self.connection.cursor()
            
            # Compter le nombre total de lignes
            cursor.execute("SELECT COUNT(*) FROM public.raw_reviews;")
            total_rows = cursor.fetchone()[0]
            
            # Compter les valeurs NULL par colonne
            null_counts_query = """
            SELECT 
                COUNT(*) - COUNT(banque) as banque_nulls,
                COUNT(*) - COUNT(ville) as ville_nulls,
                COUNT(*) - COUNT(nom_agence) as nom_agence_nulls,
                COUNT(*) - COUNT(localisation) as localisation_nulls,
                COUNT(*) - COUNT(note) as note_nulls,
                COUNT(*) - COUNT(avis) as avis_nulls,
                COUNT(*) - COUNT(date_avis) as date_avis_nulls
            FROM public.raw_reviews;
            """
            cursor.execute(null_counts_query)
            null_counts = cursor.fetchone()
            
            # Top banques
            cursor.execute("""
                SELECT banque, COUNT(*) as count 
                FROM public.raw_reviews 
                WHERE banque IS NOT NULL
                GROUP BY banque 
                ORDER BY count DESC 
                LIMIT 5;
            """)
            top_banks = cursor.fetchall()
            
            # Top villes
            cursor.execute("""
                SELECT ville, COUNT(*) as count 
                FROM public.raw_reviews 
                WHERE ville IS NOT NULL
                GROUP BY ville 
                ORDER BY count DESC 
                LIMIT 5;
            """)
            top_cities = cursor.fetchall()
            
            # Échantillon de données
            cursor.execute("SELECT id, banque, ville, LEFT(avis, 50) as avis_extrait FROM public.raw_reviews LIMIT 3;")
            sample_data = cursor.fetchall()
            
            # Afficher les statistiques
            logger.info(f"[STATS] *** STATISTIQUES D'INSERTION BRUTE ***")
            logger.info(f"[STATS] Total des lignes insérées: {total_rows}")
            
            logger.info(f"[STATS] Valeurs NULL par colonne:")
            columns = ['banque', 'ville', 'nom_agence', 'localisation', 'note', 'avis', 'date_avis']
            for i, col in enumerate(columns):
                logger.info(f"[STATS]    {col}: {null_counts[i]} valeurs NULL")
            
            logger.info(f"[STATS] Top 5 Banques:")
            for bank, count in top_banks:
                logger.info(f"[STATS]    {bank}: {count} avis")
                
            logger.info(f"[STATS] Top 5 Villes:")
            for city, count in top_cities:
                logger.info(f"[STATS]    {city}: {count} avis")
            
            logger.info(f"[STATS] Échantillon de données (3 premières lignes):")
            for i, row in enumerate(sample_data, 1):
                logger.info(f"[STATS]    Ligne {i}: ID={row[0]}, Banque={row[1]}, Ville={row[2]}, Avis='{row[3]}...'")
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"[ERREUR] Erreur lors de la vérification: {e}")
            return False
    
    def import_csv_raw(self, csv_file_path):
        """Fonction principale pour importer le CSV BRUT dans public.raw_reviews"""
        try:
            # Vérifier que le fichier existe
            if not os.path.exists(csv_file_path):
                logger.error(f"[ERREUR] Fichier CSV non trouvé: {csv_file_path}")
                logger.info(f"[INFO] Répertoire DATA/raw: {DATA_RAW_DIR}")
                
                # Lister les fichiers disponibles
                if os.path.exists(DATA_RAW_DIR):
                    files = [f for f in os.listdir(DATA_RAW_DIR) if f.endswith('.csv')]
                    if files:
                        logger.info(f"[INFO] Fichiers CSV disponibles dans DATA/raw:")
                        for file in files:
                            logger.info(f"[INFO]    {file}")
                    else:
                        logger.info("[INFO] Aucun fichier CSV trouvé dans DATA/raw/")
                else:
                    logger.error(f"[ERREUR] Répertoire DATA/raw/ non trouvé: {DATA_RAW_DIR}")
                    logger.info("[SOLUTION] Créez le répertoire DATA/raw/ et placez-y votre fichier CSV")
                
                return False
            
            # Lire le CSV exactement comme il est
            logger.info(f"[LECTURE] Lecture du fichier CSV brut: {csv_file_path}")
            
            # Essayer différents encodages
            encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_file_path, sep=';', encoding=encoding)
                    logger.info(f"[SUCCÈS] Fichier lu avec succès (encoding: {encoding})")
                    break
                except Exception as e:
                    logger.warning(f"[AVERTISSEMENT] Échec avec encoding {encoding}: {e}")
                    continue
            
            if df is None:
                logger.error("[ERREUR] Impossible de lire le fichier CSV avec les encodages testés")
                return False
            
            logger.info(f"[INFO] Nombre de lignes lues: {len(df)}")
            
            # Afficher les colonnes pour vérification
            logger.info(f"[INFO] Colonnes trouvées: {list(df.columns)}")
            
            # Afficher quelques statistiques du CSV
            logger.info(f"[INFO] Aperçu des données:")
            logger.info(f"[INFO]    Premières lignes: {len(df.head())}")
            logger.info(f"[INFO]    Valeurs manquantes par colonne:")
            for col in df.columns:
                null_count = df[col].isnull().sum()
                logger.info(f"[INFO]      {col}: {null_count} valeurs manquantes")
            
            # Tester la connexion
            if not self.test_connection():
                return False
            
            # Insérer les données brutes
            if not self.insert_raw_data(df):
                return False
            
            # Vérifier l'insertion
            if not self.verify_insertion():
                return False
            
            logger.info("[SUCCÈS] Import des données brutes terminé avec succès!")
            return True
            
        except Exception as e:
            logger.error(f"[ERREUR] Erreur générale: {e}")
            return False
        
        finally:
            # Fermer la connexion
            if self.connection:
                self.connection.close()
                logger.info("[CONNEXION] Connexion fermée")

def main():
    """Fonction principale"""
    print("=" * 80)
    print("IMPORT DONNÉES BRUTES - PROJET DBT morocco_banks_reviews")
    print("=" * 80)
    print(f"Répertoire du script: {SCRIPT_DIR}")
    print(f"Répertoire racine du projet: {PROJECT_ROOT}")
    print(f"Table cible: public.raw_reviews (bank_maroc)")
    print("AUCUN nettoyage ne sera effectué - données exactement comme dans le CSV")
    print("=" * 80)
    
    # Créer l'importeur
    importer = RawDataImporter()
    
    # Obtenir le chemin du fichier CSV
    csv_file_path = importer.get_csv_file_path()
    
    # Lancer l'import
    if importer.import_csv_raw(csv_file_path):
        print("\n" + "=" * 80)
        print("SUCCÈS: Les données BRUTES ont été importées dans public.raw_reviews!")
        print("Aucun nettoyage effectué - données exactement comme scrapées")
        print("\nPROCHAINES ÉTAPES:")
        print("   1. cd ../../  # Retourner à la racine du projet dbt")
        print("   2. dbt debug  # Tester la configuration dbt")
        print("   3. dbt run    # Exécuter les transformations")
        print("   4. dbt test   # Lancer les tests")
        print("\nVÉRIFIER LES DONNÉES:")
        print("   psql -U airflow1 -d bank_maroc")
        print("   SELECT COUNT(*) FROM raw_reviews;")
        print(f"\nLOGS SAUVEGARDÉS: {os.path.join(LOGS_DIR, 'import_raw_data.log')}")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("ÉCHEC: L'import a échoué. Vérifiez les logs pour plus de détails.")
        print(f"Consultez le fichier de log: {os.path.join(LOGS_DIR, 'import_raw_data.log')}")
        print("=" * 80)
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()