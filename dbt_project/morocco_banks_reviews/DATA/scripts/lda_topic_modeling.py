#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script d'extraction de topics avec LDA (Latent Dirichlet Allocation)
Projet: morocco_banks_reviews
Chemin: DATA/scripts/lda_topic_modeling.py
"""

import pandas as pd
import psycopg2
import numpy as np
import re
import logging
from datetime import datetime
import sys
import os

# NLP et LDA
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import SnowballStemmer

# Configuration de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../../logs/lda_topics.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class LDATopicExtractor:
    def __init__(self):
        """Initialiser l'extracteur LDA"""
        self.db_config = {
            'host': 'localhost',
            'database': 'bank_maroc',
            'user': 'airflow1',
            'password': 'airflow',
            'port': 5432
        }
        self.connection = None
        self.stemmer_fr = SnowballStemmer('french')
        self.stemmer_ar = SnowballStemmer('arabic')
        
        # Télécharger les ressources NLTK nécessaires
        self.download_nltk_resources()
        
        # Mots vides personnalisés pour le contexte bancaire
        self.custom_stopwords = {
            'fr': ['banque', 'agence', 'bank', 'cih', 'bmce', 'attijariwafa', 'barid', 
                   'très', 'tout', 'bien', 'mal', 'plus', 'moins', 'aussi', 'encore',
                   'toujours', 'jamais', 'ici', 'là', 'maintenant', 'aujourd', 'hier'],
            'ar': ['بنك', 'وكالة', 'فرع', 'جدا', 'كل', 'هذا', 'ذلك', 'هنا', 'هناك']
        }
    
    def download_nltk_resources(self):
        """Télécharger les ressources NLTK nécessaires"""
        try:
            nltk.data.find('tokenizers/punkt')
            nltk.data.find('corpora/stopwords')
        except LookupError:
            logger.info("[NLTK] Téléchargement des ressources...")
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
            logger.info("[NLTK] Ressources téléchargées")
    
    def connect_db(self):
        """Se connecter à la base de données"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("[DB] Connexion établie")
            return True
        except Exception as e:
            logger.error(f"[DB] Erreur de connexion: {e}")
            return False
    
    def get_reviews_data(self):
        """Récupérer les avis depuis la base de données"""
        try:
            query = """
            SELECT 
                id,
                avis_cleaned as avis,
                langue_detected as langue,
                banque,
                ville
            FROM int_reviews_deduplicated 
            WHERE should_keep = true 
                AND avis_cleaned IS NOT NULL 
                AND trim(avis_cleaned) != ''
                AND length(avis_cleaned) >= 10
            ORDER BY id
            """
            
            df = pd.read_sql(query, self.connection)
            logger.info(f"[DATA] {len(df)} avis récupérés pour l'analyse LDA")
            return df
            
        except Exception as e:
            logger.error(f"[DATA] Erreur lors de la récupération: {e}")
            return None
    
    def preprocess_text(self, text, language='fr'):
        """Préprocesser le texte pour LDA"""
        if pd.isna(text) or text.strip() == '':
            return ""
        
        # Nettoyer le texte
        text = re.sub(r'[^\w\s]', ' ', str(text).lower())
        text = re.sub(r'\d+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Tokenisation
        tokens = word_tokenize(text, language='french')
        
        # Supprimer les mots vides
        if language == 'fr':
            stop_words = set(stopwords.words('french')) | set(self.custom_stopwords['fr'])
            stemmer = self.stemmer_fr
        elif language == 'ar':
            stop_words = set(stopwords.words('arabic')) | set(self.custom_stopwords['ar'])
            stemmer = self.stemmer_ar
        else:
            stop_words = set(stopwords.words('french')) | set(self.custom_stopwords['fr'])
            stemmer = self.stemmer_fr
        
        # Filtrer et stemmer
        processed_tokens = []
        for token in tokens:
            if (len(token) > 2 and 
                token not in stop_words and 
                token.isalpha()):
                try:
                    stemmed = stemmer.stem(token)
                    processed_tokens.append(stemmed)
                except:
                    processed_tokens.append(token)
        
        return ' '.join(processed_tokens)
    
    def perform_lda_analysis(self, df, n_topics=8, language='fr'):
        """Effectuer l'analyse LDA"""
        logger.info(f"[LDA] Début analyse pour {language} avec {n_topics} topics")
        
        # Filtrer par langue
        df_lang = df[df['langue'] == language].copy()
        
        if len(df_lang) < 10:
            logger.warning(f"[LDA] Pas assez de données pour {language}: {len(df_lang)} avis")
            return None, None, None
        
        # Préprocesser les textes
        logger.info(f"[LDA] Préprocessing de {len(df_lang)} textes...")
        df_lang['processed_text'] = df_lang['avis'].apply(
            lambda x: self.preprocess_text(x, language)
        )
        
        # Filtrer les textes vides après preprocessing
        df_lang = df_lang[df_lang['processed_text'].str.len() > 5]
        
        if len(df_lang) < 10:
            logger.warning(f"[LDA] Pas assez de textes valides après preprocessing: {len(df_lang)}")
            return None, None, None
        
        # Vectorisation
        logger.info("[LDA] Vectorisation TF-IDF...")
        vectorizer = TfidfVectorizer(
            max_features=1000,
            min_df=2,
            max_df=0.8,
            ngram_range=(1, 2),
            stop_words=None  # Déjà traité
        )
        
        tfidf_matrix = vectorizer.fit_transform(df_lang['processed_text'])
        
        # Modèle LDA
        logger.info(f"[LDA] Entraînement du modèle LDA...")
        lda_model = LatentDirichletAllocation(
            n_components=n_topics,
            random_state=42,
            max_iter=10,
            learning_method='online',
            learning_offset=50.0
        )
        
        lda_model.fit(tfidf_matrix)
        
        # Prédiction des topics pour chaque document
        topic_distributions = lda_model.transform(tfidf_matrix)
        dominant_topics = np.argmax(topic_distributions, axis=1)
        
        # Ajouter les topics au DataFrame
        df_lang['topic_id'] = dominant_topics
        df_lang['topic_probability'] = np.max(topic_distributions, axis=1)
        
        # Noms des topics basés sur les mots-clés principaux
        feature_names = vectorizer.get_feature_names_out()
        topic_names = self.get_topic_names(lda_model, feature_names, language)
        
        df_lang['topic_name'] = df_lang['topic_id'].map(topic_names)
        
        logger.info(f"[LDA] Analyse terminée pour {language}")
        return df_lang[['id', 'topic_id', 'topic_name', 'topic_probability']], lda_model, topic_names
    
    def get_topic_names(self, lda_model, feature_names, language, n_words=5):
        """Générer des noms de topics basés sur les mots-clés principaux"""
        topic_names = {}
        
        # Mapping des mots-clés vers des catégories pour le français
        keyword_categories_fr = {
            'attent': 'Temps d\'attente',
            'file': 'Temps d\'attente', 
            'rapid': 'Temps d\'attente',
            'lent': 'Temps d\'attente',
            'accueil': 'Service client',
            'personnel': 'Service client',
            'conseil': 'Service client',
            'servic': 'Service client',
            'cart': 'Services bancaires',
            'compt': 'Services bancaires',
            'retrait': 'Services bancaires',
            'depot': 'Services bancaires',
            'horair': 'Horaires',
            'ouvert': 'Horaires',
            'ferm': 'Horaires',
            'parking': 'Accessibilité',
            'proch': 'Accessibilité',
            'transport': 'Accessibilité',
            'prix': 'Tarifs',
            'frais': 'Tarifs',
            'cout': 'Tarifs',
            'secur': 'Sécurité',
            'confianc': 'Sécurité'
        }
        
        for topic_id, topic in enumerate(lda_model.components_):
            # Top mots pour ce topic
            top_words_idx = topic.argsort()[-n_words:][::-1]
            top_words = [feature_names[i] for i in top_words_idx]
            
            # Essayer de mapper vers une catégorie connue
            category_found = False
            for word in top_words:
                for keyword, category in keyword_categories_fr.items():
                    if keyword in word.lower():
                        topic_names[topic_id] = category
                        category_found = True
                        break
                if category_found:
                    break
            
            # Si aucune catégorie trouvée, utiliser les mots-clés principaux
            if not category_found:
                if language == 'fr':
                    topic_names[topic_id] = f"Topic {topic_id + 1}: {', '.join(top_words[:3])}"
                else:
                    topic_names[topic_id] = f"موضوع {topic_id + 1}: {', '.join(top_words[:3])}"
        
        return topic_names
    
    def save_topics_to_db(self, topics_df):
        """Sauvegarder les topics dans une table temporaire"""
        if topics_df is None or len(topics_df) == 0:
            logger.warning("[SAVE] Aucun topic à sauvegarder")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Créer/recréer la table des topics
            cursor.execute("""
                DROP TABLE IF EXISTS temp_review_topics;
                CREATE TABLE temp_review_topics (
                    id INTEGER PRIMARY KEY,
                    topic_id INTEGER,
                    topic_name TEXT,
                    topic_probability DECIMAL(4,3)
                );
            """)
            
            # Insérer les données
            for _, row in topics_df.iterrows():
                cursor.execute("""
                    INSERT INTO temp_review_topics (id, topic_id, topic_name, topic_probability)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        topic_id = EXCLUDED.topic_id,
                        topic_name = EXCLUDED.topic_name,
                        topic_probability = EXCLUDED.topic_probability
                """, (
                    int(row['id']),
                    int(row['topic_id']),
                    str(row['topic_name']),
                    float(row['topic_probability'])
                ))
            
            self.connection.commit()
            logger.info(f"[SAVE] {len(topics_df)} topics sauvegardés dans temp_review_topics")
            return True
            
        except Exception as e:
            logger.error(f"[SAVE] Erreur lors de la sauvegarde: {e}")
            return False
    
    def run_lda_analysis(self):
        """Lancer l'analyse LDA complète"""
        try:
            logger.info("[START] Début de l'analyse LDA")
            
            # Connexion DB
            if not self.connect_db():
                return False
            
            # Récupérer les données
            df = self.get_reviews_data()
            if df is None or len(df) == 0:
                logger.error("[ERROR] Aucune donnée récupérée")
                return False
            
            # Analyser par langue
            all_topics = []
            
            for language in ['fr', 'ar']:
                lang_count = len(df[df['langue'] == language])
                logger.info(f"[LANG] Analyse {language}: {lang_count} avis")
                
                if lang_count >= 20:  # Minimum pour LDA
                    topics_df, model, topic_names = self.perform_lda_analysis(df, language=language)
                    if topics_df is not None:
                        all_topics.append(topics_df)
                        
                        # Afficher les topics trouvés
                        logger.info(f"[TOPICS-{language.upper()}] Topics identifiés:")
                        for topic_id, name in topic_names.items():
                            count = len(topics_df[topics_df['topic_id'] == topic_id])
                            logger.info(f"   Topic {topic_id}: {name} ({count} avis)")
            
            # Combiner tous les topics
            if all_topics:
                combined_topics = pd.concat(all_topics, ignore_index=True)
                
                # Sauvegarder en base
                if self.save_topics_to_db(combined_topics):
                    logger.info(f"[SUCCESS] {len(combined_topics)} topics traités et sauvegardés")
                    return True
            
            logger.warning("[WARNING] Aucun topic généré")
            return False
            
        except Exception as e:
            logger.error(f"[ERROR] Erreur générale: {e}")
            return False
        
        finally:
            if self.connection:
                self.connection.close()
                logger.info("[DB] Connexion fermée")

def main():
    """Fonction principale"""
    print("=" * 80)
    print("EXTRACTION DE TOPICS AVEC LDA - PROJET morocco_banks_reviews")
    print("=" * 80)
    
    extractor = LDATopicExtractor()
    
    if extractor.run_lda_analysis():
        print("\n✅ SUCCÈS: Analyse LDA terminée!")
        print("📊 Topics disponibles dans la table temp_review_topics")
        print("🔧 Utilisez maintenant dbt pour intégrer ces topics")
        return 0
    else:
        print("\n❌ ÉCHEC: Problème lors de l'analyse LDA")
        return 1

if __name__ == "__main__":
    exit(main())