{{ config(materialized='view') }}

with reviews_with_similarity as (
    select *,
        -- Créer une clé de déduplication basée sur le contenu
        md5(banque || ville || nom_agence || avis_normalized) as content_hash,
        
        -- Fenêtre pour identifier les doublons potentiels
        row_number() over (
            partition by banque, ville, nom_agence, avis_normalized 
            order by created_at asc
        ) as duplicate_rank,
        
        -- Identifier les avis très similaires (même banque, même ville, texte similaire)
        row_number() over (
            partition by banque, ville, left(avis_normalized, 100)
            order by created_at asc
        ) as similar_rank
        
    from {{ ref('int_reviews_cleaned') }}
),

-- Marquage des doublons
duplicate_analysis as (
    select *,
        
        -- Marquer les doublons exacts
        case when duplicate_rank > 1 then true else false end as is_exact_duplicate,
        
        -- Marquer les doublons similaires
        case when similar_rank > 1 then true else false end as is_similar_duplicate,
        
        -- Score de confiance pour garder l'avis
        case 
            when duplicate_rank = 1 and similar_rank = 1 then 1.0  -- Original
            when duplicate_rank = 1 and similar_rank > 1 then 0.8  -- Premier d'une série similaire
            when duplicate_rank > 1 and text_quality_score > 0.8 then 0.6  -- Doublon mais bonne qualité
            else 0.3  -- Probablement à supprimer
        end as keep_confidence_score
        
    from reviews_with_similarity
),

-- Sélection finale
final_selection as (
    select *,
        -- Décision finale de conservation
        case 
            when is_exact_duplicate = false then true  -- Garder si pas de doublon exact
            when keep_confidence_score >= 0.8 then true  -- Garder si score élevé
            else false  -- Supprimer
        end as should_keep
        
    from duplicate_analysis
)

select * from final_selection