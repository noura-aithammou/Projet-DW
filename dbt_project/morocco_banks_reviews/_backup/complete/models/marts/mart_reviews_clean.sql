{{ config(materialized='table') }}

with final_clean_reviews as (
    select 
        -- Identifiants
        id,
        content_hash,
        
        -- Informations bancaires normalisées
        banque_normalized as banque,
        ville_normalized as ville,
        nom_agence,
        localisation,
        
        -- Avis et métadonnées
        avis_original,
        avis_cleaned,
        avis_normalized,
        langue_detected as langue,
        
        -- Note et catégorisation
        note_numeric as note,
        note_category,
        
        -- Métriques de qualité
        avis_length_original,
        avis_length_cleaned,
        text_length_category,
        text_quality_score,
        
        -- Informations temporelles
        date_avis_parsed as date_avis,
        review_period,
        created_at,
        
        -- Flags de qualité
        case when text_quality_score >= 0.8 then true else false end as is_high_quality,
        case when langue_detected in ('fr', 'ar') then true else false end as is_valid_language,
        case when note_numeric > 0 then true else false end as has_rating,
        
        -- Métriques dérivées
        current_timestamp as processed_at
        
    from {{ ref('int_reviews_normalized') }}
),

-- Statistiques par banque/ville pour enrichissement
bank_city_stats as (
    select 
        banque,
        ville,
        count(*) as total_reviews_in_city,
        avg(note) as avg_rating_in_city,
        count(case when note >= 4 then 1 end) as positive_reviews_in_city
    from final_clean_reviews
    group by banque, ville
),

-- Jointure avec les statistiques
enriched_reviews as (
    select 
        fcr.*,
        bcs.total_reviews_in_city,
        bcs.avg_rating_in_city,
        bcs.positive_reviews_in_city
    from final_clean_reviews fcr
    left join bank_city_stats bcs 
        on fcr.banque = bcs.banque 
        and fcr.ville = bcs.ville
)

select * from enriched_reviews