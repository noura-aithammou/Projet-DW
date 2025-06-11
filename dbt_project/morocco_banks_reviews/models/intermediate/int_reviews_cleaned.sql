{{ config(materialized='view') }}

with cleaned_reviews as (
    select 
        id,
        banque,
        ville,
        nom_agence,
        localisation,
        
        -- Conversion de la note en numérique
        case 
            when note_raw ~ '^[0-5](\.[0-9])?$' then cast(note_raw as decimal(2,1))
            when note_raw ~ '^[0-5]$' then cast(note_raw as decimal(2,1))
            else 0.0
        end as note_numeric,
        
        -- Nettoyage du texte de l'avis
        avis_raw as avis_original,
        {{ clean_text('avis_raw') }} as avis_cleaned,
        
        -- Métadonnées du texte
        length(avis_raw) as avis_length_original,
        length({{ clean_text('avis_raw') }}) as avis_length_cleaned,
        
        -- Détection basique de langue
        case 
            when regexp_count(avis_raw, '[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDCF\uFDF0-\uFDFF\uFE70-\uFEFF]') > length(avis_raw) * 0.3 
            then 'ar'
            when regexp_count(avis_raw, '[a-zA-ZàâäéèêëïîôöùûüÿçÀÂÄÉÈÊËÏÎÔÖÙÛÜŸÇ]') > length(avis_raw) * 0.5 
            then 'fr'
            else 'mixed'
        end as langue_detected,
        
        -- Nettoyage de la date
        date_avis_raw,
        created_at
        
    from {{ ref('stg_raw_reviews') }}
),

-- Ajout de métriques de qualité
quality_metrics as (
    select *,
        
        -- Score de qualité du texte (0-1)
        case 
            when avis_length_cleaned >= 20 and langue_detected in ('fr', 'ar') then 1.0
            when avis_length_cleaned >= 10 and langue_detected in ('fr', 'ar') then 0.8
            when avis_length_cleaned >= 5 then 0.6
            else 0.3
        end as text_quality_score,
        
        -- Catégories de longueur
        case 
            when avis_length_cleaned <= 10 then 'tres_court'
            when avis_length_cleaned <= 50 then 'court'
            when avis_length_cleaned <= 150 then 'moyen'
            when avis_length_cleaned <= 300 then 'long'
            else 'tres_long'
        end as text_length_category
        
    from cleaned_reviews
)

select * from quality_metrics