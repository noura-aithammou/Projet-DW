{{ config(materialized='view') }}

with cleaned_reviews as (
    select 
        id,
        banque,
        ville,
        nom_agence,
        localisation,
        
        -- Conversion de la note en numérique
        cast(
            case 
                when note_raw ~ '^[0-5](\.[0-9])?$' then note_raw
                else '0'
            end as decimal(2,1)
        ) as note_numeric,
        
        -- Nettoyage du texte de l'avis
        avis_raw as avis_original,
        {{ clean_text('avis_raw') }} as avis_cleaned,
        {{ normalize_text('avis_raw') }} as avis_normalized,
        
        -- Métadonnées du texte
        length(avis_raw) as avis_length_original,
        length({{ clean_text('avis_raw') }}) as avis_length_cleaned,
        
        -- Détection de langue
        {{ detect_language('avis_raw') }} as langue_detected,
        
        -- Nettoyage de la date (conversion basique)
        date_avis_raw,
        case 
            when date_avis_raw ~ '^\d{4}-\d{2}-\d{2}$' then date_avis_raw::date
            when date_avis_raw ~ '^\d{1,2}/\d{1,2}/\d{4}$' then 
                to_date(date_avis_raw, 'DD/MM/YYYY')
            else null
        end as date_avis_parsed,
        
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