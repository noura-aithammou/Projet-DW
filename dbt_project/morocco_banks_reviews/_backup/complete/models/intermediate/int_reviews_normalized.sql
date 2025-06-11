{{ config(materialized='view') }}

with normalized_reviews as (
    select 
        id,
        banque,
        ville,
        nom_agence,
        localisation,
        note_numeric,
        avis_original,
        avis_cleaned,
        avis_normalized,
        avis_length_original,
        avis_length_cleaned,
        langue_detected,
        date_avis_raw,
        date_avis_parsed,
        text_quality_score,
        text_length_category,
        content_hash,
        is_exact_duplicate,
        is_similar_duplicate,
        keep_confidence_score,
        should_keep,
        created_at,
        
        -- Normalisation des noms de banques
        case 
            when lower(banque) like '%cih%' then 'CIH Bank'
            when lower(banque) like '%attijariwafa%' then 'Attijariwafa Bank'
            when lower(banque) like '%bmce%' then 'BMCE Bank'
            when lower(banque) like '%barid%' then 'Al Barid Bank'
            when lower(banque) like '%populaire%' then 'Banque Populaire'
            when lower(banque) like '%agricole%' then 'Crédit Agricole du Maroc'
            when lower(banque) like '%société générale%' then 'Société Générale Maroc'
            when lower(banque) like '%bmci%' then 'BMCI'
            else banque
        end as banque_normalized,
        
        -- Normalisation des villes
        case 
            when lower(ville) like '%casa%' or lower(ville) like '%casablanca%' then 'Casablanca'
            when lower(ville) like '%rabat%' then 'Rabat'
            when lower(ville) like '%marrakech%' or lower(ville) like '%marrakesh%' then 'Marrakech'
            when lower(ville) like '%tanger%' or lower(ville) like '%tangier%' then 'Tanger'
            when lower(ville) like '%agadir%' then 'Agadir'
            when lower(ville) like '%fès%' or lower(ville) like '%fes%' then 'Fès'
            else ville
        end as ville_normalized,
        
        -- Catégorisation de la note
        case 
            when note_numeric >= 4.0 then 'Excellent'
            when note_numeric >= 3.0 then 'Bon'
            when note_numeric >= 2.0 then 'Moyen'
            when note_numeric >= 1.0 then 'Mauvais'
            else 'Non noté'
        end as note_category,
        
        -- Extraction de la période (mois/année si possible)
        case 
            when date_avis_parsed is not null then to_char(date_avis_parsed, 'YYYY-MM')
            else null
        end as review_period
        
    from {{ ref('int_reviews_deduplicated') }}
    where should_keep = true  -- Ne garder que les avis valides
)

select * from normalized_reviews