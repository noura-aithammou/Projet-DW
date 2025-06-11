{{ config(materialized='table') }}

with base_reviews as (
    select 
        id,
        banque,
        ville,
        nom_agence,
        localisation,
        note_numeric as note,
        avis_cleaned as avis,
        langue_detected as langue,
        date_avis_raw,
        created_at
    from {{ ref('int_reviews_deduplicated') }}
    where should_keep = true  -- Ne garder que les avis valides
),

-- Jointure avec les topics LDA
reviews_with_lda as (
    select 
        br.*,
        coalesce(trt.topic_name, 'Expérience générale') as lda_topic
    from base_reviews br
    left join temp_review_topics trt on br.id = trt.id
),

-- Enrichissement avec sentiment et formatage
enriched_reviews as (
    select 
        id,
        
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
        end as banque,
        
        -- Normalisation des villes
        case 
            when lower(ville) like '%casa%' or lower(ville) like '%casablanca%' then 'Casablanca'
            when lower(ville) like '%rabat%' then 'Rabat'
            when lower(ville) like '%marrakech%' or lower(ville) like '%marrakesh%' then 'Marrakech'
            when lower(ville) like '%tanger%' or lower(ville) like '%tangier%' then 'Tanger'
            when lower(ville) like '%agadir%' then 'Agadir'
            when lower(ville) like '%fès%' or lower(ville) like '%fes%' then 'Fès'
            else ville
        end as ville,
        
        nom_agence,
        localisation,
        
        -- Note arrondie
        round(note, 1) as note,
        
        -- Avis nettoyé
        avis,
        
        -- Langue détectée
        langue,
        
        -- Date formatée en DD/MM/YYYY
        {{ format_date_dmy('date_avis_raw') }} as date_avis,
        
        -- ANALYSE DE SENTIMENT
        {{ analyze_sentiment('avis') }} as sentiment,
        
        -- TOPIC LDA (plus sophistiqué que les mots-clés)
        lda_topic as topic
        
    from reviews_with_lda
),

-- Validation et nettoyage final
final_reviews as (
    select 
        id,
        banque,
        ville,
        nom_agence,
        localisation,
        note,
        avis,
        langue,
        coalesce(date_avis, 'Date inconnue') as date_avis,
        sentiment,
        topic
        
    from enriched_reviews
    where 
        avis is not null 
        and trim(avis) != ''
        and length(trim(avis)) >= 5
        and banque is not null
)

select * from final_reviews
order by id