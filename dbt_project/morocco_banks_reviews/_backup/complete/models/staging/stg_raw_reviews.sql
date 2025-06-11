{{ config(materialized='view') }}

with source_data as (
    select 
        id,
        banque,
        ville,
        nom_agence,
        localisation,
        note,
        avis,
        date_avis,
        created_at
    from {{ source('raw_data', 'raw_reviews') }}
),

-- Nettoyage initial et validation
cleaned_source as (
    select 
        id,
        
        -- Nettoyage des champs texte
        {{ handle_missing_values('banque', 'Unknown') }} as banque,
        {{ handle_missing_values('ville', 'Unknown') }} as ville,
        {{ handle_missing_values('nom_agence', 'Unknown') }} as nom_agence,
        {{ handle_missing_values('localisation', 'Unknown') }} as localisation,
        
        -- Nettoyage de la note
        case 
            when note is null or trim(note) = '' then '0'
            when note ~ '^[0-5](\.[0-9])?$' then note
            else regexp_replace(note, '[^0-9.]', '', 'g')
        end as note_raw,
        
        -- Nettoyage de l'avis
        {{ handle_missing_values('avis', '') }} as avis_raw,
        
        -- Nettoyage de la date
        {{ handle_missing_values('date_avis', '') }} as date_avis_raw,
        
        created_at
        
    from source_data
),

-- Filtrage des données valides
filtered_data as (
    select *
    from cleaned_source
    where 
        avis_raw != ''  -- Exclure les avis vides
        and length(avis_raw) >= {{ var('min_review_length') }}  -- Minimum 5 caractères
        and length(avis_raw) <= {{ var('max_review_length') }}  -- Maximum 2000 caractères
        and banque != 'Unknown'  -- Exclure les banques inconnues
)

select * from filtered_data