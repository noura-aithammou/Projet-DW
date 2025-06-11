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

-- Nettoyage basique et validation
cleaned_source as (
    select 
        id,
        
        -- Nettoyage des champs texte
        case 
            when banque is null or trim(banque) = '' then 'Unknown'
            else trim(banque)
        end as banque,
        
        case 
            when ville is null or trim(ville) = '' then 'Unknown'
            else trim(ville)
        end as ville,
        
        case 
            when nom_agence is null or trim(nom_agence) = '' then 'Unknown'
            else trim(nom_agence)
        end as nom_agence,
        
        case 
            when localisation is null or trim(localisation) = '' then 'Unknown'
            else trim(localisation)
        end as localisation,
        
        -- Nettoyage de la note (renommé en note_raw pour cohérence)
        case 
            when note is null or trim(note) = '' then '0'
            else trim(note)
        end as note_raw,
        
        -- Nettoyage de l'avis (renommé en avis_raw pour cohérence)
        case 
            when avis is null or trim(avis) = '' then ''
            else trim(avis)
        end as avis_raw,
        
        -- Nettoyage de la date (renommé en date_avis_raw pour cohérence)
        case 
            when date_avis is null or trim(date_avis) = '' then ''
            else trim(date_avis)
        end as date_avis_raw,
        
        created_at
        
    from source_data
),

-- Filtrage des données valides
filtered_data as (
    select *
    from cleaned_source
    where 
        avis_raw != ''  -- Exclure les avis vides
        and length(avis_raw) >= 5  -- Minimum 5 caractères
        and length(avis_raw) <= 2000  -- Maximum 2000 caractères
        and banque != 'Unknown'  -- Exclure les banques inconnues
)

select * from filtered_data