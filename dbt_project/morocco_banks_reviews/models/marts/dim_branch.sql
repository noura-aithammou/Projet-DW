{{ config(materialized='table') }}

with branch_data as (
    select distinct
        nom_agence as branch_name,
        banque as bank_name,
        ville as city
    from {{ ref('mart_reviews_enriched') }}
    where nom_agence is not null
),

enriched_branches as (
    select 
        row_number() over (order by bank_name, city, branch_name) as branch_key,
        branch_name,
        bank_name,
        city,
        
        -- Type d'agence basé sur le nom
        case 
            when lower(branch_name) like '%centre%' or lower(branch_name) like '%principal%' then 'Agence Principale'
            when lower(branch_name) like '%mall%' or lower(branch_name) like '%centre commercial%' then 'Agence Centre Commercial'
            when lower(branch_name) like '%aéroport%' or lower(branch_name) like '%airport%' then 'Agence Aéroport'
            when lower(branch_name) like '%université%' or lower(branch_name) like '%campus%' then 'Agence Universitaire'
            else 'Agence Standard'
        end as branch_type,
        
        -- Services supposés (basés sur le type d'agence)
        case 
            when lower(branch_name) like '%centre%' or lower(branch_name) like '%principal%' 
                then 'Services complets, Conseillers, Coffres-forts'
            when lower(branch_name) like '%mall%' or lower(branch_name) like '%centre commercial%' 
                then 'Services de base, Distributeurs, Guichets'
            when lower(branch_name) like '%aéroport%' or lower(branch_name) like '%airport%' 
                then 'Change, Carte de crédit, Services voyage'
            else 'Services standards'
        end as services,
        
        current_timestamp as created_at
    from branch_data
)

select * from enriched_branches
order by branch_key