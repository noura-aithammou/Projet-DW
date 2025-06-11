{{ config(materialized='table') }}

with bank_data as (
    select distinct
        banque as bank_name
    from {{ ref('mart_reviews_enriched') }}
    where banque is not null
),

enriched_banks as (
    select 
        row_number() over (order by bank_name) as bank_key,
        bank_name,
        
        -- Classification des banques
        case 
            when bank_name in ('Attijariwafa Bank', 'BMCE Bank', 'Banque Populaire') then 'Banque Commerciale'
            when bank_name in ('CIH Bank', 'Crédit Agricole du Maroc') then 'Banque d''Investissement'
            when bank_name = 'Al Barid Bank' then 'Banque Postale'
            when bank_name in ('Société Générale Maroc', 'BMCI') then 'Banque Internationale'
            else 'Autre'
        end as bank_type,
        
        -- Année de fondation approximative
        case 
            when bank_name = 'Attijariwafa Bank' then 1904
            when bank_name = 'BMCE Bank' then 1959
            when bank_name = 'Banque Populaire' then 1961
            when bank_name = 'CIH Bank' then 1920
            when bank_name = 'Crédit Agricole du Maroc' then 1961
            when bank_name = 'Al Barid Bank' then 2009
            when bank_name = 'Société Générale Maroc' then 1913
            when bank_name = 'BMCI' then 1943
            else null
        end as founded_year,
        
        current_timestamp as created_at
    from bank_data
)

select * from enriched_banks
order by bank_key