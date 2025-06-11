{{ config(materialized='table') }}

with location_data as (
    select distinct
        ville as city,
        localisation as address
    from {{ ref('mart_reviews_enriched') }}
    where ville is not null
),

enriched_locations as (
    select 
        row_number() over (order by city) as location_key,
        city,
        
        -- Région basée sur la ville
        case 
            when city in ('Casablanca', 'Rabat', 'Salé') then 'Grand Casablanca-Settat'
            when city in ('Marrakech', 'Agadir') then 'Marrakech-Safi'
            when city in ('Tanger', 'Tétouan') then 'Tanger-Tétouan-Al Hoceima'
            when city in ('Fès', 'Meknès') then 'Fès-Meknès'
            when city in ('Oujda') then 'Oriental'
            when city in ('Laâyoune') then 'Laâyoune-Sakia El Hamra'
            else 'Autre Région'
        end as region,
        
        -- Coordonnées approximatives des centres-villes
        case 
            when city = 'Casablanca' then '33.5731,-7.5898'
            when city = 'Rabat' then '34.0209,-6.8416'
            when city = 'Marrakech' then '31.6295,-7.9811'
            when city = 'Tanger' then '35.7595,-5.8340'
            when city = 'Agadir' then '30.4278,-9.5981'
            when city = 'Fès' then '34.0181,-5.0078'
            when city = 'Meknès' then '33.8935,-5.5473'
            when city = 'Oujda' then '34.6867,-1.9114'
            else null
        end as coordinates,
        
        -- Taille de la ville
        case 
            when city in ('Casablanca', 'Rabat', 'Marrakech') then 'Grande Ville'
            when city in ('Tanger', 'Agadir', 'Fès') then 'Ville Moyenne'
            else 'Petite Ville'
        end as city_size,
        
        current_timestamp as created_at
    from location_data
)

select * from enriched_locations
order by location_key