{{ config(materialized='table') }}

with topic_data as (
    select distinct
        topic as topic_name
    from {{ ref('mart_reviews_enriched') }}
    where topic is not null
),

enriched_topics as (
    select 
        row_number() over (order by topic_name) as topic_key,
        topic_name,
        
        -- Catégorisation des topics
        case 
            when topic_name like '%Service%' then 'Expérience Client'
            when topic_name like '%bancaire%' then 'Produits & Services'
            when topic_name like '%attente%' then 'Opérations'
            when topic_name like '%Horaire%' then 'Accessibilité'
            when topic_name like '%Tarif%' then 'Prix & Frais'
            when topic_name like '%Accessibilité%' then 'Infrastructure'
            else 'Général'
        end as category,
        
        -- Description du topic
        case 
            when topic_name = 'Service Client' then 'Qualité de l''accueil et du service'
            when topic_name = 'Services bancaires' then 'Produits et services bancaires'
            when topic_name = 'Temps d''attente' then 'Durée d''attente et files'
            when topic_name = 'Horaires' then 'Heures d''ouverture et disponibilité'
            when topic_name = 'Tarifs' then 'Prix et frais bancaires'
            when topic_name = 'Accessibilité' then 'Facilité d''accès et transport'
            when topic_name = 'Expérience générale' then 'Impression générale de l''expérience'
            else topic_name
        end as description,
        
        current_timestamp as created_at
    from topic_data
)

select * from enriched_topics
order by topic_key