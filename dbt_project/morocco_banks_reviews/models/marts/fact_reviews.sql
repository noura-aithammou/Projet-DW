{{ config(materialized='table') }}

with enriched_reviews as (
    select 
        r.id as review_id,
        r.note as rating,
        r.avis as review_text,
        r.langue as language,
        r.date_avis,
        r.sentiment,
        r.topic,
        r.banque as bank_name,
        r.nom_agence as branch_name,
        r.ville as city
    from {{ ref('mart_reviews_enriched') }} r
),

-- Jointures avec les dimensions pour récupérer les clés
fact_table as (
    select 
        er.review_id,
        
        -- Clés étrangères
        coalesce(db.bank_key, -1) as bank_key,
        coalesce(dbr.branch_key, -1) as branch_key,
        coalesce(dl.location_key, -1) as location_key,
        coalesce(ds.sentiment_key, -1) as sentiment_key,
        coalesce(dt.topic_key, -1) as topic_key,
        
        -- Métriques et faits
        er.rating,
        er.review_text,
        er.language,
        
        -- Conversion de date
        case 
            when er.date_avis ~ '^\d{1,2}/\d{1,2}/\d{4}$' then 
                to_date(er.date_avis, 'DD/MM/YYYY')
            else null
        end as review_date,
        
        current_timestamp as created_at,
        
        -- Métriques calculées
        case when er.rating >= 4 then 1 else 0 end as is_positive_rating,
        case when er.rating <= 2 then 1 else 0 end as is_negative_rating,
        case when er.sentiment = 'Positif' then 1 else 0 end as is_positive_sentiment,
        case when er.sentiment = 'Negatif' then 1 else 0 end as is_negative_sentiment,
        
        length(er.review_text) as review_length,
        
        -- Période d'analyse
        case 
            when er.date_avis ~ '^\d{1,2}/\d{1,2}/\d{4}$' then 
                extract(year from to_date(er.date_avis, 'DD/MM/YYYY'))
            else extract(year from current_timestamp)
        end as review_year,
        
        case 
            when er.date_avis ~ '^\d{1,2}/\d{1,2}/\d{4}$' then 
                extract(month from to_date(er.date_avis, 'DD/MM/YYYY'))
            else extract(month from current_timestamp)
        end as review_month
        
    from enriched_reviews er
    
    -- Jointures avec les dimensions
    left join {{ ref('dim_bank') }} db 
        on er.bank_name = db.bank_name
        
    left join {{ ref('dim_branch') }} dbr 
        on er.branch_name = dbr.branch_name 
        and er.bank_name = dbr.bank_name
        
    left join {{ ref('dim_location') }} dl 
        on er.city = dl.city
        
    left join {{ ref('dim_sentiment') }} ds 
        on er.sentiment = ds.sentiment
        
    left join {{ ref('dim_topic') }} dt 
        on er.topic = dt.topic_name
)

select * from fact_table
order by review_id