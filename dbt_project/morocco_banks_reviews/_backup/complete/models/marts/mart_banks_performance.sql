{{ config(materialized='table') }}

with bank_metrics as (
    select 
        banque as bank_name,
        ville as city,
        
        -- Métriques de base
        count(*) as total_reviews,
        count(distinct nom_agence) as total_agencies,
        avg(rating) as avg_rating,
        avg(composite_score) as avg_composite_score,
        
        -- Métriques de sentiment
        sum(is_positive) as positive_reviews,
        sum(is_negative) as negative_reviews,
        sum(is_neutral) as neutral_reviews,
        
        round(sum(is_positive) * 100.0 / count(*), 2) as positive_percentage,
        round(sum(is_negative) * 100.0 / count(*), 2) as negative_percentage,
        
        -- Métriques temporelles
        min(review_date) as first_review_date,
        max(review_date) as last_review_date,
        
        -- Métriques de langue
        sum(case when language = 'fr' then 1 else 0 end) as french_reviews,
        sum(case when language = 'ar' then 1 else 0 end) as arabic_reviews,
        
        current_timestamp as calculated_at
        
    from {{ ref('mart_reviews_final') }}
    group by banque, ville
),

ranked_banks as (
    select 
        *,
        -- Classement par ville
        row_number() over (partition by city order by avg_composite_score desc) as city_rank,
        -- Classement global
        row_number() over (order by avg_composite_score desc) as global_rank
    from bank_metrics
)

select * from ranked_banks