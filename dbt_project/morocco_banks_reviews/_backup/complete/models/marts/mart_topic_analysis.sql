{{ config(materialized='table') }}

with topic_analysis as (
    select 
        topic_category,
        banque as bank_name,
        
        -- Métriques par sujet
        count(*) as review_count,
        avg(rating) as avg_rating,
        avg(topic_confidence) as avg_confidence,
        
        -- Sentiment par sujet
        sum(is_positive) as positive_count,
        sum(is_negative) as negative_count,
        sum(is_neutral) as neutral_count,
        
        round(sum(is_positive) * 100.0 / count(*), 2) as positive_rate,
        round(sum(is_negative) * 100.0 / count(*), 2) as negative_rate,
        
        -- Langues par sujet
        sum(case when language = 'fr' then 1 else 0 end) as french_count,
        sum(case when language = 'ar' then 1 else 0 end) as arabic_count
        
    from {{ ref('mart_reviews_final') }}
    group by topic_category, banque
),

topic_summary as (
    select 
        topic_category,
        sum(review_count) as total_reviews,
        avg(avg_rating) as overall_avg_rating,
        sum(positive_count) as total_positive,
        sum(negative_count) as total_negative,
        round(sum(positive_count) * 100.0 / sum(review_count), 2) as overall_positive_rate
    from topic_analysis
    group by topic_category
)

select 
    ta.*,
    ts.overall_positive_rate as topic_benchmark
from topic_analysis ta
left join topic_summary ts on ta.topic_category = ts.topic_category