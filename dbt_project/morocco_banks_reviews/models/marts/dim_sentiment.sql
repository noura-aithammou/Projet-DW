{{ config(materialized='table') }}

with sentiment_mapping as (
    select * from (
        values 
            (1, 'Positif', '4.0-5.0', 'Avis positifs et satisfaisants'),
            (2, 'Neutre', '2.5-3.9', 'Avis neutres ou mitigés'),
            (3, 'Negatif', '0.0-2.4', 'Avis négatifs et mécontents')
    ) as t(sentiment_key, sentiment, score_range, description)
)

select 
    sentiment_key,
    sentiment,
    score_range,
    description,
    current_timestamp as created_at
from sentiment_mapping