{%- macro get_lda_topic(review_id) -%}
    -- Récupérer le topic LDA depuis la table temporaire
    (
        SELECT topic_name 
        FROM temp_review_topics 
        WHERE id = {{ review_id }}
        LIMIT 1
    )
{%- endmacro -%}