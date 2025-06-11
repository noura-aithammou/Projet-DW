{%- macro analyze_sentiment(text_column) -%}
    -- Analyse de sentiment basée sur des mots-clés en français et arabe
    case 
        -- Sentiment POSITIF
        when lower({{ text_column }}) ~ '(excellent|formidable|parfait|superbe|magnifique|fantastique|merveilleux|génial|top|très bien|bien|bon|bonne|rapide|efficace|professionnel|aimable|courtois|satisfait|content|recommande|رائع|ممتاز|جيد|سريع|مهني|راض|أنصح)' 
        then 'Positif'
        
        -- Sentiment NÉGATIF  
        when lower({{ text_column }}) ~ '(mauvais|horrible|nul|catastrophique|décevant|lent|inefficace|impoli|malpoli|pas bien|très mal|mal|problème|souci|attente|queue|retard|fermé|indisponible|غير جيد|سيء|بطيء|مشكلة|انتظار|مغلق)' 
        then 'Negatif'
        
        -- Sentiment basé sur la note si disponible
        when {{ text_column }} ~ '[5].*étoiles?' or {{ text_column }} ~ '5/5' then 'Positif'
        when {{ text_column }} ~ '[4].*étoiles?' or {{ text_column }} ~ '4/5' then 'Positif'
        when {{ text_column }} ~ '[1-2].*étoiles?' or {{ text_column }} ~ '[1-2]/5' then 'Negatif'
        
        -- Sentiment NEUTRE par défaut
        else 'Neutre'
    end
{%- endmacro -%}