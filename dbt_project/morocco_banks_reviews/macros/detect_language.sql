{%- macro detect_language(text_column) -%}
    -- Détection basique de langue basée sur les caractères
    case 
        when regexp_count({{ text_column }}, '[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDCF\uFDF0-\uFDFF\uFE70-\uFEFF]') > length({{ text_column }}) * 0.3 
        then 'ar'
        when regexp_count({{ text_column }}, '[a-zA-ZàâäéèêëïîôöùûüÿçÀÂÄÉÈÊËÏÎÔÖÙÛÜŸÇ]') > length({{ text_column }}) * 0.5 
        then 'fr'
        else 'mixed'
    end
{%- endmacro -%}