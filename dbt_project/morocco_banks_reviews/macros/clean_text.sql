{%- macro clean_text(column_name) -%}
    -- Macro pour nettoyer le texte des avis
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    lower({{ column_name }}),
                    'https?://[^\s]+', '', 'g'  -- Supprimer URLs
                ),
                '[^\w\s\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDCF\uFDF0-\uFDFF\uFE70-\uFEFFàâäéèêëïîôöùûüÿçÀÂÄÉÈÊËÏÎÔÖÙÛÜŸÇ'']', ' ', 'g'  -- Garder lettres, espaces, caractères arabes et français
            ),
            '\s+', ' ', 'g'  -- Remplacer espaces multiples par un seul
        )
    )
{%- endmacro -%}