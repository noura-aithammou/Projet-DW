{%- macro normalize_text(column_name) -%}
    -- Macro pour normaliser le texte (supprimer ponctuation excessive, mots vides basiques)
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    {{ clean_text(column_name) }},
                    '\.{2,}', '.', 'g'  -- Remplacer points multiples
                ),
                '!{2,}', '!', 'g'  -- Remplacer exclamations multiples
            ),
            '\?{2,}', '?', 'g'  -- Remplacer interrogations multiples
        )
    )
{%- endmacro -%}