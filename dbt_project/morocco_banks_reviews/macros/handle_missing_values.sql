{%- macro handle_missing_values(column_name, default_value='') -%}
    -- Macro pour gérer les valeurs manquantes
    case 
        when {{ column_name }} is null then '{{ default_value }}'
        when trim({{ column_name }}) = '' then '{{ default_value }}'
        when trim({{ column_name }}) = 'nan' then '{{ default_value }}'
        when trim({{ column_name }}) = 'null' then '{{ default_value }}'
        else {{ column_name }}
    end
{%- endmacro -%}