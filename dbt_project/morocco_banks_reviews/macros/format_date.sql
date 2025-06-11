{%- macro format_date_dmy(date_column) -%}
    -- Conversion de dates en format DD/MM/YYYY
    case 
        -- Si c'est déjà une date valide
        when {{ date_column }} ~ '^\d{4}-\d{2}-\d{2}$' then 
            to_char({{ date_column }}::date, 'DD/MM/YYYY')
        
        -- Si c'est au format DD/MM/YYYY déjà
        when {{ date_column }} ~ '^\d{1,2}/\d{1,2}/\d{4}$' then 
            to_char(to_date({{ date_column }}, 'DD/MM/YYYY'), 'DD/MM/YYYY')
        
        -- Si c'est au format MM/DD/YYYY (format US)
        when {{ date_column }} ~ '^\d{1,2}/\d{1,2}/\d{4}$' and 
             split_part({{ date_column }}, '/', 1)::int > 12 then
            to_char(to_date({{ date_column }}, 'DD/MM/YYYY'), 'DD/MM/YYYY')
        
        -- Si c'est une date relative (il y a X jours, X mois, etc.)
        when lower({{ date_column }}) ~ 'il y a.*jour' then 
            to_char(current_date - interval '1 day', 'DD/MM/YYYY')
        when lower({{ date_column }}) ~ 'il y a.*semaine' then 
            to_char(current_date - interval '1 week', 'DD/MM/YYYY')
        when lower({{ date_column }}) ~ 'il y a.*mois' then 
            to_char(current_date - interval '1 month', 'DD/MM/YYYY')
        
        -- Tentative de parsing avec différents formats
        when {{ date_column }} is not null and trim({{ date_column }}) != '' then
            case 
                when {{ date_column }} ~ '\d{1,2}-\d{1,2}-\d{4}' then
                    to_char(to_date({{ date_column }}, 'DD-MM-YYYY'), 'DD/MM/YYYY')
                else null
            end
        
        else null
    end
{%- endmacro -%}