{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}          {# fallback: usa el schema del profile #}
    {%- else -%}
        {{ custom_schema_name | trim }}   {# usa EXACTAMENTE lo que pongas en config/yml #}
    {%- endif -%}
{%- endmacro %}