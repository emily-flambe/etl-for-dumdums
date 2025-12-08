{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Override dbt's default schema naming to use ONLY the custom schema name
        when specified, without prepending the default schema.

        This gives us clean dataset names like 'linear', 'github', 'hacker_news'
        instead of 'dbt_default_linear', 'dbt_default_github', etc.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
