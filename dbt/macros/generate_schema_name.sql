-- By default dbt concatenates the profile schema with the model's custom schema, so
-- gold would land in silver_gold. This makes +schema absolute instead.
-- The ci target is the exception: it prefixes, so a pull request builds into ci_silver
-- and ci_gold instead of overwriting the real ones.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'ci' -%}
        ci_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
