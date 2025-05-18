-- put this in macros/get_custom_schema.sql

{% macro generate_schema_name(custom_schema_name, node) %}
    {% if target.name == 'dev' %}
        {# In production, use custom schema if provided #}
        {% if custom_schema_name is not none %}
            {{ custom_schema_name | trim }}
        {% else %}
            {{ target.schema }}
        {% endif %}
    {% else %}
        {# In other environments, always use the target schema #}
        {{ target.schema }}
    {% endif %}
{% endmacro %}