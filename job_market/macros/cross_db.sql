{#
  Tiny wrappers for the two places DuckDB and BigQuery SQL differ in this
  project. Models call these, so switching warehouses doesn't touch model SQL.
#}

{% macro strip_html(col) %}
  {{ return(adapter.dispatch('strip_html')(col)) }}
{% endmacro %}

{% macro default__strip_html(col) %}
  regexp_replace({{ col }}, '<[^>]+>', '', 'g')
{% endmacro %}

{% macro bigquery__strip_html(col) %}
  regexp_replace({{ col }}, r'<[^>]+>', '')
{% endmacro %}


{% macro regex_contains(col, pattern) %}
  {{ return(adapter.dispatch('regex_contains')(col, pattern)) }}
{% endmacro %}

{% macro default__regex_contains(col, pattern) %}
  regexp_matches({{ col }}, {{ pattern }})
{% endmacro %}

{% macro bigquery__regex_contains(col, pattern) %}
  regexp_contains({{ col }}, {{ pattern }})
{% endmacro %}
