{% macro log_effective_conf() %}
  {% if execute %}
    {% set keys = [
      'spark.hadoop.fs.s3a.endpoint',
      'spark.hadoop.fs.s3a.path.style.access',
      'spark.hadoop.fs.s3a.connection.ssl.enabled',
      'spark.sql.extensions'
    ] %}

    {% for key in keys %}
      {% set res = run_query('SET ' ~ key) %}
      {% if res is not none and res.rows|length > 0 %}
        {# Spark may return one column like "key=value" or two columns key/value #}
        {% set row = res.rows[0] %}
        {% if row|length == 1 %}
          {% do log(row[0], info=true) %}
        {% else %}
          {% do log(row[0] ~ '=' ~ row[1], info=true) %}
        {% endif %}
      {% else %}
        {% do log(key ~ '=MISSING', info=true) %}
      {% endif %}
    {% endfor %}
  {% endif %}
  {% do log('CATALOG_WAREHOUSE_PATH is ' ~ env_var('CATALOG_WAREHOUSE_PATH', 'MISSING'), info=true) %}
  {% do log('STORAGE_PATH_STYLE_ACCESS is ' ~ env_var('STORAGE_PATH_STYLE_ACCESS', 'MISSING'), info=true) %}
{% endmacro %}