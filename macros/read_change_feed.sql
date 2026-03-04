{% macro read_change_feed(table_path, starting_version) %}
    (
        SELECT *
        FROM delta.`{{ table_path }}`
        VERSION AS OF {{ starting_version }}
        OPTIONS (
            readChangeFeed = 'true',
            startingVersion = {{ starting_version }}
        )
    )
{% endmacro %}
