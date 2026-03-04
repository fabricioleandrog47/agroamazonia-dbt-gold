-- depends_on: {{ ref('cdf_watermark') }}

{{
    config(
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key='LoteNumero'
    )
}}

{% set last_version = 0 %}
{% if is_incremental() %}
    {% set query %}
        SELECT COALESCE(MAX(last_processed_version), 0) 
        FROM {{ ref('cdf_watermark') }} 
        WHERE table_name = 'fato_edi_syngenta_notas_fiscais'
    {% endset %}
    {% set result = run_query(query) %}
    {% if execute and result %}
        {% set last_version = result.columns[0].values()[0] %}
    {% endif %}
{% endif %}

{% if is_incremental() %}

WITH cdf_data AS (
    SELECT
        LOTE_INTERNO as LoteNumero,
        _change_type,
        _commit_version,
        ROW_NUMBER() OVER (PARTITION BY LOTE_INTERNO ORDER BY _commit_version DESC) as rn
    FROM table_changes('cliente_5037.fato_edi_syngenta_notas_fiscais', {{ last_version }})
    WHERE _change_type IN ('insert', 'update_postimage')
        AND LOTE_INTERNO IS NOT NULL
)

SELECT
    LoteNumero,
    current_timestamp() as data_atualizacao
FROM cdf_data
WHERE rn = 1

{% else %}

WITH source_data AS (
    SELECT DISTINCT
        LOTE_INTERNO as LoteNumero
    FROM {{ source('silver', 'fato_edi_syngenta_notas_fiscais') }}
    WHERE LOTE_INTERNO IS NOT NULL
)

SELECT
    LoteNumero,
    current_timestamp() as data_atualizacao
FROM source_data

{% endif %}
