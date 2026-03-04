-- depends_on: {{ ref('cdf_watermark') }}

{{
    config(
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['EstoqueFilial', 'EstoqueProduto', 'EstoqueFabricante']
    )
}}

{% set last_version = 0 %}
{% if is_incremental() %}
    {% set query %}
        SELECT COALESCE(MAX(last_processed_version), 0) 
        FROM {{ ref('cdf_watermark') }} 
        WHERE table_name = 'fato_edi_syngenta_estoque'
    {% endset %}
    {% set result = run_query(query) %}
    {% if execute and result %}
        {% set last_version = result.columns[0].values()[0] %}
    {% endif %}
{% endif %}

{% if is_incremental() %}

WITH cdf_data AS (
    SELECT
        COD_FILIAL as EstoqueFilial,
        COD_PRODUTO as EstoqueProduto,
        COD_FABRICANTE as EstoqueFabricante,
        QTDE_SALDO_EMBALAGEM as EstoqueSaldo,
        _change_type,
        _commit_version,
        ROW_NUMBER() OVER (PARTITION BY COD_FILIAL, COD_PRODUTO, COD_FABRICANTE ORDER BY _commit_version DESC) as rn
    FROM table_changes('delta.`s3a://brid-silver/5037/FATO_EDI_SYNGENTA_ESTOQUE`', {{ last_version }})
    WHERE _change_type IN ('insert', 'update_postimage')
)

SELECT
    EstoqueFilial,
    EstoqueProduto,
    EstoqueFabricante,
    EstoqueSaldo,
    current_timestamp() as data_atualizacao
FROM cdf_data
WHERE rn = 1

{% else %}

SELECT
    COD_FILIAL as EstoqueFilial,
    COD_PRODUTO as EstoqueProduto,
    COD_FABRICANTE as EstoqueFabricante,
    QTDE_SALDO_EMBALAGEM as EstoqueSaldo,
    current_timestamp() as data_atualizacao
FROM delta.`s3a://brid-silver/5037/FATO_EDI_SYNGENTA_ESTOQUE`

{% endif %}
