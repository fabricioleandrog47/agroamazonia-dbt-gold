-- depends_on: {{ ref('cdf_watermark') }}

{{
    config(
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key='VendedorCodigo'
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
        COD_VENDEDOR as VendedorCodigo,
        NOME_VENDEDOR as VendedorDescricao,
        CPF_VENDEDOR as VendedorCPF,
        _change_type,
        _commit_version,
        _commit_timestamp,
        ROW_NUMBER() OVER (PARTITION BY COD_VENDEDOR ORDER BY _commit_version DESC) as rn
    FROM table_changes('delta.`s3a://brid-silver/5037/FATO_EDI_SYNGENTA_NOTAS_FISCAIS`', {{ last_version }})
    WHERE _change_type IN ('insert', 'update_postimage')
        AND COD_VENDEDOR IS NOT NULL
)

SELECT
    VendedorCodigo,
    VendedorDescricao,
    VendedorCPF,
    _commit_timestamp as data_atualizacao
FROM cdf_data
WHERE rn = 1

{% else %}

SELECT
    COD_VENDEDOR as VendedorCodigo,
    NOME_VENDEDOR as VendedorDescricao,
    CPF_VENDEDOR as VendedorCPF,
    current_timestamp() as data_atualizacao
FROM delta.`s3a://brid-silver/5037/FATO_EDI_SYNGENTA_NOTAS_FISCAIS`
WHERE COD_VENDEDOR IS NOT NULL

{% endif %}
