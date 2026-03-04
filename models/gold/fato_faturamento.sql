-- depends_on: {{ ref('cdf_watermark') }}

{{
    config(
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key='FaturamentoChaveNF'
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
        CHAVE_NFE as FaturamentoChaveNF,
        COD_FILIAL as FaturamentoFilial,
        COD_FABRICANTE as FaturamentoFabricante,
        NR_NF as FaturamentoNumeroNF,
        SERIE_NF as FaturamentoSerieNF,
        CPF_CNPJ_CLIFOR as FaturamentoCliente,
        COD_CFOP as FaturamentoCFOP,
        DATA_EMISSAO as FaturamentoData,
        COD_PRODUTO as FaturamentoProduto,
        COD_CFOP as FaturamentoOperacao,
        COD_IBGE as FaturamentoMunicipioIBGE,
        COD_VENDEDOR as FaturamentoVendedor,
        VALOR_LIQUIDO as FaturamentoValorItem,
        QUANTIDADE_LT_KG as FaturamentoVolumeItem,
        VALOR_TOTAL as FaturamentoValorBrutoItem,
        TIPO_NF as FaturamentoCancelado,
        UNIDADE_MEDIDA as FaturamentoUnidadeMedida,
        FATOR_CONVERSAO as FaturamentoValorConversao,
        LOTE_INTERNO as FaturamentoLote,
        COD_TRATAMENTO as FaturamentoTratamento,
        PESO_KG_SEMENTE as FaturamentoVolumeItemKG,
        _change_type,
        _commit_version,
        ROW_NUMBER() OVER (PARTITION BY CHAVE_NFE ORDER BY _commit_version DESC) as rn
    FROM table_changes('cliente_5037.fato_edi_syngenta_notas_fiscais', {{ last_version }})
    WHERE _change_type IN ('insert', 'update_postimage')
        AND CHAVE_NFE IS NOT NULL
)

SELECT
    FaturamentoChaveNF,
    FaturamentoFilial,
    FaturamentoFabricante,
    FaturamentoNumeroNF,
    FaturamentoSerieNF,
    FaturamentoCliente,
    FaturamentoCFOP,
    FaturamentoData,
    FaturamentoProduto,
    FaturamentoOperacao,
    FaturamentoMunicipioIBGE,
    FaturamentoVendedor,
    FaturamentoValorItem,
    FaturamentoVolumeItem,
    FaturamentoValorBrutoItem,
    FaturamentoCancelado,
    FaturamentoUnidadeMedida,
    FaturamentoValorConversao,
    FaturamentoLote,
    FaturamentoTratamento,
    FaturamentoVolumeItemKG,
    current_timestamp() as data_atualizacao
FROM cdf_data
WHERE rn = 1

{% else %}

SELECT
    CHAVE_NFE as FaturamentoChaveNF,
    COD_FILIAL as FaturamentoFilial,
    COD_FABRICANTE as FaturamentoFabricante,
    NR_NF as FaturamentoNumeroNF,
    SERIE_NF as FaturamentoSerieNF,
    CPF_CNPJ_CLIFOR as FaturamentoCliente,
    COD_CFOP as FaturamentoCFOP,
    DATA_EMISSAO as FaturamentoData,
    COD_PRODUTO as FaturamentoProduto,
    COD_CFOP as FaturamentoOperacao,
    COD_IBGE as FaturamentoMunicipioIBGE,
    COD_VENDEDOR as FaturamentoVendedor,
    VALOR_LIQUIDO as FaturamentoValorItem,
    QUANTIDADE_LT_KG as FaturamentoVolumeItem,
    VALOR_TOTAL as FaturamentoValorBrutoItem,
    TIPO_NF as FaturamentoCancelado,
    UNIDADE_MEDIDA as FaturamentoUnidadeMedida,
    FATOR_CONVERSAO as FaturamentoValorConversao,
    LOTE_INTERNO as FaturamentoLote,
    COD_TRATAMENTO as FaturamentoTratamento,
    PESO_KG_SEMENTE as FaturamentoVolumeItemKG,
    current_timestamp() as data_atualizacao
FROM {{ source('silver', 'fato_edi_syngenta_notas_fiscais') }}
WHERE CHAVE_NFE IS NOT NULL

{% endif %}
