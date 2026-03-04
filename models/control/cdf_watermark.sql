{{
    config(
        materialized='table',
        file_format='delta'
    )
}}

SELECT
    'fato_edi_syngenta_notas_fiscais' as table_name,
    CAST(0 as BIGINT) as last_processed_version,
    current_timestamp() as updated_at

UNION ALL

SELECT
    'fato_edi_syngenta_estoque' as table_name,
    CAST(0 as BIGINT) as last_processed_version,
    current_timestamp() as updated_at

UNION ALL

SELECT
    'dim_filial_silver' as table_name,
    CAST(0 as BIGINT) as last_processed_version,
    current_timestamp() as updated_at
