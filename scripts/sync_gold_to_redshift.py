import json
import os
import sys
from datetime import datetime
import boto3
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, DateType, TimestampType
from string import Template
from dotenv import load_dotenv

# Carregar .env
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

def load_config(config_path):
    """Carrega e substitui variáveis de ambiente no JSON"""
    with open(config_path, 'r') as f:
        content = f.read()
    
    # Substituir ${VAR} por valores do ambiente
    template = Template(content)
    config_str = template.safe_substitute(os.environ)
    return json.loads(config_str)

def get_last_update(cursor, schema, table, column):
    """Busca última data de atualização no Redshift"""
    try:
        cursor.execute(f"SELECT MAX({column}) FROM {schema}.{table}")
        result = cursor.fetchone()[0]
        return result if result else '1900-01-01'
    except:
        return '1900-01-01'

def create_table_ddl(schema, table, columns, primary_keys):
    """Gera DDL da tabela"""
    cols = [f"{col} {dtype}" for col, dtype in columns.items()]
    pk = f"PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
    
    return f"""
CREATE TABLE IF NOT EXISTS "{schema}".{table} (
    {', '.join(cols)}
    {', ' + pk if pk else ''}
)
"""

def create_staging_ddl(schema, table, columns):
    """Gera DDL da tabela staging (sem PK)"""
    cols = [f"{col} {dtype}" for col, dtype in columns.items()]
    return f"""
CREATE TABLE IF NOT EXISTS "{schema}".{table}_staging (
    {', '.join(cols)}
)
"""

def merge_query(schema, table, primary_keys, columns):
    """Gera query de MERGE"""
    pk_join = " AND ".join([f'"{schema}".{table}.{pk} = "{schema}".{table}_staging.{pk}' for pk in primary_keys])
    insert_cols = ", ".join(columns.keys())
    
    return f"""
BEGIN TRANSACTION;

-- Deletar registros que serão atualizados
DELETE FROM "{schema}".{table}
USING "{schema}".{table}_staging
WHERE {pk_join};

-- Inserir novos e atualizados
INSERT INTO "{schema}".{table} ({insert_cols})
SELECT {insert_cols}
FROM "{schema}".{table}_staging;

-- Limpar staging
TRUNCATE "{schema}".{table}_staging;

END TRANSACTION;
"""

def process_table(spark, cursor, conn, config, table_config):
    """Processa uma tabela: lê Delta, exporta Parquet, COPY, MERGE"""
    
    name = table_config['name']
    gold_path = table_config['gold_path']
    schema = table_config['redshift_schema']
    table = table_config['redshift_table']
    pk = table_config['primary_keys']
    incr_col = table_config['incremental_column']
    columns = table_config['columns']
    
    print(f"\n{'='*60}")
    print(f"Processando: {name}")
    print(f"{'='*60}")
    
    # 1. Criar schema
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    conn.commit()
    
    # 2. Criar tabelas
    cursor.execute(create_table_ddl(schema, table, columns, pk))
    cursor.execute(create_staging_ddl(schema, table, columns))
    conn.commit()
    
    # 3. Buscar última atualização
    last_update = get_last_update(cursor, f'"{schema}"', table, incr_col)
    print(f"Última atualização: {last_update}")
    
    # 4. Ler dados incrementais
    df = spark.read.format("delta").load(gold_path) \
        .filter(f"{incr_col} > '{last_update}'")
    
    count = df.count()
    print(f"Registros novos: {count}")
    
    if count == 0:
        print("✓ Nenhum dado novo")
        return
    
    # 5. Selecionar apenas colunas definidas no config (evita colunas extras)
    df = df.select(*columns.keys())
    
    # 5.1. Converter tipos para compatibilidade com Redshift
    for col_name, col_type in columns.items():
        print(f"Convertendo {col_name} para {col_type}")
        if 'DECIMAL' in col_type.upper():
            # Extrair precisão do tipo DECIMAL(18,2) ou usar padrão
            if '(' in col_type:
                precision = col_type.split('(')[1].split(')')[0].split(',')
                p = int(precision[0])
                s = int(precision[1]) if len(precision) > 1 else 0
                df = df.withColumn(col_name, F.col(col_name).cast(DecimalType(p, s)))
            else:
                df = df.withColumn(col_name, F.col(col_name).cast(DecimalType(18, 4)))
        elif 'VARCHAR' in col_type.upper():
            df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
        elif col_type.upper() == 'DATE':
            df = df.withColumn(col_name, F.col(col_name).cast(DateType()))
        elif col_type.upper() == 'TIMESTAMP':
            df = df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
    
    # 6. Exportar para staging S3
    staging_path_s3a = f"s3a://{config['s3']['staging_bucket']}/{config['s3']['staging_prefix']}/{schema}/{table}"
    staging_path_s3 = f"s3://{config['s3']['staging_bucket']}/{config['s3']['staging_prefix']}/{schema}/{table}"
    
    print(f"Exportando para: {staging_path_s3a}")
    df.write.mode("overwrite").parquet(staging_path_s3a)
    
    # 6. COPY para staging
    print("Copiando para Redshift staging...")
    copy_query = f"""
COPY "{schema}".{table}_staging ({', '.join(columns.keys())})
FROM '{staging_path_s3}'
IAM_ROLE '{config['redshift']['iam_role']}'
FORMAT AS PARQUET
"""
    cursor.execute(copy_query)
    conn.commit()
    
    # 8. MERGE
    print("Executando MERGE...")
    cursor.execute(merge_query(schema, table, pk, columns))
    conn.commit()
    
    print(f"✓ {count} registros carregados!")

def main():
    # Carregar configuração
    config_path = os.path.join(os.path.dirname(__file__), 'redshift_config.json')
    config = load_config(config_path)
    
    # Spark
    spark = SparkSession.builder \
        .appName("Gold_to_Redshift") \
        .getOrCreate()
    
    # Redshift
    rs = config['redshift']
    conn = psycopg2.connect(
        host=rs['host'],
        port=rs['port'],
        database=rs['database'],
        user=rs['user'],
        password=rs['password']
    )
    cursor = conn.cursor()
    
    # Processar cada tabela
    for table_config in config['tables']:
        try:
            process_table(spark, cursor, conn, config, table_config)
        except Exception as e:
            print(f"✗ Erro em {table_config['name']}: {e}")
            conn.rollback()
    
    # Cleanup
    cursor.close()
    conn.close()
    spark.stop()
    
    print(f"\n{'='*60}")
    print("Processamento concluído!")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
