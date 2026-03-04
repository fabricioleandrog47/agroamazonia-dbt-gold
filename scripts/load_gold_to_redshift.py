import boto3
import psycopg2
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

region = os.getenv('AWS_REGION')
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Configurar variáveis de ambiente para Python
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Configurar Spark
spark = (
    SparkSession
    .builder
    .appName("Load_Gold_to_Redshift")
    .getOrCreate()
)

# Conexão Redshift
redshift_host = os.getenv('REDSHIFT_HOST')
redshift_port = int(os.getenv('REDSHIFT_PORT', 5439))
redshift_database = os.getenv('REDSHIFT_DATABASE')
redshift_user = os.getenv('REDSHIFT_USER')
redshift_password = os.getenv('REDSHIFT_PASSWORD')

print(f"Conectando ao Redshift: {redshift_host}:{redshift_port}/{redshift_database} como {redshift_user}")

if not redshift_host:
    raise ValueError("REDSHIFT_HOST não está definido!")

redshift_conn = psycopg2.connect(
    host=redshift_host,
    port=redshift_port,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password,
    connect_timeout=30
)
cursor = redshift_conn.cursor()

# Configurações
BUCKET = 'brid-datawarehouse-spark'
GOLD_PATH = f's3a://{BUCKET}/warehouse/juca/cliente_juca_gold_juca.db/fato_pedido'
STAGING_PATH = f's3a://{BUCKET}/warehouse/juca/cliente_juca_gold_juca.db/redshift-staging/fato_pedido'
STAGING_PATH_REDSHIFT = f's3://{BUCKET}/warehouse/juca/cliente_juca_gold_juca.db/redshift-staging/fato_pedido'
REDSHIFT_SCHEMA = 'dbt_teste'
REDSHIFT_TABLE = f'{REDSHIFT_SCHEMA}.fato_pedido'
REDSHIFT_TABLE_HIST = f'{REDSHIFT_SCHEMA}.fato_pedido_hist'  # SCD2

print("=== Carregando dados incrementais da Gold ===")

# 1. Criar schema se não existir
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA}")

# 2. Criar tabela principal (se não existir)
print("Verificando/criando tabela principal...")
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE} (
    pedido_id INT NOT NULL,
    cliente_id INT,
    data_pedido DATE,
    valor_total DECIMAL(10,2),
    status VARCHAR(50),
    item_id INT NOT NULL,
    produto_id INT,
    quantidade INT,
    preco_unitario DECIMAL(10,2),
    subtotal DECIMAL(10,2),
    dt_atualizacao TIMESTAMP,
    PRIMARY KEY (pedido_id, item_id)
)
DISTKEY(pedido_id)
SORTKEY(data_pedido, dt_atualizacao)
""")

# 3. Criar tabela histórica SCD2 (se não existir)
print("Verificando/criando tabela histórica SCD2...")
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE_HIST} (
    pedido_id INT NOT NULL,
    cliente_id INT,
    data_pedido DATE,
    valor_total DECIMAL(10,2),
    status VARCHAR(50),
    item_id INT NOT NULL,
    produto_id INT,
    quantidade INT,
    preco_unitario DECIMAL(10,2),
    subtotal DECIMAL(10,2),
    dt_atualizacao TIMESTAMP,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    change_type VARCHAR(10)  -- INSERT, UPDATE, DELETE
)
DISTKEY(pedido_id)
SORTKEY(pedido_id, item_id, valid_from)
""")

redshift_conn.commit()

# 4. Buscar última data processada no Redshift
cursor.execute(f"SELECT COALESCE(MAX(dt_atualizacao), '1900-01-01') FROM {REDSHIFT_TABLE}")
last_update = cursor.fetchone()[0]
print(f"Última atualização no Redshift: {last_update}")

# 5. Ler dados incrementais do Delta Lake (incluindo deletados para SCD2)
df_incremental = spark.read.format("delta").load(GOLD_PATH) \
    .filter(f"dt_atualizacao > '{last_update}'")

count = df_incremental.count()
print(f"Registros novos/atualizados: {count}")

if count == 0:
    print("Nenhum dado novo para processar.")
    spark.stop()
    exit(0)

# 6. Exportar para Parquet no S3 (staging)
print("Exportando para staging S3...")
df_incremental.write.mode("overwrite").parquet(STAGING_PATH)

# 7. Criar tabela staging no Redshift (se não existir)
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE}_staging (
    pedido_id INT,
    cliente_id INT,
    data_pedido DATE,
    valor_total DECIMAL(10,2),
    status VARCHAR(50),
    item_id INT,
    produto_id INT,
    quantidade INT,
    preco_unitario DECIMAL(10,2),
    subtotal DECIMAL(10,2),
    is_deleted BOOLEAN,
    dt_atualizacao VARCHAR(50)
)
""")

# 8. COPY do S3 para staging
print("Copiando dados para Redshift staging...")
try:
    copy_query = f"""
COPY {REDSHIFT_TABLE}_staging
FROM '{STAGING_PATH_REDSHIFT}'
IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}'
FORMAT AS PARQUET
COMPUPDATE OFF
STATUPDATE OFF
"""
    cursor.execute(copy_query)
    redshift_conn.commit()
except Exception as e:
    print(f"Erro no COPY: {e}")
    redshift_conn.rollback()
    raise

# 9. Processar SCD2: Historificar registros alterados/deletados
print("Processando SCD2...")
try:
    scd2_query = f"""
BEGIN TRANSACTION;

-- Inserir versões antigas na tabela histórica (registros que serão atualizados ou deletados)
INSERT INTO {REDSHIFT_TABLE_HIST} (
    pedido_id, cliente_id, data_pedido, valor_total, status,
    item_id, produto_id, quantidade, preco_unitario, subtotal,
    dt_atualizacao, valid_from, valid_to, is_current, change_type
)
SELECT 
    t.pedido_id, t.cliente_id, t.data_pedido, t.valor_total, t.status,
    t.item_id, t.produto_id, t.quantidade, t.preco_unitario, t.subtotal,
    t.dt_atualizacao,
    t.dt_atualizacao as valid_from,
    CAST(s.dt_atualizacao AS TIMESTAMP) as valid_to,
    FALSE as is_current,
    CASE WHEN s.is_deleted THEN 'DELETE' ELSE 'UPDATE' END as change_type
FROM {REDSHIFT_TABLE} t
INNER JOIN {REDSHIFT_TABLE}_staging s
    ON t.pedido_id = s.pedido_id AND t.item_id = s.item_id;

-- Deletar registros que foram marcados como deletados
DELETE FROM {REDSHIFT_TABLE}
USING {REDSHIFT_TABLE}_staging
WHERE {REDSHIFT_TABLE}.pedido_id = {REDSHIFT_TABLE}_staging.pedido_id
  AND {REDSHIFT_TABLE}.item_id = {REDSHIFT_TABLE}_staging.item_id
  AND {REDSHIFT_TABLE}_staging.is_deleted = TRUE;

-- Atualizar registros existentes (não deletados)
DELETE FROM {REDSHIFT_TABLE}
USING {REDSHIFT_TABLE}_staging
WHERE {REDSHIFT_TABLE}.pedido_id = {REDSHIFT_TABLE}_staging.pedido_id
  AND {REDSHIFT_TABLE}.item_id = {REDSHIFT_TABLE}_staging.item_id
  AND {REDSHIFT_TABLE}_staging.is_deleted = FALSE;

-- Inserir novos/atualizados (exceto deletados)
INSERT INTO {REDSHIFT_TABLE} (
    pedido_id, cliente_id, data_pedido, valor_total, status,
    item_id, produto_id, quantidade, preco_unitario, subtotal, dt_atualizacao
)
SELECT 
    pedido_id, cliente_id, data_pedido, valor_total, status,
    item_id, produto_id, quantidade, preco_unitario, subtotal, 
    CAST(dt_atualizacao AS TIMESTAMP)
FROM {REDSHIFT_TABLE}_staging
WHERE is_deleted = FALSE;

-- Limpar staging
TRUNCATE {REDSHIFT_TABLE}_staging;

END TRANSACTION;
"""
    cursor.execute(scd2_query)
    redshift_conn.commit()
    print(f"✓ {count} registros carregados no Redshift!")
except Exception as e:
    print(f"Erro no SCD2: {e}")
    redshift_conn.rollback()
    raise

# Cleanup
cursor.close()
redshift_conn.close()
spark.stop()
