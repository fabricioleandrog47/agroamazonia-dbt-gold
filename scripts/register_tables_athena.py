import boto3
import time
from dotenv import load_dotenv
import os

load_dotenv()

# Configurar cliente Athena
athena = boto3.client(
    'athena',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

# Configurações
DATABASE = 'spark_dbt'
OUTPUT_LOCATION = 's3://brid-incremental/athena-results/'
BUCKET = 'brid-datawarehouse-spark'
PREFIX = 'warehouse/juca'

def execute_query(query):
    """Executa query no Athena e aguarda resultado"""
    print(f"Executando: {query[:100]}...")
    
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT_LOCATION}
    )
    
    query_execution_id = response['QueryExecutionId']
    
    # Aguardar conclusão
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = result['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if status == 'SUCCEEDED':
        print(f"✓ Sucesso")
        return True
    else:
        error = result['QueryExecution']['Status'].get('StateChangeReason', 'Erro desconhecido')
        print(f"✗ Falhou: {error}")
        return False

# Criar database
print("\n=== Criando Database ===")
execute_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# Registrar tabelas Silver
print("\n=== Registrando Tabelas Silver ===")

execute_query(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.silver_pedido
LOCATION 's3://{BUCKET}/{PREFIX}/cliente_juca_silver_juca.db/silver_pedido/'
TBLPROPERTIES ('table_type'='DELTA')
""")

execute_query(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.silver_pedido_item
LOCATION 's3://{BUCKET}/{PREFIX}/cliente_juca_silver_juca.db/silver_pedido_item/'
TBLPROPERTIES ('table_type'='DELTA')
""")

# Registrar tabela Gold
print("\n=== Registrando Tabela Gold ===")

execute_query(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.fato_pedido
LOCATION 's3://{BUCKET}/{PREFIX}/cliente_juca_gold_juca.db/fato_pedido/'
TBLPROPERTIES ('table_type'='DELTA')
""")

# Atualizar partições (não necessário para Delta)
print("\n✓ Todas as tabelas registradas no Athena!")
print(f"Database: {DATABASE}")
print(f"Região: {os.getenv('AWS_REGION')}")
