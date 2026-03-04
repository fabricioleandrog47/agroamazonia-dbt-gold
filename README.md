# DBT EKS Template

Template para pipeline de dados usando DBT + Spark no Amazon EKS com deploy automatizado via GitHub Actions.

## 📋 Pré-requisitos

- Docker e Docker Compose
- Git
- Conta AWS com acesso ao EKS
- Repositório GitHub

## 🚀 Desenvolvimento Local

### 1. Clonar o Projeto

```bash
git clone <url-do-repositorio>
cd dbt-eks-template
```

### 2. Configurar Ambiente Local

#### Etapa 1: Usando Docker para Spark Thrift Server

O Docker fornece o servidor Spark Thrift para conexão do DBT:

```bash
cd spark/
docker-compose up -d
```

#### Etapa 2: Instalar DBT Localmente

Para desenvolvimento DBT local:

```bash
# Criar ambiente virtual
python -m venv venv

# Ativar ambiente virtual
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Instalar DBT
pip install dbt-spark
```

**Arquivos importantes:**
- `spark/Dockerfile` - Imagem Spark com Delta Lake e AWS JARs
- `spark/docker-compose.yml` - Servidor Spark Thrift
- `spark/requirements.txt` - Dependências completas do Spark
- `spark/.env` - Variáveis de ambiente locais

### 3. Desenvolver e Testar

#### Com Docker + DBT Local:
```bash
# 1. Subir Spark Thrift Server
cd spark && docker-compose up -d

# 2. Ativar venv e executar DBT
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
dbt run
```

#### Apenas DBT Local (sem Spark):
```bash
# Ativar ambiente virtual
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Executar DBT
dbt run
```
teste
Com o ambiente configurado, você pode:
- Desenvolver modelos DBT em `models/`
- Testar transformações localmente
- Validar scripts em `scripts/`

## 🔧 Configuração para Produção

### 1. Configurar Secrets no GitHub

Vá em **Settings > Secrets and variables > Actions** e adicione:

```
AWS_ACCESS_KEY_ID=sua_access_key
AWS_SECRET_ACCESS_KEY=sua_secret_key
CLIENTE_ID=id_do_cliente
TEAMS_WEBHOOK_URL=url_do_webhook_teams
```
TESTE

## 📁 Estrutura do Projeto

```
dbt-eks-template/
├── .github/workflows/
│   └── deploy.yml              # Pipeline CI/CD
├── models/
│   ├── silver/                 # Camada Silver
│   ├── gold/                   # Camada Gold
│   └── control/                # Controles
├── scripts/
│   ├── teams_approval.py       # Integração Teams
│   ├── register_tables_athena.py
│   └── load_gold_to_redshift.py
├── spark/
│   ├── Dockerfile              # Imagem Spark
│   ├── docker-compose.yml      # Ambiente local
│   └── .env                    # Variáveis locais
├── dbt_project.yml
├── profile.yml
└── README.md
```

## 🛠️ Comandos Úteis

### Desenvolvimento Local
```bash
# Opção 1: Docker + DBT Local
cd spark && docker-compose up -d  # Spark Thrift Server
source venv/bin/activate          # Ativar venv
dbt run                           # Executar DBT

# Opção 2: Apenas DBT Local
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
dbt run
```

### Deploy Manual
```bash
# Fazer push para produção
git add .
git commit -m "feat: nova funcionalidade"
git push origin main
```

## 🔍 Troubleshooting

### Problemas Comuns

**Docker não sobe:**
- Verificar se Docker está rodando
- Verificar portas disponíveis
- Checar arquivo `.env`

**Deploy falha:**
- Verificar secrets do GitHub
- Confirmar permissões AWS
- Checar logs no GitHub Actions

**Teams não notifica:**
- Verificar webhook URL
- Testar webhook manualmente
- Confirmar environment configurado

## 📚 Documentação Adicional

- [Configuração Teams](docs/teams-approval.md)
- [DBT Documentation](https://docs.getdbt.com/)
- [Spark Operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/nova-funcionalidade`)
3. Commit suas mudanças (`git commit -m 'feat: nova funcionalidade'`)
4. Push para a branch (`git push origin feature/nova-funcionalidade`)
5. Abra um Pull Request


Criar o database
docker exec spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -e "CREATE DATABASE IF NOT EXISTS cliente_5037;"

Registrar a tabela silver

docker exec spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -e "USE cliente_5037; CREATE TABLE IF NOT EXISTS fato_edi_syngenta_notas_fiscais USING delta LOCATION 's3a://brid-silver/5037/FATO_EDI_SYNGENTA_NOTAS_FISCAIS';"

docker exec spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -e "USE cliente_5037; CREATE TABLE IF NOT EXISTS fato_edi_syngenta_estoque USING delta LOCATION 's3a://brid-silver/5037/FATO_EDI_SYNGENTA_ESTOQUE';"

docker exec spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -e "USE cliente_5037; CREATE TABLE IF NOT EXISTS dim_filial USING delta LOCATION 's3a://brid-silver/5037/DIM_FILIAL';"


# Registrar como dim_filial_silver (source)
docker exec spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -e "USE cliente_5037; CREATE TABLE IF NOT EXISTS dim_filial_silver USING delta LOCATION 's3a://brid-silver/5037/DIM_FILIAL';"

# Rodar DBT (vai criar dim_filial no gold)
dbt run --target dev --profiles-dir . --vars "{\"cliente_id\": \"5037\"}"

docker exec spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -e "USE cliente_5037; DROP TABLE IF EXISTS dim_filial;"


docker exec spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -e "DROP DATABASE IF EXISTS default_gold_5037 CASCADE;"