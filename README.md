# lambda-yfinance-extract
Armazenamento do codigo com lambda para realizar consulta ao YFINANCE


# pre requisitos 
AWS CLI instalado
Configurar a AWS localmente (aws configure)
Ter conta na aws (pode ser free tier)
Crie o bucket para receber os arquivos na aws

# criar as imagens e subir no AWS ECR
localmente execute os comandos para 

 -- Comando para login na AWS
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin <ID_DA_CONTA>.dkr.ecr.us-east-1.amazonaws.com

  -- comando para fazer o build da imagem  
  docker build -t minha-imagem:latest .
  docker build --no-cache -t lambda-yfinance-upt-9:latest .
  
  -- comando para tag da imagem
  docker tag minha-imagem:latest <ID_DA_CONTA>.dkr.ecr.us-east-1.amazonaws.com/meu-repo:latest
  
  -- comando para push no repo do ECR (crie um antes)
  docker push <ID_DA_CONTA>.dkr.ecr.us-east-1.amazonaws.com/meu-repo:latest


# crie as lambdas com base nas imagens
Lambdas criadas

# configure o Event Bridge 
Configure o agendamento da execução da lambda bovespa 

# Configure o bucket
Configure o Event Notification no bucket S3 - na pasta raw .parquet acionar a lambda de start do Glue

# Crie o glue
Utilize o arquivo script-glue como exemplo

# valide o Athena

=======================================================

Requisitos para entrega:

✅Requisito 1: scrap de dados de ações ou índices da B3 (granularidade diária). 
✅Requisito 2: os dados brutos devem ser ingeridos no s3 em formato parquet com partição diária. 
✅Requisito 3: o bucket deve acionar uma lambda,que por sua vez irá chamar o job de ETL no glue
✅Requisito 4: a lambda pode ser em qualquer linguagem. Ela apenas deverá iniciar o job Glue. 
✅Requisito 5: o job Glue pode ser feito no modo visual ou via código. Este job deve conter as seguintes transformações obrigatórias:
A: agrupamento numérico, sumarização, contagem ou soma. 
B: renomear duas colunas existentes além das de agrupamento. 
C: realizar um cálculo com base na data, como por exemplo média móvel, diferença entre períodos, valores extremos no período etc. 
✅Requisito 6: os dados refinados no job glue devem ser salvos no formato parquet em uma pasta chamada refined, particionado por data e pelo nome ou código da ação/índice.
✅Requisito 7: job Glue deve automaticamente catalogar o dado no Glue Catalog e criar uma tabela no banco de dados (pode ser o default). 
✅Requisito 8: os dados devem estar disponíveis e serem consultados usando SQL através do Athena

## Arquitetura 

## 🔄 Fluxo de Funcionamento

# Agendamento com EventBridge
Executa diariamente às 12h.
Dispara a Lambda bovespa-lambda-container

# Extração com Lambda bovespa-lambda-container
coleta dados da API yfinance e brapi.dev
Conecta à API Yahoo Finance e brapi.dev.
Extrai dados de ações da Bovespa.
Limpa e padroniza os dados.
Gera arquivos Parquet e salva no bucket S3 fiap-bovespa-id_conta/raw

# Detecção com S3 Event Notification 
Armazenamento dos arquivos brutos em formato Parquet.
Detecta novos arquivos no bucket raw.
Dispara a Lambda lambda-start-glue

# Processamento com Lambda lambda-start-glue
Dispara a Lambda de processamento ao detectar novos arquivos.
Inicia o Glue Job glue-bovespa-trigger via start_job_run

# Transformação com Glue Job
Normaliza colunas, identifica campos de preço e volume.
Calcula agregações por ticker e data_extraida.
Aplica média ponderada pelo volume.
Calcula variação e variação percentual.
Salva dados refinados em fiap-bovespa-id_conta/refined.
Atualiza o Glue Data Catalog com a tabela bovespa_refinado

# Consulta com Amazon Athena
Permite consultas SQL sobre os dados refinados.


# Exemplo:
SELECT 
    date_str as data,
    ticker,
    ROUND(preco_medio, 4) as preco_medio,
    ROUND(preco_anterior, 4) as preco_anterior,
    ROUND(variacao, 4) as variacao,
    ROUND(variacao_percentual, 2) as variacao_percentual,
    CAST(volume_total AS BIGINT) as volume_total,
    CAST(volume_medio AS BIGINT) as volume_medio
FROM bovespa_db.bovespa_refinado 
WHERE ticker = 'PETR4'
ORDER BY date DESC
LIMIT 10;


## 🗂️ Organização no S3
raw/ticker=ABEV3/data=2026-01-19/dados.parquet
refined/date=2026-01-19/ticker=ABEV3/

## 🧪 Teste de Consulta Athena
SELECT date, ticker, preco_medio, preco_anterior,
       ROUND(variacao, 4) AS variacao,
       ROUND(variacao_percentual, 2) AS variacao_percent,
       volume_total
FROM bovespa_db.bovespa_refinado
ORDER BY date DESC, ticker
LIMIT 20;
