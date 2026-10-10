# ClimaLake 🌦️

Pipeline de dados meteorológicos desenvolvido para praticar Engenharia de Dados com **Python, PySpark, Airflow e AWS**, seguindo a arquitetura medalhão **Bronze → Silver → Gold**.

O projeto consulta a previsão horária de **Belo Horizonte (MG)** na API pública [Open-Meteo](https://open-meteo.com/), salva a resposta original no S3, transforma os registros com PySpark e gera indicadores diários. O Airflow coordena as etapas por meio de uma DAG que, nesta versão, é **executada manualmente**.

> **Escopo atual:** a API é chamada com `forecast_days=1`. Os dados representam uma previsão para um dia, não medições históricas observadas. O pipeline usa o Amazon S3 como serviço AWS; o Airflow e o Spark são executados localmente em contêineres Docker.

## Arquitetura

```text
                 Open-Meteo Forecast API
                           │
                           ▼
                 ┌──────────────────┐
                 │  1. BRONZE       │
                 │  JSON original   │
                 └────────┬─────────┘
                          │ 
                          ▼
                 ┌──────────────────┐
                 │  2. SILVER       │
                 │ PySpark:         │
                 │ registro tratado │
                 │ por hora         │
                 └────────┬─────────┘
                          │ 
                          ▼
                 ┌──────────────────┐
                 │  3. GOLD         │
                 │  indicadores     │
                 │ agregados do dia.│
                 │  Ex: temperatura │
                 │  média           │
                 └──────────────────┘

       Apache Airflow: orquestra Bronze → Silver → Gold
       Armazenamento: Amazon S3 | Formato: JSON
```

### Camadas de dados

| Camada | Conteúdo | Exemplo de chave no S3 |
|---|---|---|
| **Bronze** | Resposta bruta da API, incluindo metadados e séries horárias retornadas. | `bronze/ingestion_date=AAAA-MM-DD/open_meteo_clima_AAAAMMDD.json` |
| **Silver** | Dados normalizados em registros com horário, temperatura, precipitação e umidade relativa. | `silver/ingestion_date=AAAA-MM-DD/clima_silver_AAAAMMDD.json` |
| **Gold** | Indicadores agregados para o dia: temperaturas mínima, máxima e média; horários dos extremos; precipitação total; umidade mínima, máxima e média; e amplitude térmica. | `gold/ingestion_date=AAAA-MM-DD/clima_gold_AAAAMMDD.json` |

Os arquivos usam uma partição `ingestion_date=AAAA-MM-DD`. Os nomes dos objetos são determinísticos por data de ingestão; uma nova execução no mesmo dia substitui os objetos com as mesmas chaves. O versionamento do bucket S3 pode ser ativado para preservar versões anteriores.

### Indicadores calculados na Gold

- **Temperatura mínima, máxima e média:** calculadas a partir dos valores horários.
- **Horário da temperatura mínima e máxima:** identifica quando os extremos ocorreram.
- **Precipitação total:** soma da precipitação horária retornada pela API para o dia.
- **Umidade relativa mínima, máxima e média:** calculadas a partir dos valores horários.
- **Horário da umidade mínima e máxima:** identifica quando os extremos ocorreram.
- **Amplitude térmica:** temperatura máxima menos temperatura mínima.

A Silver mantém um registro por horário; a Gold resume esses registros em um conjunto de indicadores diários. Como a consulta usa `forecast_days=1`, a implementação atual calcula os indicadores para o único dia consultado.

## Tecnologias utilizadas

- **Python** — extração, integração com a AWS e lógica do pipeline.
- **Requests** — chamadas HTTP para a API Open-Meteo.
- **PySpark** — transformação dos dados brutos em arrays de horários e agregações diárias.
- **Apache Airflow 3** — definição e execução ordenada das tarefas Bronze, Silver e Gold.
- **Docker Compose** — execução local dos serviços do Airflow e das dependências necessárias.
- **Amazon S3 / Boto3** — armazenamento e leitura dos objetos das camadas.
- **AWS CLI** — autenticação do perfil AWS utilizado no desenvolvimento.

O volume atual de dados é pequeno e não exige Spark do ponto de vista de desempenho. O objetivo principal é **praticar PySpark** e uma arquitetura que pode ser expandida para conjuntos maiores.

## Estrutura do repositório

Os módulos Python ficam diretamente na raiz do projeto.

```text
ClimaLake/
├── dags/
│   └── clima_lake_dag.py   # DAG manual: Bronze → Silver → Gold
├── config/
│   └── .gitkeep            # Diretório de configuração do Airflow
├── plugins/
│   └── .gitkeep
├── config.py               # Variáveis de ambiente e cliente S3
├── extracao.py             # Consulta à API Open-Meteo
├── importacao.py           # Download de objetos Bronze/Silver 
├── ingestao.py             # Grava Bronze, Silver e Gold no S3.
├── transform.py            # Transformações PySpark Silver e Gold.
├── Dockerfile              # Airflow e demais dependências.
├── docker-compose.yaml     # Execução local do .Airflow.
├── requirements.txt        # Dependências Python.
├── .env.example            # Modelo de configuração local.
├── .gitignore              # Exclusão de segredos.
└── README.md
```

O `data/`, assim como arquivos de execução do Airflow, são gerados localmente e não precisam ser enviados ao GitHub.

## Pré-requisitos

- Git.
- Docker Desktop com Docker Compose v2 (no Windows, com backend WSL 2 recomendado).
- AWS CLI v2 com suporte ao comando `aws login`.
- Uma conta AWS e um bucket S3 privado na região configurada.
- Permissões no bucket para listar, ler e gravar objetos.

Não é necessário instalar Java e PySpark diretamente no Windows para executar a DAG pelo Docker: o `Dockerfile` instala Java 17 dentro da imagem do Airflow. O Docker Desktop precisa de memória e espaço suficientes para baixar e executar as imagens.

## Configuração

### 1. Clone o repositório

```powershell
git clone https://github.com/<seu-usuario>/ClimaLake.git
cd ClimaLake
```

Substitua `<seu-usuario>` pelo nome da conta ou organização onde o repositório será publicado.

### 2. Crie um bucket S3

No console da AWS, crie um bucket com nome globalmente único. O projeto utiliza `us-east-1` por padrão, mas a região pode ser alterada em `.env`.

Mantenha **Block all public access** ativado. O projeto não precisa de um bucket público.

### 3. Configure a autenticação AWS

No terminal do host, autentique o perfil usado pelo projeto.

```powershell
aws login --profile seu_perfil
aws sts get-caller-identity --profile seu_perfil
aws s3 ls s3://<nome-do-seu-bucket> --profile seu_perfil
```

Ajuste o nome do perfil. A identidade deve ter apenas as permissões necessárias ao bucket do projeto, incluindo leitura e gravação de objetos e listagem do bucket.

### 4. Configure o `.env`

Copie o modelo e edite o arquivo criado:

```powershell
Copy-Item .env.example .env
notepad .env
```

Preencha os valores conforme seu ambiente:

| Variável | Finalidade |
|---|---|
| `AWS_REGION` | Região do bucket, por exemplo `us-east-1`. |
| `S3_BUCKET` | Nome exato do bucket S3. |
| `AWS_PROFILE` | Perfil AWS autenticado no passo anterior. |
| `AWS_HOST_DIR` | Caminho da pasta `.aws` do usuário no host, usando barras `/` no Windows. Exemplo: `C:/Users/SEU_USUARIO/.aws`. |
| `AIRFLOW_UID` | Mantenha `50000` no Windows/macOS; em Linux, use o UID indicado na documentação do Airflow. |
| `FERNET_KEY` | Chave Fernet para o Airflow. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Segredo JWT para a API do Airflow. |
| `_AIRFLOW_WWW_USER_USERNAME` | Usuário de acesso à interface web. |
| `_AIRFLOW_WWW_USER_PASSWORD` | Senha escolhida para esse usuário. |

A pasta de configuração AWS precisa existir e o caminho indicado em `AWS_HOST_DIR` deve corresponder ao caminho real do seu computador. A configuração Compose monta essa pasta para que o Boto3 possa utilizar o perfil e o cache de autenticação.

### 5. Gere os segredos do Airflow

Não reutilize os valores de exemplo de outra instalação. Você pode gerar valores aleatórios executando:

```powershell
docker run --rm python:3.12-slim python -c "import os, base64, secrets; print('FERNET_KEY=' + base64.urlsafe_b64encode(os.urandom(32)).decode()); print('AIRFLOW__API_AUTH__JWT_SECRET=' + secrets.token_urlsafe(32)); print('_AIRFLOW_WWW_USER_PASSWORD=' + secrets.token_urlsafe(16))"
```

Copie os valores gerados para as variáveis correspondentes no `.env`. Escolha também um nome de usuário para `_AIRFLOW_WWW_USER_USERNAME`.

## Executar o pipeline

Execute os comandos na pasta raiz do repositório, onde estão `Dockerfile` e `docker-compose.yaml`.

### 1. Validar e construir os serviços

```powershell
docker compose config
docker compose build
```

### 2. Inicializar e iniciar o Airflow

```powershell
docker compose up airflow-init
docker compose up -d
docker compose ps
```

A inicialização prepara o banco de metadados e cria o usuário da interface. O projeto usa uma configuração local de desenvolvimento, com `LocalExecutor` e banco SQLite persistido em um volume Docker; não é uma configuração indicada para produção.

### 3. Disparar a DAG manualmente

1. Abra [http://localhost:8080](http://localhost:8080) no navegador.
2. Entre com o usuário e a senha definidos no `.env`.
3. Localize a DAG `climalake_pipeline`.
4. Se estiver pausada, ative-a e clique em **Trigger** para iniciar uma execução.
5. Acompanhe o resultado das tarefas `bronze`, `silver` e `gold` pela interface. Cada tarefa possui logs próprios.

A DAG está configurada com `schedule=None`: **ela não executa automaticamente por horário**. Cada execução deve ser iniciada manualmente pela interface do Airflow.

### 4. Conferir os objetos no S3

```powershell
aws s3 ls s3://<nome-do-seu-bucket>/bronze/ --recursive --profile climalake
aws s3 ls s3://<nome-do-seu-bucket>/silver/ --recursive --profile climalake
aws s3 ls s3://<nome-do-seu-bucket>/gold/ --recursive --profile climalake
```

Os nomes seguem a estrutura descrita na seção [Camadas de dados](#camadas-de-dados). Para visualizar um objeto Gold no terminal:

```powershell
aws s3 cp s3://<nome-do-seu-bucket>/gold/ingestion_date=AAAA-MM-DD/clima_gold_AAAAMMDD.json - --profile climalake
```

Substitua a data e o nome do objeto pelos valores existentes no bucket.

### 5. Parar o ambiente

```powershell
docker compose down
```

Esse comando para e remove os contêineres, mantendo os volumes nomeados, como o banco do Airflow. Para remover também os volumes e apagar o estado local do Airflow:

```powershell
docker compose down -v
```

O comando `down -v` não apaga os objetos já enviados ao S3.

## Logs e tratamento de falhas

Os logs de execução ficam disponíveis na interface do Airflow, dentro da tarefa correspondente. A DAG registra mensagens de início, fim das etapas e chaves dos objetos gerados. O Airflow está configurado para uma nova tentativa em caso de falha, com intervalo de cinco minutos.

A extração verifica se a resposta inclui os arrays horários necessários e se eles possuem comprimentos compatíveis. A DAG também verifica se a ingestão retorna uma referência S3 com bucket e chave. Ainda não há uma suíte de testes automatizados nem regras completas de qualidade de dados; a validação detalhada das faixas e dos valores pode ser uma evolução futura.

## Limitações e próximos passos

- A localização está fixa em Belo Horizonte (MG), e a consulta usa `forecast_days=1`.
- Os dados são uma previsão meteorológica da Open-Meteo, e não uma série de medições históricas reais.
- As camadas são armazenadas em JSON. Formatos colunares, como Parquet, e uma solução de catálogo/consulta analítica podem ser explorados futuramente.
- A DAG é disparada manualmente e não possui agendamento automático.
- O ambiente Docker Compose e o banco SQLite são destinados ao desenvolvimento local, não à produção.
- O projeto não provisiona recursos de infraestrutura na AWS para evitar custos; o bucket S3 é criado e configurado separadamente.

## Fonte de dados

- [Open-Meteo Forecast API](https://open-meteo.com/en/docs)

Consulte a documentação e os termos de uso da Open-Meteo para conhecer a atribuição e as condições de uso dos dados.
