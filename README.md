# Objetivo  
O objetivo desta POC (Prova de conceito) e montar uma plataforma de dados   mínima usando ferramentas de código aberta. 

# Objetivos específicos  
- Montar uma plataforma de dados mínima com as camadas de orquestração, processamento e armazenamento 
- Usar ferramentas de código aberto 

# Ferramentas usadas  
- [airflow - 3.0.2](https://airflow.apache.org/docs/apache-airflow/stable/index.html)  
- [pyspark - 4.0.0](https://spark.apache.org/docs/latest/api/python/index.html#)  
- [delta lake - 4.0.0](https://docs.delta.io/latest/index.html)
- [minio - RELEASE.2025-04-22T22-12-26Z](https://min.io/docs/minio/kubernetes/upstream/index.html)
- [papermil - 2.4.0](https://papermill.readthedocs.io/en/latest/index.html)  

# Configuração dos serviços  
> Atenção! Este projeto deve ser executado em uma maquina Linux ou wsl2 do windows  
> 
> Para saber mais sobre wsl2 click [aqui](https://learn.microsoft.com/pt-br/windows/wsl/about)  

## 0 - Pré-requisitos    
- Certifique-se de ter o Docker e Docker Compse instalados na sua máquina.    
- Para instalar o Docker, click [aqui](https://docs.docker.com/engine/install/)   
- Para instalar o Docker Compose, clique [aqui](https://docs.docker.com/compose/install/)    

## 1 - Abra o terminal de sua preferência  
- No Windows, click [aqui](https://elsefix.com/pt/tech/tejana/how-to-use-windows-terminal-in-windows-10-beginners-guide) para saber como abrir um terminal.  
- No Linux, click [aqui](https://pt.linux-console.net/?p=12268) para saber como abrir um terminal.  

## 2 - Clone do repositorio  
- Click [aqui](https://docs.github.com/pt/repositories/creating-and-managing-repositories/cloning-a-repository) e siga o passo de como clonar um respositório do github  

## 3 - Mude para a pasta do projeto  
~~~bash  
$ cd projeto-lakehouse  
~~~  

## 4 - Configure uma rede externa do docker  
~~~bash  
$ docker network create projeto-lakehouse  
~~~  

## 5 - Crie o arquivo .env baseado no .env.template  
~~~bash  
$ cp .env.template .env  
~~~  

## 6 - Abra um editor de código de sua preferência.  
> Sugestão! Use o vscode.  
- No Windows com wsl2, click [aqui](https://learn.microsoft.com/pt-br/windows/wsl/tutorials/wsl-vscode) para saber como instalar e usar.  
- No Linux, click [aqui](https://code.visualstudio.com/docs/setup/linux) para saber como instalar e usar.  

## 7 - Abra o arquivo .env e preencha os valores das variáveis  

## 8 - Volte para o terminal e execute o comando abaixo para iniciar os serviços  
~~~bash  
$ docker compose up  
~~~  
Este comando deixa o terminal preso, se preferir pode usar o comando abaixo para iniciar os serviços e liberar o terminal  
~~~bash  
$ docker compose up -d  
~~~  
  
## 9 - Verifique o status dos serviços Docker  
~~~bash  

$ docker ps

~~~  
Você deverá ver os contêineres para Airflow (database, scheduler, worker), MinIO, Postgres, etc., com o status Up.  

Para verificar os logs de um serviço específico (substitua airflow_api_server pelo nome do serviço que deseja inspecionar, como minio, airflow_scheduler etc.):  

~~~bash  

$ docker compose logs -f airflow_dag-processor

~~~  
Pressione Ctrl + C para sair dos logs.  

## 10 - Acesse as interfaces dos serviços  

  Acesse o MinIO pela interface web em:  

~~~bash 

http://localhost:10000  

~~~ 

Conforme definido no docker-compose.yml, certifique-se de que essa porta foi mapeada corretamente no seu ambiente.  

Faça login utilizando as credenciais definidas no arquivo .env:  

Access Key: MINIO_ROOT_USER  

Key: MINIO_ROOT_PASSWORD  

---
Para a criação dos buckets, após o login, siga os passos abaixo que representam as camadas da arquitetura de medalhão: 

No menu lateral esquerdo, clique em Administrator. 

Em seguida, clique em Buckets. 

No canto superior direito da tela, clique em Create Bucket. 

Crie o bucket inicial chamado landing e clique em Create Bucket para confirmar. 

Para verificar a criação, acesse o menu Object Browser (no menu esquerdo) e confira se o bucket landing aparece na lista. 

Repita o processo para criar os demais buckets necessários, como: bronze, silver, gold, entre outros. 

Esses buckets serão usados para armazenar os dados nos diferentes estágios de processamento do pipeline. 


## 11 - Configuração da conexão do MinIO 

Para permitir a comunicação entre o pipeline e o MinIO, foi necessário configurar uma conexão chamada minio_connection. 

Essa configuração foi realizada dentro do notebook, por meio da criação de um arquivo .json com as credenciais e o endpoint de acesso. O arquivo foi salvo no seguinte caminho: 

src/dags/geography/coordinates/variables/minio_connection.json 
O conteúdo do arquivo inclui as seguintes informações: 

"endpoint": URL interna de acesso ao MinIO 

"access_key": chave de acesso (gerada pelo MinIO)

"key": chave secreta (também gerada pelo MinIO)

Como gerar as credenciais: 

Acesse o MinIO pela interface web (http://localhost:10000).

No menu lateral, clique em Access Keys.

Clique em Create Access Key.

Copie os valores de Access Key e Secret Key gerados.

Preencha o arquivo minio_connection.json com essas informações.

Esse arquivo será utilizado posteriormente para autenticação automática nas DAGs ou scripts que interagem com o armazenamento do MinIO.

## 12 - Configuração do ambiente de desenvolvimento

Ambiente Python com uv, clique [aqui](https://docs.astral.sh/uv/) para saber como instalar e usar.

Após configurar os serviços, o ambiente Python foi instalado e gerenciado com a ferramenta uv, que oferece uma forma rápida e moderna de instalar dependências Python. 

O terminal foi aberto na pasta raiz do projeto dentro do VSCode, onde o comando de instalação das dependências foi executado (com base no arquivo pyproject.toml).

~~~bash 

uv sync 

~~~ 

O teste de funcionamento foi bem-sucedido. A aplicação Spark processou os dados conforme o esperado e os arquivos foram salvos corretamente no MinIO no formato Delta Lake. Isso valida a capacidade do ambiente de:

Processar dados com Apache Spark.

Utilizar as capacidades transacionais e de versionamento do Delta Lake.

Persistir dados de forma eficiente em um armazenamento de objetos compatível com S3 (MinIO).

A arquitetura demonstrada aqui estabelece uma base sólida para pipelines de dados escaláveis e confiáveis, utilizando tecnologias de código aberto para gerenciamento de data lakes.

## 13 Validando de infraestrura: Spark, Delta Lake e MinIO para um Data Lake 

Este repositório documenta um teste de validação da infraestrutura de dados, executado localmente no VS Code. O objetivo foi verificar se a escrita de dados no formato Delta Lake para um bucket MinIO (rodando via Docker) funcionava corretamente utilizando o Apache Spark com PySpark. 

O que foi testado: 

Execução local do código PySpark no VS Code, arquivo : src_lnd_geography_coordinates.ipynb 

Gravação de arquivos Delta (.parquet) em um bucket MinIO (S3 compatível) 

Integração com bibliotecas essenciais: delta-spark, hadoop-aws, aws-java-sdk 

Persistência bem-sucedida dos dados, com estrutura Delta armazenada no MinIO 

Resultado: A escrita no formato Delta funcionou conforme esperado, validando a comunicação entre Spark e MinIO e confirmando que a infraestrutura está pronta para os próximos passos do pipeline. 


## 14 - Ativação do Apache Airflow e Configuração da DAG

Para resolver o erro de conexão da DAG `geography_coordinates` no Airflow, foram realizados os seguintes passos:

- O Airflow foi aberto no navegador na porta **8080** (ex: http://localhost:8080). 

- No menu superior, acessei **Admin > Variables**. 

- Cliquei em **Add Variable** no canto superior direito. 

---

- Criei uma variável com: 

  - **Key:** `minio_connection.json` (nome do arquivo JSON localizado na pasta `variables` do projeto) 

  - **Value:** o conteúdo completo do arquivo JSON copiado e colado diretamente. 

- Após essa configuração, o erro da DAG desapareceu. 

- A DAG `geography_coordinates` aparece ativa no menu lateral esquerdo do Airflow, indicando conexão bem-sucedida com MinIO, PySpark e Delta Lake.

Este procedimento validou a integração do Airflow com a infraestrutura de dados local, garantindo a execução confiável das pipelines.

## 15 Criação da DAG ibge_pme e Organização do Fluxo de Diretórios 

Para estruturar o pipeline de ingestão e transformação dos dados da Pesquisa Mensal de Emprego (PME/IBGE), foi criada a DAG ibge_ipca_amplo no Airflow, com execução agendada a cada 3 minutos (schedule="*/3 * * * *"). A DAG está localizada no caminho: 

~~~bash 

/projeto-lakehouse/src/dags/ibge/ipca_amplo/ibge_ipca_amplo.py 

~~~ 

Essa DAG executa um fluxo dividido nas seguintes camadas: 

- Landing: ingestão inicial dos dados 

- Bronze: padronização e limpeza 

- Silver: transformação e enriquecimento 

Cada camada possui um notebook .ipynb responsável por sua respectiva etapa de processamento.  

Vazias por enquanto.
Cada notebook é executado por um PapermillOperator. 

### Lógica de Dependência entre Tarefas

A DAG define a seguinte sequência entre as tasks: 

~~~bash 

landing → bronze → silver

~~~ 

Essa DAG utiliza notebooks que acessam o MinIO por meio da variável minio_connection, configurada previamente no Airflow (conforme explicado na seção anterior).

## 16 Detalhamento Técnico da Integração: Ingestão de Dados e Execução com Airflow

Após a criação da DAG `ibge_ipca_amplo` e a organização dos notebooks por camadas (Landing, Bronze, Silver), foram implementadas as seguintes etapas para viabilizar a ingestão de dados e execução das tarefas via Airflow:

---

### 1. Desenvolvimento do Processo de Ingestão de Dados

Foi desenvolvido um fluxo de ingestão baseado no arquivo `ibge_ipca_amplo.py`. Nele, foi criada a função `ibge_ipca_amplo()` que realiza as seguintes ações:

- Define o caminho para os dados de entrada na camada **Landing**.
- Cria a conexão com o **MinIO** utilizando as configurações passadas via variável `minio_connection`.
- Define a lógica para ler os arquivos `.json` com **listas aninhadas**, desaninhá-las e transformar os dados em formato tabular.
- Salva os dados tratados no Data Lake no formato de **tabela Delta**.

---

Essa etapa é executada por meio do notebook `src_lnd_ibge_ipca_amplo.ipynb`, que possui o seguinte fluxo:

1. **Importação de bibliotecas**.
2. **Criação da conexão com o MinIO** via PySpark.
3. **Criação do SparkSession**.
4. **Leitura e transformação dos arquivos JSON com listas aninhadas**.
5. **Gravação no MinIO** no formato Delta.
6. **Configuração básica de logging** para monitoramento.

### 2. Visualização dos Dados no MinIO

Com os dados processados, é possível visualizar os arquivos diretamente no navegador:

- Acesse o MinIO via browser.
- Clique em **"Buckets"** → Selecione o bucket da camada `landing`.
- O lado esquerdo mostrará a **quantidade de objetos e uso de espaço**.
- Clique em **"Object Browser"** para navegar entre pastas e localizar os arquivos armazenados.

### 3. Correções Necessárias para Execução via Airflow

Durante os testes iniciais de execução da DAG via Airflow, foram encontrados alguns problemas que exigiram ações manuais para garantir o funcionamento correto. Abaixo estão os passos realizados:

#### Etapa 1 – Criação do Diretório de Logs no Container

Para que o Papermill possa salvar os notebooks de saída com os parâmetros executados, é necessário que o diretório de destino exista. Isso foi feito manualmente dentro do container do `airflow-worker`:

~~~bash
docker exec -it airflow-worker bash
mkdir -p /opt/airflow/logs/tasks/landing
~~~

> Esse caminho é utilizado automaticamente pelo PapermillOperator para armazenar os notebooks executados com parâmetros.

#### Etapa 2 – Adição da Tag `parameters` no Notebook

O Papermill precisa saber quais células contêm parâmetros que devem ser substituídos em tempo de execução. Para isso, a célula onde está a variável `minio_connection = ""` precisa ser marcada com a tag `"parameters"`.

**Passos para adicionar a tag no VS Code:**

1. Abra o notebook `src_lnd_ibge_ipca_amplo.ipynb`.
2. Localize a célula onde está `minio_connection = ""` (logo após as importações).
3. Clique com o botão direito sobre essa célula e selecione **"Edit Cell Tags"**.
4. Insira a seguinte tag:

~~~json
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "7681ecd0",
   "metadata": {
   "tags": [
  "parameters"
 ]  
   },
   "outputs": [],
   "source": [
    "minio_connection = \"\""
   ]
  },
~~~

5. Salve o notebook.

> Isso garante que o Airflow consiga injetar corretamente os parâmetros definidos no PapermillOperator ao executar o notebook.

### 4. Testes e Validação no Airflow

Com os ajustes feitos:

- A pasta de logs criada manualmente.
- A célula marcada com a tag `parameters`.

Foi possível executar a DAG no Airflow com sucesso. O notebook é processado, os dados são salvos corretamente no MinIO, e o output do notebook é armazenado em:

~~~bash
/opt/airflow/logs/tasks/landing/
~~~

Estrutura de Diretórios e Arquivos:  
Abaixo está a hierarquia de pastas utilizada para organizar os notebooks da DAG ibge_pme: 

~~~bash 

ibge/
└── ipca_amplo/
    ├── ibge_ipca_amplo.py
    ├── tasks/
    │   ├── landing/
    │   │   └── artifacts/
    │   │       └── src_lnd_ibge_ipca_amplo.ipynb
    │   ├── bronze/
    │   │   └── lnd_brz_ipca_amplo.ipynb
    │   └── silver/
    │       └── brz_slv_ipca_amplo.ipynb
    └── variables/
        └── minio_connection.json
~~~ 

Cada notebook é executado por um PapermillOperator. 

### 5. Automação da Coleta de Dados IBGE (API SIDRA)

Antes, foi realizado um teste manual para a coleta dos dados, definindo períodos específicos e fazendo múltiplas requisições para obter todos os arquivos desejados, no entanto, devido a quantidade de arquivos essa prática é inviável, especialmente devido à limitação de volume de dados que podem ser requisitados por vez na API do IBGE.

Para resolver esse problema, foi implementada uma automatização do processo de requisição, que agora gera de forma dinâmica todos os períodos do tipo YYYYMM desde janeiro de 2020 até a data atual. Com isso, a URL de consulta à API é montada automaticamente para cada período, garantindo que os dados sejam baixados de forma contínua, escalável e organizada. 

Essa automatização foi feita apenas no parâmetro periodos, onde o mesmo foi colocado em outra celula e com o uso da biblioteca datetime foi criado essa requisição mês a mês. 

Para garantir uma coleta estável e eficiente dos dados, foi adicionado um intervalo de 0.5 segundos entre cada requisição à API. Essa pausa tem dois objetivos principais: Evitar sobrecarga na API pública do IBGE e evitar sobrecarga no hardware local. 

# 17 DAG: Levantamento de Preços ANP (Camada Landing)

## Descrição do Projeto

Este projeto automatiza a extração, download e armazenamento de arquivos públicos da Agência Nacional do Petróleo (ANP) contendo séries históricas do levantamento de preços de combustíveis no Brasil.

Fonte dos Dados: 

https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/informacoes-levantamento-de-precos-de-combustiveis

A DAG (Directed Acyclic Graph) implementada foi desenvolvida para ser executada no Apache Airflow, fazendo a orquestração do processo de coleta dos dados, armazenamento no datalake (MinIO) e garantindo a organização dos dados na camada **landing**.

---

## Estrutura de Pastas e Arquivos

src/dags/
└── anp/serie_levantamento_precos/
├── tasks/
│ ├── bronze/
│ │ └── lnd_brz_anp_serie_levantamento_precos.ipynb # Notebook para camada bronze
│ └── landing/
│ ├── artifacts/ # Pasta para artefatos gerados no processo (logs, dados auxiliares)
│ ├── download/ # Pasta temporária onde arquivos baixados são armazenados localmente
│ └── src_lnd_anp_serie_levantamento_precos.ipynb # Notebook principal que executa a extração, download e upload para MinIO
├── variables/
│ └── minio_connection.json # Arquivo com credenciais de acesso ao MinIO
└── anp_serie_levantamento_precos.py # Script Python para execução da DAG no Airflow


### 1. Extração de URLs de arquivos

A DAG inicia acessando a página pública da ANP onde estão disponíveis os arquivos de séries históricas de preços.

Utilizamos um crawler simples implementado com a biblioteca **BeautifulSoup** para fazer o parsing da página HTML e extrair os links que contêm arquivos `.xls`, `.xlsx` e `.zip` que contenham no nome a palavra "mensal, só foram encontrados arquivos '.xlsx'.

Isso garante que apenas arquivos relevantes para o levantamento mensal sejam coletados.

### 2. Download dos arquivos

Com a lista de URLs em mãos, a DAG baixa os arquivos para uma pasta local temporária chamada `download`.

Cada arquivo é salvo localmente com seu nome original.

Durante o download, metadados são coletados, como nome do arquivo, link de origem, caminho local e data/hora do download.

Há um pequeno delay entre downloads para evitar sobrecarga no servidor da ANP.

### 3. Upload para o datalake (MinIO)

Após o download, os arquivos são enviados para o bucket `landing` dentro do datalake MinIO.

O caminho dentro do bucket segue o prefixo `anp/serie_levantamento_precos/` para organização.

O MinIO é uma solução de armazenamento compatível com S3 que permite armazenar os arquivos de forma segura e acessível para processos posteriores.

O upload é realizado via SDK MinIO em Python, utilizando as credenciais armazenadas em `variables/minio_connection.json`.

### 4. Limpeza

Depois que os arquivos são carregados no datalake, os arquivos temporários locais na pasta `download` são excluídos para economizar espaço.

---

## Motivação do Fluxo

- **Automação:** Garante que a coleta dos dados da ANP seja feita automaticamente e regularmente.  
- **Organização:** Arquivos são versionados e organizados em um datalake, facilitando o acesso e futuras etapas de processamento.  
- **Reprodutibilidade:** A DAG pode ser agendada e reexecutada pelo Airflow, mantendo o pipeline confiável e rastreável.  
- **Manutenção facilitada:** O código está organizado em notebooks e scripts para fácil manutenção e evolução.

---

## 18 Conversão de arquivos .xlsx da camada Landing para Bronze(formato Delta)

- Este pipeline tem como objetivo processar os arquivos `.xlsx` provenientes da camada **Landing**, realizar transformações padronizadas nos dados e armazená-los na camada **Bronze** no formato Delta Lake. Essa etapa é crucial para garantir a persistência confiável, leitura otimizada e versionamento dos dados brutos já padronizados, preservando sua integridade para uso nas próximas camadas (Silver e Gold).

---
### Estrutura de Entrada

Os arquivos de entrada estão localizados no diretório:

Origem (Landing): Arquivos .xlsx no bucket 
s3a://landing/anp/serie_levantamento_precos/

### Etapas do processo

- Inicialização da SparkSession com suporte a Excel
- Listagem dos arquivos da camada Landing

---
### Etapas Realizadas no Pipeline

1. Listagem dos Arquivos Fonte

- Função: listar_arquivos_xlsx()

- Descrição: Conecta-se ao bucket S3 Landing, filtra arquivos com extensão .xlsx usando o prefixo anp/serie_levantamento_precos/.

- Finalidade: Automatiza a detecção dos arquivos que devem ser processados.

2. Leitura dos Arquivos Excel com Layout Específico

- Função: ler_arquivos_anp()

- Descrição:

- Os arquivos possuem o cabeçalho a partir da linha de índice 11, por isso usamos o parâmetro header=11.

- Existem rodapés não estruturados que são ignorados com skipfooter=12.

- Algumas colunas extras sem nome são eliminadas com df.dropna(axis=1, how='all').

- A leitura é feita via pandas.read_excel() a partir do binário do objeto S3.

-Biblioteca usada:

io: para manipular os binários dos arquivos no formato BytesIO.

3. Conversão de DataFrame Pandas para Spark

Feita com:

df_spark = spark.createDataFrame(df_pandas)

Necessária para que os dados possam ser processados em escala distribuída e salvos como Delta.

4. Padronização dos Nomes das Colunas

- Função: limpar_nome_colunas_spark()

- O que faz:

- Remove acentos das colunas usando unicodedata.normalize().

- Substitui caracteres inválidos por - usando re.sub().

- Converte todos os nomes para minúsculo.

- Evita múltiplos underscores consecutivos.

- Bibliotecas usadas:

unicodedata: remove acentos (ex: "Preço" → "Preco").

re: expressões regulares, ou seja, são sequências de caracteres que formam um padrão.

5. Enriquecimento do DataFrame com Metadados

- Função: salvar_como_delta()

- Colunas adicionadas:

_carga_data: marcação de data/hora da carga (current_timestamp()).

_arquivo_origem: nome original do arquivo .xlsx, com lit(nome_arquivo).

### Estrutura de saída:

Os arquivos de saída estão localizados no diretório:

Destino (Bronze): Tabelas Delta salvas em 
s3a://bronze/anp/precos-combustiveis/

---