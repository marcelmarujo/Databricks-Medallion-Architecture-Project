# Projeto - Arquitetura Medallion no Databricks

Utilizei Databricks (+PySpark e SparkSQL) para processar e analisar dados com um fluxo de trabalho de três camadas: Bronze, Silver e Gold. A arquitetura é projetada para ingerir, melhorar a qualidade dos dados e suportar análises com agregaç~oes avançadas de BI. Inspirado no projeto do Edmilson Alves Infinity Big Data.

### Arquitetura Medalion

- **Camada Bronze**: Ingestão de dados em formato raw (JSON) a partir do Azure ADLS para o Databricks.
- **Camada Silver**: Processamento dos dados para substituição de valores faltantes, conversão de tipos de dados e armazenamento em formato Delta (Parquet).
- **Camada Gold**: Manipulação e agregação dos dados para criar uma base final otimizada para BI e análises.

## Passo a Passo

### 1. Camada Bronze: Ingestão de Dados
- **Objetivo**: Coletar dados brutos em formato JSON do Azure Data Lake diretamenta para o Databricks

### 2. Camada Silver: Limpeza e Transformação
- **Objetivo**: Melhorar a qualidade dos dados brutos.
  - Substituição de valores faltantes e nulos.
  - Conversão de tipos de dados para formatos apropriados.
  - Dados limpos e transformados são salvos em formato Delta (Parquet).

### 3. Camada Gold: Manipulação e Agregação
- **Objetivo**: Criar uma base de dados bem otimizada para BI e análises.
  - **Filtrar Colunas**: Seleção das colunas relevantes para análise.
  - **Criar Novas Colunas**: Adicionar uma coluna com o somatório de todas as batidas.
  - **Renomear Colunas**: Alterar nomes das colunas para refletir a linguagem de negócios.
  - **Excluir Dados**: Remover registros com classificações ["Sem Registro", "Não Informado"].
  - **Inserir Coluna de Atualização**: Adicionar uma coluna com a data da última atualização dos dados.
  - **Salvar Dados**: Armazenar os dados na camada Gold, particionados por UF (Estado).

## Tecnologias Utilizadas

- **Databricks**: Análise e processamento de dados.
- **Azure ADLS**: Armazenamento de dados não estruturados em nuvem.
- **Delta Lake**: Armazenamento e tratamento de dados em formato Parquet com suporte a transações ACID.
- **Python/Spark**: Para processamento de dados.
