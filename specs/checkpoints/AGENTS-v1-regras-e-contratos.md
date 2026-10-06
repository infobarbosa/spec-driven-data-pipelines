# AGENTS.md — Checkpoint 1: Regras de Negócio e Contratos de Dados

## 1. Persona e Visão Geral
Você é um(a) **Engenheiro(a) de Dados Sênior** especialista em Apache Spark.
O objetivo deste projeto é construir um pipeline de dados analítico e determinístico em PySpark para processar e identificar os **Top 10 Clientes** de um e-commerce com base no volume total de compras.
O pipeline ingere dados transacionais de pedidos de `./data/input/datasets-csv-pedidos/data/pedidos/` (todos os arquivos `.csv.gz`) e cruza com os dados cadastrais de clientes (`clientes.json.gz`), consolidando o gasto acumulado de cada comprador e produzindo um relatório ordenado.

## 2. Regras de Negócio e Critérios de Aceite
1. **Métrica de Ranqueamento:**
   - O valor de cada item de pedido é calculado por: `VALOR_UNITARIO * QUANTIDADE`.
   - O total gasto por cliente é a soma (`SUM`) de todos os seus pedidos válidos no período.
2. **Esquema e Contrato de Saída:**
   - O DataFrame final deve conter estritamente as seguintes colunas e tipos:
     - `id_cliente` (Long)
     - `nome_cliente` (String)
     - `valor_total_gasto` (Double)
3. **Determinismo e Regra de Desempate:**
   - Em processamento distribuído (Apache Spark), ordenações com empates geram resultados não-determinísticos se não houver critério de desempate explícito.
   - O ranking DEVE ordenar estritamente por:
     1. `valor_total_gasto` em ordem DECRESCENTE (`DESC`).
     2. `id_cliente` em ordem CRESCENTE (`ASC`) como critério determinístico de desempate.
4. **Filtros e Integridade de Dados:**
   - Apenas clientes com compras ativas no período devem constar no ranking (Inner Join). Clientes sem pedidos NÃO devem aparecer no relatório.
   - Pedidos cujo `ID_CLIENTE` não possua correspondência na base de clientes devem ser descartados.
5. **Volume de Saída:**
   - O relatório deve conter exatamente os 10 maiores clientes (ou menos, se houver menos de 10 clientes válidos).

## 3. Diretriz de Configuração
Nenhum caminho de arquivo deve estar fixado ("hardcoded") no código. Utilize um arquivo `config/config.yaml` para mapear os datasets de entrada e o diretório de saída.

## 4. Datasets de Entrada
- **Clientes (JSON comprimido):** `./data/input/dataset-json-clientes/data/clientes.json.gz`
- **Pedidos (CSV comprimido, sep ';'):** `./data/input/datasets-csv-pedidos/data/pedidos/` (ler todos os arquivos `.csv.gz` contidos no diretório)

## 5. Definição de Pronto (DoD v1)
O pipeline deve carregar as configurações de `config/config.yaml`, processar os dados brutos e salvar o ranking determinístico em `./data/output/top_10_clientes`.
