# AGENTS.md — Checkpoint 2: Clean Architecture e POO

## 1. Persona e Visão Geral
Você é um(a) **Engenheiro(a) de Dados Sênior** especialista em Apache Spark e Clean Architecture.
O objetivo deste projeto é construir um pipeline de dados analítico e determinístico em PySpark para processar e identificar os **Top 10 Clientes** de um e-commerce com base no volume total de compras.

## 2. Princípios Arquiteturais e Separação de Camadas
* **Paradigma:** Orientação a Objetos (POO).
* **Clean Architecture:** Separação estrita entre lógica de configuração, I/O (leitura e escrita), lógica pura de transformação e orquestração do pipeline.
* **Injeção de Dependências:** O script `src/main.py` atua como *Composition Root*, instanciando as dependências (`SparkManager`, `DataIOManager`) e injetando-as no job de orquestração.
* **Transformações Puras:** As classes em `src/transforms/` devem conter métodos que recebem DataFrames e retornam DataFrames. Elas NÃO realizam leitura de disco, escrita nem manipulam a sessão Spark diretamente.
* **Config-Driven:** Caminhos de entrada e saída são centralizados em `config/config.yaml`.

## 3. Estrutura de Pastas Obrigatória
```text
.
├── config/             # Configurações do projeto
│   └── config.yaml
└── src/                # Código-fonte da aplicação
    ├── core/           # ConfigLoader e exceções de negócio
    ├── utils/          # SparkManager e Logging
    ├── data_io/        # DataIOManager (abstração de leitura e escrita)
    ├── transforms/     # Transformações analíticas puras (Top 10)
    ├── jobs/           # Orquestração do pipeline
    └── main.py         # Composition Root e ponto de entrada
```

## 4. Regras de Negócio e Critérios de Aceite
1. **Métrica de Ranqueamento:** `VALOR_UNITARIO * QUANTIDADE` somado por cliente (`SUM`).
2. **Esquema de Saída:** Exatamente as colunas `id_cliente` (Long), `nome_cliente` (String) e `valor_total_gasto` (Double).
3. **Determinismo e Desempate:** Ordenação por `valor_total_gasto` DESC, seguido de `id_cliente` ASC.
4. **Filtros:** Apenas clientes com compras ativas no período (Inner Join). Pedidos sem correspondência em clientes devem ser descartados.
5. **Volume de Saída:** Limite de 10 clientes no ranking final.

## 5. Datasets de Entrada
- **Clientes (JSON comprimido):** `./data/input/dataset-json-clientes/data/clientes.json.gz`
- **Pedidos (CSV comprimido, sep ';'):** `./data/input/datasets-csv-pedidos/data/pedidos/` (ler todos os arquivos `.csv.gz` contidos no diretório)

## 6. Definição de Pronto (DoD v2)
O código deve estar desacoplado nas camadas de `src/`, executando com sucesso via `spark-submit ./src/main.py` e gerando o resultado em `./data/output/top_10_clientes`.
