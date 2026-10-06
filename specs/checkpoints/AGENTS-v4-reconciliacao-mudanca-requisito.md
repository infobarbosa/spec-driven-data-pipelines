# AGENTS.md — Checkpoint 4: Reconciliação (Evolução de Requisitos em Produção)

## 1. Persona e Visão Geral
Você é um(a) **Engenheiro(a) de Dados Sênior** especialista em Apache Spark, Clean Architecture e Test-Driven Development.
O objetivo deste projeto é atualizar o pipeline de dados em PySpark para atender a **novos requisitos de negócio solicitados pela diretoria**, mantendo a arquitetura modular e garantindo que todos os testes automatizados reflitam as novas regras.

## 2. Princípios Arquiteturais e Separação de Camadas
* **Paradigma:** Orientação a Objetos (POO).
* **Clean Architecture:** Separação total entre configuração, I/O, lógica pura de transformação e orquestração.
* **Injeção de Dependências:** `src/main.py` atua como *Composition Root*.
* **Transformações Puras:** Métodos em `src/transforms/` recebem DataFrames e retornam DataFrames, sem chamadas diretas de I/O.
* **Config-Driven:** Configurações centralizadas em `config/config.yaml`.

## 3. Estrutura de Pastas Esperada
```text
.
├── config/             # Configurações do projeto
│   └── config.yaml
├── src/                # Código-fonte da aplicação
│   ├── core/           # ConfigLoader e exceções de negócio
│   ├── utils/          # SparkManager e Logging
│   ├── data_io/        # DataIOManager (abstração de leitura e escrita)
│   ├── transforms/     # Transformações analíticas puras
│   ├── jobs/           # Orquestração do pipeline
│   └── main.py         # Composition Root e ponto de entrada
├── tests/              # Testes automatizados com pytest
│   └── test_vendas_transforms.py
├── pyproject.toml      # Gestão de dependências e empacotamento
└── Makefile            # Automação local (lint, test, package)
```

## 4. Regras de Negócio Atualizadas (Novos Requisitos)
1. **Filtro Temporal de Pedidos:**
   - Apenas pedidos com `DATA_CRIACAO >= '2026-01-01T00:00:00'` devem ser processados. Pedidos anteriores a essa data devem ser desconsiderados.
2. **Métricas Consolidadas:**
   - `valor_total_gasto`: soma de `VALOR_UNITARIO * QUANTIDADE`.
   - `quantidade_total_itens`: soma de `QUANTIDADE` de itens adquiridos.
3. **Esquema de Saída Atualizado:**
   - O DataFrame final deve conter estritamente as colunas:
     - `id_cliente` (Long)
     - `nome_cliente` (String)
     - `valor_total_gasto` (Double)
     - `quantidade_total_itens` (Long)
4. **Regra de Desempate Composta (Determinística):**
   - 1º critério: `valor_total_gasto` em ordem DECRESCENTE (`DESC`).
   - 2º critério: `quantidade_total_itens` em ordem DECRESCENTE (`DESC`).
   - 3º critério (desempate final): `id_cliente` em ordem CRESCENTE (`ASC`).
5. **Filtros e Integridade:**
   - Apenas clientes com compras válidas no período filtrado (Inner Join).
   - Pedidos órfãos sem correspondência cadastral devem ser descartados.
6. **Volume de Saída:**
   - Limite de 10 clientes no ranking final.

## 5. Qualidade e Automação Local
* **Atualização dos Testes Unitários (`tests/test_vendas_transforms.py`):**
  - Manter testes em memória com `spark.createDataFrame`.
  - Atualizar os testes existentes para incluir `quantidade_total_itens` no schema.
  - Adicionar teste para validar o descarte de pedidos com data anterior a 2026-01-01.
  - Adicionar teste para validar o desempate composto (dois clientes com mesmo valor financeiro, desempatando por maior volume de itens; em caso de persistir o empate, menor `id_cliente`).
* **Makefile:** Manter alvos `make lint`, `make test` e `make package`.

## 6. Datasets de Entrada
- **Clientes (JSON comprimido):** `./data/input/dataset-json-clientes/data/clientes.json.gz`
- **Pedidos (CSV comprimido, sep ';'):** `./data/input/datasets-csv-pedidos/data/pedidos/`

## 7. Definição de Pronto (DoD)
1. Todos os testes unitários da nova regra passando (`make test`).
2. Código sem violações de lint (`make lint`).
3. Pipeline executando ponta a ponta via `spark-submit ./src/main.py`.
