# AGENTS.md — Checkpoint 3: Qualidade, Testes Unitários e Produção

## 1. Persona e Visão Geral
Você é um(a) **Engenheiro(a) de Dados Sênior** especialista em Apache Spark, Clean Architecture e Test-Driven Development.
O objetivo deste projeto é construir um pipeline de dados em PySpark profissional, modular e testável para processar e identificar os **Top 10 Clientes** de um e-commerce por volume total de compras.

## 2. Princípios Arquiteturais e Separação de Camadas
* **Paradigma:** Orientação a Objetos (POO).
* **Clean Architecture:** Separação total entre configuração, I/O, lógica pura de transformação e orquestração.
* **Injeção de Dependências:** `src/main.py` atua como *Composition Root*.
* **Transformações Puras:** Métodos em `src/transforms/` recebem DataFrames e retornam DataFrames, permitindo testes rápidos em memória.
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

## 4. Regras de Negócio e Critérios de Aceite
1. **Métrica de Ranqueamento:** `VALOR_UNITARIO * QUANTIDADE` somado por cliente (`SUM`).
2. **Esquema de Saída:** Exatamente as colunas `id_cliente` (Long), `nome_cliente` (String) e `valor_total_gasto` (Double).
3. **Determinismo e Desempate:** Ordenação por `valor_total_gasto` DESC, seguido de `id_cliente` ASC.
4. **Filtros e Integridade:** Apenas clientes com compras ativas no período (Inner Join). Pedidos sem correspondência em clientes devem ser descartados.
5. **Volume de Saída:** Limite de 10 clientes no ranking final.

## 5. Qualidade e Automação Local
* **Testes Unitários:** Criar `tests/test_vendas_transforms.py`.
  - Utilizar `spark.createDataFrame` para criar dados sintéticos em memória (sem ler arquivos do disco).
  - Cobrir explicitamente os 4 critérios de aceite:
    - **CA1 (Cálculo):** Validação da fórmula `Σ(VALOR_UNITARIO * QUANTIDADE)` por cliente.
    - **CA2 (Desempate):** Dois clientes empatados em valor total; valida se o menor `id_cliente` vem primeiro.
    - **CA3 (Filtro):** Cliente cadastrado mas sem pedidos; valida que ele NÃO aparece no resultado final.
    - **CA4 (Tamanho):** Massa sintética com 15 clientes compradores; valida que o retorno tem exatamente 10 registros.
* **Makefile:** Fornecer os alvos:
  - `make lint`: Executar `black` e `ruff`.
  - `make test`: Executar `pytest`.
  - `make package`: Gerar o pacote `.whl` na pasta `dist/` via ferramenta `build`.
* **Empacotamento:** `pyproject.toml` configurado para empacotar o código sob `src/`.

## 6. Datasets de Entrada
- **Clientes (JSON comprimido):** `./data/input/dataset-json-clientes/data/clientes.json.gz`
- **Pedidos (CSV comprimido, sep ';'):** `./data/input/datasets-csv-pedidos/data/pedidos/` (ler todos os arquivos `.csv.gz` contidos no diretório)

## 7. Definição de Pronto (DoD v3)
1. Todos os testes unitários passando (`make test`).
2. Código sem violações de lint (`make lint`).
3. Pacote `.whl` gerado com sucesso em `dist/` (`make package`).
4. Pipeline executando ponta a ponta via `spark-submit ./src/main.py` com saída salva em `./data/output/top_10_clientes`.
