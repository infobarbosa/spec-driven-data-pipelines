# Spec-Driven Data Pipelines: Engenharia de Software com PySpark e GenAI

> Construindo pipelines PySpark modulares, determinísticos e testáveis através de **Desenvolvimento Dirigido por Especificação** (Spec-Driven Development) e agentes de IA.

- **Autor:** Prof. Barbosa
- **Contato:** infobarbosa@gmail.com
- **GitHub:** [infobarbosa](https://github.com/infobarbosa)
- **Público:** Engenheiros de Dados (Pós-Graduação / Visão de Mercado)
- **Duração Estimada:** 2h30 a 3h
- **Pré-requisito conceitual:** Fundamentos de estruturação de projetos PySpark e Clean Architecture (vistos nas aulas do laboratório [pyspark-poo](https://github.com/infobarbosa/pyspark-poo)).

---

## 1. Visão Geral e Metodologia

Nas aulas anteriores da disciplina, construímos manualmente um pipeline PySpark seguindo 14 passos de engenharia de software: schemas explícitos, separação de I/O, classes de transformação pura, injeção de dependências, tratamento de erros, empacotamento e testes com PyTest.

O objetivo deste laboratório é conectar esse aprendizado com o uso prático de IA generativa: aprender a orientar o assistente de código para projetar, refatorar, testar e manter essa mesma arquitetura de forma ágil, consistente e com rigor técnico.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        CICLO DO SPEC-DRIVEN DEVELOPMENT (SDD)                          │
│                                                                                        │
│    1. ESPECIFICAR         2. PLANEJAR            3. EXECUTAR         4. AUDITAR        │
│    Definir regras,        Exigir plano           Agente refatora     Validar testes,   │
│    contratos e DoD ────>  estruturado do  ────>  código e cria  ────> linters e plano   │
│    no AGENTS.md           agente de IA           testes unitários    de execução       │
│         ▲                                                                 │            │
│         └────────────── 5. RECONCILIAR (Mudança de Requisito) ────────────┘            │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### O que é Spec-Driven Development (SDD)?

No fluxo mais comum e direto com IA (o prompt ad-hoc), o desenvolvedor conversa com o chat pedindo código solto. O resultado muitas vezes são scripts descartáveis, regras de negócio adivinhadas pela IA e falta de rastreabilidade.

No **Spec-Driven Development (SDD)**:
1. **O arquivo de especificação (`AGENTS.md`) é a única fonte da verdade**: Regras de negócio, contratos de schema, restrições arquiteturais e critérios de aceite residem no repositório.
2. **Plan-First (Modo Planejamento)**: O agente é expressamente proibido de alterar arquivos antes de submeter um plano detalhado à aprovação humana.
3. **Código como consequência**: O agente gera e refatora o código para atender à especificação.
4. **Reconciliação Contínua**: Quando o negócio muda, atualiza-se a especificação e comanda-se o agente a reconciliar o código e os testes automatizados.

---

## 2. Preparação do Ambiente e Datasets

No terminal do seu ambiente (GitHub Codespaces, container Docker ou máquina local), execute os comandos abaixo para preparar o projeto, instalar as dependências e baixar os datasets:

```sh
# 1. Criar a estrutura de pastas e acessar o projeto
mkdir -p top-10-clientes/data/{input,output}
cd top-10-clientes

# 2. Instalar dependências
pip install pyspark pytest ruff black pyyaml build

# 3. Baixar os datasets de entrada
git clone https://github.com/infobarbosa/dataset-json-clientes ./data/input/dataset-json-clientes
git clone https://github.com/infobarbosa/datasets-csv-pedidos ./data/input/datasets-csv-pedidos
```

---

## 3. Roteiro Prático: A Jornada em 4 Passos

O laboratório utiliza o mesmo domínio de negócio que você já domina: o cálculo dos **Top 10 Clientes de um e-commerce por volume total de compras**, cruzando transações de pedidos (`pedidos-*.csv.gz`) e cadastros de clientes (`clientes.json.gz`).

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             CRONOGRAMA DO LABORATÓRIO                            │
├───────────────────┬───────────────────┬───────────────────┬──────────────────────┤
│ Passo 1 (20 min)  │ Passo 2 (40 min)  │ Passo 3 (50 min)  │ Passo 4 (30 min)     │
│ O Risco do        │ SDD Nível 1:      │ SDD Nível 2:      │ O Teste de Fogo:     │
│ Prompt Ad-hoc     │ Contratos e       │ Clean Arch e      │ Reconciliação sob    │
│ (Sem Spec)        │ Determinismo      │ Testes (DoD)      │ Mudança de Requisito │
└───────────────────┴───────────────────┴───────────────────┴──────────────────────┘
```

---

### Passo 1 — O Ponto de Partida: O Risco do Prompt Ad-hoc (20 min)

Antes de adotarmos a metodologia de especificação, vamos observar como um modelo de IA reage quando recebe uma solicitação de negócio direta sem restrições explícitas.

#### 1.1 Enviando o Prompt Livre
Abra o assistente de IA disponível no seu ambiente (GitHub Copilot Chat ou agente de codificação) e envie o prompt:

```text
Atuando como Engenheiro de Dados Sênior especialista em Apache Spark, elabore um projeto PySpark que leia os arquivos de pedidos em ./data/input/datasets-csv-pedidos/data/pedidos/ e clientes em ./data/input/dataset-json-clientes/data/clientes.json.gz e gere um relatório com os Top 10 Clientes com base no valor total das compras.
```

Se o assistente sugerir a criação de um script (normalmente `main.py` na raiz), autorize a criação.

#### 1.2 Executando o Script Inicial
No terminal do Codespaces, execute o script gerado:

```sh
spark-submit ./main.py
```

O script provavelmente executará e exibirá dados no console. No entanto, analise o código com o olhar crítico de engenharia que desenvolvemos nas aulas de `pyspark-poo`.

#### 1.3 O que você deve analisar criticamente:

1. **Ordenação Não-Determinística no Spark:**
   - Como a IA ordenou o Top 10? Geralmente ela fez apenas:
     ```python
     df.groupBy("id_cliente").agg(sum("total")).orderBy(col("total").desc()).limit(10)
     ```
   - **O problema técnico:** Em um cluster distribuído, se dois ou mais clientes empatarem no 10º lugar com o mesmo valor financeiro, a ordenação sem critério de desempate explícito produz **resultados diferentes a cada execução**. Um relatório gerencial de diretoria não pode ter números oscilantes a cada reprocessamento.
2. **Decisões Ocultas sobre Clientes Inativos:**
   - A IA usou `left_join` ou `inner_join`? Se houver menos de 10 clientes compradores na base, clientes com R$ 0 gastos poderiam entrar no Top 10 se a regra não for explícita.
3. **Ausência de Contrato de Dados (Schema):**
   - Quais nomes de colunas ela atribuiu? `total`, `sum(valor)`, `valor_total_gasto`? Em ambientes corporativos, tabelas analíticas downstream e contratos de dados exigem nomes e tipos padronizados.
4. **Acoplamento e Código Macarrônico:**
   - Todos os caminhos de arquivo estão fixos no código (*hardcoded*).
   - Não há separação de camadas nem como testar a regra sem instanciar leitura de disco.

> **Conclusão do Passo 1:** Sem uma especificação formal, a IA preenche as lacunas com premissas próprias. O código pode até funcionar no cenário básico, mas traz regras implícitas, falta de determinismo e acoplamento desnecessário.

---

### Passo 2 — SDD Nível 1: Regras de Negócio, Contratos e Plan Mode (40 min)

Agora aplicamos a metodologia **Spec-Driven Development**. Vamos blindar as regras de negócio e os contratos de dados antes de permitir que a IA gere ou altere qualquer arquivo.

#### 2.1 Criando o `AGENTS.md` Inicial
Crie o arquivo `AGENTS.md` na raiz do projeto e copie o conteúdo do [Checkpoint 1](specs/checkpoints/AGENTS-v1-regras-e-contratos.md):

```markdown
# AGENTS.md — Pipeline Top 10 Clientes

## 1. Persona e Visão Geral
Você é um(a) Engenheiro(a) de Dados Sênior especialista em Apache Spark.
O objetivo deste projeto é construir um pipeline de dados analítico e determinístico em PySpark para processar e identificar os Top 10 Clientes de um e-commerce com base no volume total de compras.
O pipeline ingere dados de pedidos de `./data/input/datasets-csv-pedidos/data/pedidos/` e cruza com cadastros de clientes (`./data/input/dataset-json-clientes/data/clientes.json.gz`), consolidando o gasto acumulado de cada comprador.

## 2. Regras de Negócio e Critérios de Aceite
1. Métrica de Ranqueamento:
   - Valor do item: `VALOR_UNITARIO * QUANTIDADE`.
   - Total do cliente: soma (`SUM`) de todos os seus pedidos válidos.
2. Esquema e Contrato de Saída:
   - O DataFrame final deve conter estritamente as colunas:
     - `id_cliente` (Long)
     - `nome_cliente` (String)
     - `valor_total_gasto` (Double)
3. Determinismo e Regra de Desempate:
   - Ordenar estritamente por:
     1. `valor_total_gasto` em ordem DECRESCENTE (`DESC`).
     2. `id_cliente` em ordem CRESCENTE (`ASC`) como critério determinístico de desempate.
4. Filtros e Integridade de Dados:
   - Apenas clientes com compras ativas no período (Inner Join). Clientes sem pedidos NÃO aparecem no relatório.
   - Pedidos sem correspondência cadastral devem ser descartados.
5. Volume de Saída:
   - Exatamente os 10 maiores clientes (ou menos, se houver menos de 10 clientes válidos).

## 3. Diretriz de Configuração
Nenhum caminho de arquivo deve estar fixado no código. Utilize um arquivo `config/config.yaml` para mapear datasets de entrada e diretório de saída.

## 4. Definição de Pronto (DoD v1)
O pipeline deve ler configurações de `config/config.yaml`, processar os dados e salvar a saída determinística em `./data/output/top_10_clientes`.
```

#### 2.2 Exigindo o Modo Planejamento (Plan Mode)
No assistente de IA, envie o prompt forçando a elaboração de um plano prévio:

```text
Analise o arquivo @AGENTS.md.
NÃO altere nem crie nenhum arquivo de código ainda.
Apresente primeiro um plano passo a passo detalhando as modificações necessárias no projeto para atender rigorosamente às regras de negócio, ao determinismo de desempate e ao uso do arquivo config/config.yaml definidos na especificação.
```

#### 2.3 O que você deve auditar no plano da IA:
- O plano identificou a ordenação composta (`valor_total_gasto DESC`, seguido de `id_cliente ASC`)?
- O plano propôs a criação de `config/config.yaml` em vez de caminhos fixos?
- O plano garantiu que clientes inativos serão filtrados via Inner Join?

#### 2.4 Aprovando a Execução
Se o plano estiver correto, responda ao assistente:

```text
Plano aprovado. Prossiga com a implementação dos arquivos conforme planejado.
```

#### 2.5 Validação
Execute o pipeline atualizado:

```sh
spark-submit ./main.py
```

Confira a pasta de saída:
```sh
ls -lh ./data/output/top_10_clientes
```

**Resultado do Passo 2:** O pipeline agora é 100% determinístico, com contrato de dados estrito e configuração externa. Mas o código ainda está concentrado em um único script monolítico.

---

### Passo 3 — SDD Nível 2: Clean Architecture e Testes com PyTest (50 min)

Nas aulas de `pyspark-poo`, aprendemos que código profissional de engenharia de dados precisa de:
- Separação em camadas (`core`, `data_io`, `transforms`, `jobs`).
- Transformações puras desacopladas de I/O e de `SparkSession` para viabilizar testes sem cluster.
- Testes unitários com `pytest` rodando em memória com dados sintéticos em segundos.
- Automação via `Makefile` (`make lint`, `make test`, `make package`).

Em vez de refatorar classe por classe na mão, vamos elevar o nível da especificação no `AGENTS.md` e orientar o agente a fazer a refatoração completa.

#### 3.1 Atualizando a Especificação (`AGENTS.md`)
Substitua todo o conteúdo de `AGENTS.md` pelo [Checkpoint 3](specs/checkpoints/AGENTS-v3-qualidade-e-producao.md):

```markdown
# AGENTS.md — Pipeline Top 10 Clientes (Produção e Qualidade)

## 1. Persona e Princípios Arquiteturais
Você é um(a) Engenheiro(a) de Dados Sênior especialista em Apache Spark e Clean Architecture.
O projeto deve seguir estritamente:
- Paradigma: Orientação a Objetos (POO).
- Clean Architecture: Separação estrita entre core/config, data_io, transforms puras e jobs de orquestração.
- Injeção de Dependências: `src/main.py` atua como Composition Root.
- Transformações Puras: Métodos em `src/transforms/` recebem DataFrames e retornam DataFrames, sem executar I/O nem referenciar SparkSession.
- Config-Driven: Caminhos centralizados em `config/config.yaml`.

## 2. Estrutura de Pastas Obrigatória
.
├── config/
│   └── config.yaml
├── src/
│   ├── core/           # ConfigLoader e exceções
│   ├── utils/          # SparkManager e Logging
│   ├── data_io/        # DataIOManager (abstração de leitura/escrita)
│   ├── transforms/     # Lógica pura de transformação analítica
│   ├── jobs/           # Orquestração do pipeline
│   └── main.py         # Ponto de entrada (Composition Root)
├── tests/
│   └── test_vendas_transforms.py
├── pyproject.toml
└── Makefile

## 3. Regras de Negócio e Critérios de Aceite
1. Métrica: `VALOR_UNITARIO * QUANTIDADE` somado por cliente (`SUM`).
2. Schema de Saída: `id_cliente` (Long), `nome_cliente` (String), `valor_total_gasto` (Double).
3. Desempate Determinístico: `valor_total_gasto` DESC, seguido de `id_cliente` ASC.
4. Filtro: Apenas clientes com compras ativas (Inner Join).
5. Volume: Máximo de 10 clientes.

## 4. Qualidade e Automação Local
- Testes Unitários em `tests/test_vendas_transforms.py`:
  - Utilizar `spark.createDataFrame` com dados sintéticos em memória (sem ler disco).
  - Cobrir explicitamente:
    - CA1: Fórmula de agregação do valor total.
    - CA2: Desempate determinístico (dois clientes empatados no valor; menor id_cliente na frente).
    - CA3: Exclusão de clientes cadastrados sem pedidos.
    - CA4: Limite exato de 10 linhas.
- Makefile com alvos:
  - `make lint`: Executa black e ruff.
  - `make test`: Executa pytest.
  - `make package`: Gera pacote .whl em dist/ via build.
- Empacotamento via pyproject.toml para o pacote em src/.

## 5. Definição de Pronto (DoD)
1. Todos os testes unitários passando (`make test`).
2. Código sem violações de linter (`make lint`).
3. Empacotamento gerado com sucesso (`make package`).
4. Pipeline executando via `spark-submit ./src/main.py`.
```

#### 3.2 Solicitando o Plano de Refatoração Modular
Envie o prompt ao assistente:

```text
Com base nas diretrizes de Clean Architecture, Qualidade e Automação descritas no @AGENTS.md:
1. Apresente um plano passo a passo da refatoração de src/, da criação da suíte de testes com dados sintéticos em tests/ e dos arquivos Makefile e pyproject.toml.
2. Certifique-se de que a camada src/transforms/ não fará leitura de disco nem receberá a SparkSession diretamente.
Aguarde minha aprovação antes de alterar os arquivos.
```

#### 3.3 Aprovando e Executando a Refatoração
Revise se o plano respeitou o desacoplamento de transformações. Em seguida, aprove:

```text
Plano aprovado. Implemente a refatoração modular, os testes unitários e os arquivos de automação conforme especificado.
```

#### 3.4 Validando a Definição de Pronto (DoD)
No terminal do Codespaces, valide a entrega executando a suíte de automação:

```sh
# 1. Validar lint e formatação
make lint

# 2. Executar testes unitários em memória (roda em segundos)
make test

# 3. Gerar pacote Python distribuível
make package

# 4. Executar pipeline em produção
export PYTHONPATH=$(pwd)
spark-submit ./src/main.py
```

#### 3.5 Exercício de Auditoria: O Teste Pega Erro Real?
Para garantir que a IA não gerou "testes moles" (que passam mesmo com erro):
1. Abra o arquivo `src/transforms/` onde está a ordenação.
2. Inverta temporariamente o desempate de `.asc()` para `.desc()` no `id_cliente`.
3. Rode `make test` no terminal.
4. **Verifique:** O teste falhou acusando a quebra do critério de aceite CA2? Desfaça a alteração e confirme que os testes voltaram a passar.

---

### Passo 4 — O Teste de Fogo: Reconciliação sob Mudança de Requisito (30 min)

Aqui está a verdadeira demonstração de valor do Spec-Driven Development: **o que acontece quando os requisitos de negócio mudam em produção?**

No fluxo manual, você precisaria abrir múltiplos arquivos Python, alterar transformações, reescrever schemas e ajustar asserções de teste manualmente. Com SDD, atualizamos a especificação e delegamos a reconciliação.

#### 4.1 O Novo Cenário de Negócio
A diretoria da empresa aprovou a entrega, mas solicitou dois ajustes imediatos:
1. **Filtro Temporal:** Considerar apenas pedidos com `DATA_CRIACAO >= '2026-01-01T00:00:00'`.
2. **Nova Métrica e Desempate Composto:**
   - Adicionar a coluna `quantidade_total_itens` (soma de `QUANTIDADE`).
   - Novo critério de ordenação:
     1. `valor_total_gasto` DESC
     2. `quantidade_total_itens` DESC
     3. `id_cliente` ASC (desempate final determinístico)

#### 4.2 Atualizando Apenas a Especificação
Substitua o conteúdo de `AGENTS.md` pelo [Checkpoint 4](specs/checkpoints/AGENTS-v4-reconciliacao-mudanca-requisito.md).

Observe que você **não alterou nenhuma linha de código Python**.

#### 4.3 Comandando a Reconciliação com a IA
Envie a instrução de reconciliação para o assistente:

```text
Atualizei a especificação no @AGENTS.md com as novas regras de negócio solicitadas pela diretoria:
1. Filtro de pedidos com DATA_CRIACAO >= '2026-01-01T00:00:00'.
2. Inclusão da métrica quantidade_total_itens no schema de saída.
3. Desempate composto: valor_total_gasto DESC, quantidade_total_itens DESC e id_cliente ASC.

Reconcilie a camada src/transforms/, o contrato de dados e a suíte de testes em tests/test_vendas_transforms.py para que todos os critérios de aceite passem no make test.
```

#### 4.4 Validando a Reconciliação
Após o agente concluir as alterações nos arquivos, execute no terminal:

```sh
# Rodar a nova suíte de testes reconciliada
make test

# Executar o pipeline com as novas regras
export PYTHONPATH=$(pwd)
spark-submit ./src/main.py
```

Abra o arquivo de teste e inspecione o diff gerado:
- A IA adicionou os campos novos nos DataFrames sintéticos?
- O teste de desempate composto foi coberto?
- O código de transformação reflete a data de corte?

---

## 4. Comparativo de Produtividade: Manual vs. Spec-Driven

| Etapa de Engenharia | Abordagem Manual Tradicional | Abordagem Spec-Driven com GenAI |
| :--- | :--- | :--- |
| **Criação da Arquitetura Limpa** | 2 a 3 aulas digitando classes e contratos | 1 comando de planejamento + 1 refatoração guiada por spec |
| **Geração de Testes com PyTest** | Demorado; desenvolvedor costuma criar apenas 1 caso feliz | IA gera suíte completa com fixtures sintéticas em minutos |
| **Reconciliação por Mudança de Regra** | 30 a 60 min caçando código, schemas e testes | 2 min: edita a spec e dispara a reconciliação automática |
| **Garantia de Qualidade** | Depende da disciplina individual | Formalizada em contrato no `AGENTS.md` e validada por `Makefile` |

---

## 5. Boas Práticas para Produção com Agentes de IA

1. **Nunca autorize código sem plano prévio:** Exija o raciocínio da IA antes da edição de arquivos para evitar alucinações arquiteturais.
2. **Mantenha transformações puras:** Em pipelines Spark, isole computação de I/O. Isso torna os testes de IA instantâneos e baratos.
3. **Audite os testes da IA:** Provoque falhas deliberadas no código para garantir que a suíte gerada pela IA é rigorosa e não tautológica.
4. **Trate o `AGENTS.md` como código:** Versionar especificações no Git permite que qualquer membro da equipe (ou agente autônomo) mantenha o sistema com as mesmas premissas de engenharia.

---

## 6. Parabéns!

Você concluiu o laboratório de **Spec-Driven Data Pipelines com PySpark e GenAI**!

Nas aulas anteriores do projeto [pyspark-poo](https://github.com/infobarbosa/pyspark-poo), trabalhamos a estrutura de um pipeline passo a passo: a razão de cada schema explícito, a separação de leitura e escrita, as transformações puras e os testes automatizados.

Neste laboratório, você colocou tudo isso em prática com o apoio de ferramentas de IA generativa, experimentando um fluxo de trabalho moderno e estruturado:
- **Observou o comportamento da IA sem restrições:** Compreendeu por que prompts diretos e sem contexto geram soluções frágeis ou com regras de negócio implícitas.
- **Trabalhou com especificações claras:** Utilizou o `AGENTS.md` para registrar contratos de dados, regras determinísticas de desempate e critérios de aceite.
- **Adotou o modo planejamento:** Experimentou o valor de avaliar a proposta da IA antes de aplicar qualquer alteração aos arquivos.
- **Construiu testes de alta fidelidade:** Usou a IA para gerar testes unitários com dados sintéticos em memória, validando múltiplos cenários de negócio em segundos.
- **Experimentou a reconciliação prática:** Viu como atualizar um requisito na especificação permite adaptar o código e os testes de forma rápida e segura.

Independentemente do seu ponto de partida — seja dando os primeiros passos nesse ecossistema ou aprofundando práticas que você já utiliza no dia a dia —, o aprendizado mais valioso aqui é consolidar esse método: usar a inteligência artificial como uma alavanca de produtividade, mantendo sempre o senso crítico sobre a qualidade e o funcionamento do código.

---

## 7. Referências

### Livros
- **Clean Architecture: A Craftsman's Guide to Software Structure and Design** (Robert C. Martin): Leitura clássica sobre separação de responsabilidades, independência de frameworks e limites arquiteturais.
- **Designing Data-Intensive Applications** (Martin Kleppmann): Referência definitiva sobre confiabilidade, consistência, contratos de dados e modelos de dados em sistemas distribuídos.
- **Spark: The Definitive Guide** (Bill Chambers & Matei Zaharia): Guia completo e aprofundado sobre o funcionamento interno do Apache Spark, Catalyst Optimizer e planos de execução.
- **Clean Code: A Handbook of Agile Software Craftsmanship** (Robert C. Martin): Princípios de clareza, coesão, refatoração contínua e manutenibilidade de código.
- **Engenharia de Software Moderna** (Marco Tulio Valente): Excelente referência nacional sobre princípios fundamentais de engenharia de software, testes automatizados e design orientado a objetos.

### Metodologia e Desenvolvimento com IA
- [Spec-Driven Development: From Code to Contract in the Age of AI Coding Assistants](https://arxiv.org/abs/2602.00180): Artigo acadêmico de referência sobre a transição do foco em código manual para o desenvolvimento orientado a contratos e especificações formais com assistentes de IA.
- [Spec-Driven Development in 2026: What It Is, the Tooling, and How Teams Actually Use It](https://dev.to/krlz/spec-driven-development-in-2026-what-it-is-the-tooling-and-how-teams-actually-use-it-2fk2): Artigo prático cobrindo o estado da arte das ferramentas de mercado, padrões de governança (`AGENTS.md`) e como times de engenharia aplicam a metodologia no dia a dia.
- **Plan-First Workflow:** Padrão arquitetural em que agentes de codificação elaboram e submetem um plano detalhado antes de realizar alterações físicas em arquivos do repositório.
- **Shift-Left em Pipelines de Dados:** Prática de antecipar a validação de regras de negócio, tipagem de schemas e critérios de aceite para a fase de especificação prévia.

### Documentação Oficial e Ferramentas
- [Apache Spark — Documentação Oficial](https://spark.apache.org/docs/latest/): Configurações, APIs de DataFrames e SQL, e boas práticas de tuning.
- [PySpark — API Reference](https://spark.apache.org/docs/latest/api/python/): Referência técnica detalhada de módulos e classes do PySpark.
- [Pytest — Documentação Oficial](https://docs.pytest.org/): Criação de testes unitários, fixtures e asserções em Python.
- [Ruff — Linter e Formatador](https://docs.astral.sh/ruff/): Ferramenta de altíssima performance para validação de estilo e qualidade de código Python.
- [GNU Make Manual](https://www.gnu.org/software/make/manual/): Referência para automação de tarefas e padronização de rotinas de desenvolvimento local.

### Artigos e Blogs Técnicos
- [Databricks Engineering Blog](https://www.databricks.com/blog/category/engineering): Artigos técnicos aprofundados sobre boas práticas, testes com PySpark e otimização de queries distribuídas.
- [GitHub Blog — AI & Developer Experience](https://github.blog/): Casos de uso reais, métricas de produtividade e evolução de agentes de inteligência artificial aplicados ao ciclo de vida de desenvolvimento de software.
- [PEP 8 — Style Guide for Python Code](https://peps.python.org/pep-0008/): Guia oficial de estilo de código para a linguagem Python.

---

## Apêndice — E quando o AGENTS.md fica grande demais? A Cadeia de Especificações Modulares

Em projetos corporativos com múltiplos pipelines e tabelas, manter todas as definições em um único arquivo `AGENTS.md` pode gerar acoplamento de responsabilidades. Em uma equipe multidisciplinar, uma pergunta fundamental surge: **quem revisa o quê?**

- A liderança de **Negócios** valida a intenção estratégica e o valor gerado.
- A equipe de **Analytics** valida regras de cálculo, fórmulas de agregação e desempate.
- Os **Analistas e Engenheiros de Dados** validam contratos de schemas, tipos, particionamento e integridade.
- A equipe de **QA / Testes** valida os critérios de aceite e cenários de borda.
- A equipe de **Engenharia de Software** valida arquitetura, separação de camadas, empacotamento e qualidade local.

### A Solução: Decomposição em Specs Modulares

Para viabilizar governança clara no versionamento e otimizar a janela de contexto dos modelos de IA, a abordagem recomendada é dividir a especificação em arquivos especializados:

```text
meu-projeto/
├── specs/
│   ├── 01-intencao-de-negocio.md    # [Negócios] Contexto, impacto e objetivos
│   ├── 02-regras-e-metricas.md      # [Analytics] Fórmulas, grão e desempate
│   ├── 03-contratos-de-dados.md     # [Eng. Dados] Schemas estritos e caminhos de I/O
│   └── 04-criterios-de-aceite.md    # [QA/Engenharia] Cenários para suíte de testes
├── AGENTS.md                        # [Engenharia] Orquestrador técnico enxuto e DoD
└── src/                             # Código da aplicação
```

### O Ganho Prático na Interação com a IA

Com especificações modulares:
1. **Pull Requests com donos claros:** Cada área revisa apenas o arquivo que lhe compete no repositório (`CODEOWNERS`).
2. **Contexto sob demanda:** O assistente de IA é alimentado apenas com a especificação necessária para a tarefa do momento, reduzindo ruído e melhorando a precisão da resposta.
3. **Reconciliação focada:** Se uma regra analítica mudar, edita-se apenas `specs/02-regras-e-metricas.md` e solicita-se ao agente:
   ```text
   As regras em @specs/02-regras-e-metricas.md foram atualizadas. 
   Reconcilie a camada de transformação e os testes unitários preservando os contratos de dados de @specs/03-contratos-de-dados.md.
   ```

---

### Exemplo Prático: O Projeto Top 10 Clientes Decomposto

Abaixo está a demonstração de como o nosso projeto do laboratório seria estruturado segundo essa abordagem modular, definindo no próprio corpo de cada documento a persona responsável, o escopo de atuação e os critérios de validação:

#### 1. `specs/01-intencao-de-negocio.md`
```markdown
# 01 — Intenção de Negócio (Briefing Estratégico)

| Metadado | Definição |
| :--- | :--- |
| **Persona Responsável** | Product Owner (PO) / Liderança Comercial |
| **Revisores Obrigatórios** | Head de CRM e Gerente de E-commerce |
| **Status** | Aprovado (v1.0) |
| **Papel na Cadeia SDD** | Define a motivação estratégica, metas e impacto de negócio (o *porquê*) |

## 1. Contexto e Motivação
A área comercial do e-commerce está estruturando o programa anual de fidelidade para os maiores compradores da plataforma. Para direcionar benefícios exclusivos (como cashback diferenciado e atendimento prioritário), a equipe precisa de visibilidade exata sobre a concentração de faturamento na base de clientes.

## 2. Perguntas-Chave de Negócio
- Quem são os 10 clientes com maior volume financeiro acumulado?
- Qual é o volume financeiro acumulado por cada um desses clientes?
- O ranking é consistente e confiável para fins de auditoria de campanhas?

## 3. Critérios de Sucesso do Produto
- **Determinismo:** O ranking deve produzir exatamente o mesmo resultado a cada execução para a mesma base histórica.
- **Auditoria:** Cada cliente classificado deve possuir histórico de compras comprovado.
- **Fora de Escopo:** Segmentação geográfica ou filtros de categoria de produto não fazem parte desta versão.
```

#### 2. `specs/02-regras-e-metricas.md`
```markdown
# 02 — Regras Analíticas e Métricas

| Metadado | Definição |
| :--- | :--- |
| **Persona Responsável** | Analytics Engineer / Analista de BI |
| **Revisores Obrigatórios** | Analista de Negócios e Engenheiro de Dados |
| **Status** | Aprovado (v1.0) |
| **Papel na Cadeia SDD** | Define fórmulas de cálculo, granularidade e lógica de desempate (o *quê*) |

## 1. Grão e Granularidade Analítica
- **Grão de Entrada (Pedidos):** Linha de item de pedido (`ID_PEDIDO`, `PRODUTO`).
- **Grão de Saída (Relatório):** Um registro por cliente único (`id_cliente`).

## 2. Fórmulas e Regras de Agregação
- **Valor por Item:** `VALOR_UNITARIO * QUANTIDADE`.
- **Gasto Total Acumulado:** Soma financeira (`SUM`) de todas as linhas de pedido válidas associadas ao cliente:
  $$\text{valor\_total\_gasto} = \sum (\text{VALOR\_UNITARIO} \times \text{QUANTIDADE})$$

## 3. Política de Ranqueamento e Desempate
Para garantir determinismo em ambiente distribuído (Apache Spark):
1. **1º Critério:** `valor_total_gasto` em ordem DECRESCENTE (`DESC`).
2. **2º Critério (Desempate Mandatório):** `id_cliente` em ordem CRESCENTE (`ASC`).

## 4. Regras de Inclusão e Filtros
- **Apenas Compradores Ativos:** Clientes cadastrados que não possuam nenhum pedido registrado NÃO devem figurar no ranking (uso obrigatório de `Inner Join`).
- **Tamanho do Corte:** O ranking deve retornar no máximo 10 registros (ou a totalidade de compradores se a base tiver menos de 10).
```

#### 3. `specs/03-contratos-de-dados.md`
```markdown
# 03 — Contratos de Dados e Interfaces Técnicas

| Metadado | Definição |
| :--- | :--- |
| **Persona Responsável** | Engenheiro(a) de Dados / Arquiteto(a) de Dados |
| **Revisores Obrigatórios** | Analytics Engineer e Engenheiro de Plataforma |
| **Status** | Aprovado (v1.0) |
| **Papel na Cadeia SDD** | Formaliza schemas estritos, formatos físicos, partições e integridade (com *quais dados*) |

## 1. Interface de Entrada (Sources)
- **Dataset de Clientes:**
  - Formato: JSON comprimido (`clientes.json.gz`).
  - Caminho: gerenciado via parâmetro `datasets.clientes` em `config/config.yaml`.
  - Campos obrigatórios: `id` (Long, PK), `nome` (String).
- **Dataset de Pedidos:**
  - Formato: Diretório com múltiplos CSV comprimidos (`.csv.gz`, sep `;`, header presente).
  - Caminho: gerenciado via parâmetro `datasets.pedidos` em `config/config.yaml`.
  - Campos obrigatórios: `ID_CLIENTE` (Long, FK), `VALOR_UNITARIO` (Double), `QUANTIDADE` (Integer).

## 2. Contrato de Saída (Sink Schema)
- **Destino:** Salvo no diretório configurado em `config/config.yaml` (`output.caminho`).
- **Schema Estrito (PySpark StructType):**
  - `id_cliente`: `LongType` (Nullable = False)
  - `nome_cliente`: `StringType` (Nullable = False)
  - `valor_total_gasto`: `DoubleType` (Nullable = False)

## 3. Invariantes de Integridade
- **Integridade Referencial:** Pedidos com `ID_CLIENTE` ausente na base cadastral de clientes devem ser descartados.
- **Validação de Nulos:** Nenhuma coluna do DataFrame final pode conter valores `NULL` ou `NaN`.
```

#### 4. `specs/04-criterios-de-aceite.md`
```markdown
# 04 — Critérios de Aceite para Testes Automatizados

| Metadado | Definição |
| :--- | :--- |
| **Persona Responsável** | QA / Engenheiro(a) de Qualidade de Dados (SDET) |
| **Revisores Obrigatórios** | Analytics Engineer e Engenheiro de Software |
| **Status** | Aprovado (v1.0) |
| **Papel na Cadeia SDD** | Formaliza asserções de teste automatizado e massas sintéticas (*como provar*) |

## 1. Diretriz de Execução da Suíte
Os testes devem ser implementados com `pytest` em `tests/test_vendas_transforms.py`, gerando DataFrames em memória via `spark.createDataFrame` (execução hermética sem leitura de disco).

## 2. Critérios de Aceite Obrigatórios (DoD de Testes)

### CA1 — Exatidão da Métrica Financeira
- **Dado:** Um cliente com 2 pedidos (Item A: R$ 100,00 x 2; Item B: R$ 50,00 x 3).
- **Quando:** O pipeline calcular o total gasto.
- **Então:** O `valor_total_gasto` retornado deve ser exatamente R$ 350,00.

### CA2 — Desempate Determinístico sob Empate Perfeito
- **Dado:** Cliente A (`id_cliente = 10`) e Cliente B (`id_cliente = 2`), ambos com gasto de R$ 1.000,00.
- **Quando:** O ranking for ordenado.
- **Então:** O Cliente B (`id_cliente = 2`) deve anteceder o Cliente A na ordenação.

### CA3 — Exclusão de Clientes Sem Compras
- **Dado:** Um cliente cadastrado na base cadastral que possui zero pedidos vinculados.
- **Quando:** A transformação cruzar pedidos e clientes.
- **Então:** Esse cliente NÃO deve constar em nenhuma posição do DataFrame final.

### CA4 — Cumprimento do Limite de Linhas
- **Dado:** Uma massa sintética com 15 clientes distintos que realizaram compras.
- **Quando:** A ordenação e corte forem aplicados.
- **Então:** O DataFrame de saída deve conter exatamente 10 linhas.
```

#### 5. `AGENTS.md` (O Manifesto Técnico Enxuto do Repositório)
```markdown
# AGENTS.md — Manifesto do Repositório e Governança Técnica

| Metadado | Definição |
| :--- | :--- |
| **Persona Responsável** | Engenheiro(a) de Software / Tech Lead |
| **Revisores Obrigatórios** | Time de Engenharia de Dados |
| **Status** | Aprovado (v1.0) |
| **Papel na Cadeia SDD** | Orquestra a cadeia de specs, impõe arquitetura, linters e padrões de entrega (*como construir*) |

## 1. Cadeia de Especificações (Fonte da Verdade)
O comportamento funcional e os contratos deste repositório são governados exclusivamente pelos documentos em `specs/`:
- Intenção Estratégica: `@specs/01-intencao-de-negocio.md`
- Lógica de Negócio e Ranqueamento: `@specs/02-regras-e-metricas.md`
- Contratos de Dados e Schemas: `@specs/03-contratos-de-dados.md`
- Critérios de Aceite e Asserções: `@specs/04-criterios-de-aceite.md`

## 2. Diretrizes de Arquitetura e Engenharia
- **Estrutura Modular em `src/`:**
  - `src/core/`: Leitura de configurações e exceções customizadas.
  - `src/utils/`: Gerenciamento de SparkSession (`SparkManager`) e logging.
  - `src/data_io/`: Camada de I/O desacoplada (`DataIOManager`).
  - `src/transforms/`: Classes de transformações analíticas puras (recebem e retornam DataFrames, sem I/O direto).
  - `src/jobs/`: Orquestração do pipeline.
  - `src/main.py`: Composition Root e injeção de dependências.
- **Config-Driven:** Nenhum caminho físico deve estar no código; utilizar `config/config.yaml`.

## 3. Definição de Pronto (DoD)
Antes de submeter código para produção, o agente de IA deve garantir:
1. 100% dos testes unitários passando em memória (`make test`).
2. Conformidade total de formatação e lint com `black` e `ruff` (`make lint`).
3. Empacotamento válido da biblioteca via `pyproject.toml` (`make package`).
4. Pipeline executando ponta a ponta via `spark-submit ./src/main.py`.
```

