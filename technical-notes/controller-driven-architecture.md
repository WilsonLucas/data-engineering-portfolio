# Controller-driven Architecture — Parametrização de Pipeline via Tabelas Delta

**Categoria:** Padrão Arquitetural
**Stack:** Azure Databricks · Delta Lake · Azure Data Factory · PySpark · Unity Catalog

---

## O Problema que Motiva

Pipelines de dados crescem de formas que ninguém planeja no início. O que começa com 10 tabelas cresce para 50, depois 200, depois 400. Cada nova tabela, em uma arquitetura convencional, significa:

- Um novo job parametrizado no orquestrador (ADF, Airflow, etc.)
- Um novo notebook ou script com lógica ligeiramente diferente do anterior
- Um novo lugar para manter configuração de schema, fonte, destino e estratégia de escrita

O resultado é uma explosão de configuração hard-coded — notebooks quase idênticos que diferem em três linhas, pipelines ADF com dezenas de atividades Copy quase iguais, e nenhum lugar único que responda "quais tabelas fazem parte desta pipeline?"

A **controller-driven architecture** inverte essa lógica: a configuração vive em tabelas de dados (Delta), não em código. O código processa o que a tabela diz. Adicionar uma nova tabela à pipeline é uma operação de dados (INSERT), não uma operação de código (commit).

---

## Princípio Central

```
Arquitetura convencional:
  código define QUAIS tabelas processar
  código define COMO processar cada tabela
  → N tabelas = N configurações de código

Arquitetura controller-driven:
  tabela Delta define QUAIS tabelas processar e COM QUAIS PARÂMETROS
  código define COMO processar (genérico, parametrizado)
  → N tabelas = N linhas na tabela controller
```

---

## Estrutura das Tabelas Controller

A implementação típica usa 3-4 tabelas Delta em um schema dedicado (`<catalog>.controller`):

### 1. Tabela de Extração da Fonte (`source_to_lake`)

Define quais tabelas da fonte devem ser extraídas, com qual query e para qual destino.

```sql
-- DDL simplificado
CREATE TABLE controller.source_to_lake (
    id_tabela       INTEGER,
    schema_fonte    STRING,       -- schema no SQL Server de origem
    tabela_fonte    STRING,       -- nome da tabela de origem
    query_override  STRING,       -- query customizada (NULL = SELECT * sem filtro)
    path_destino    STRING,       -- path no ADLS de destino (landing zone)
    batch_flag      INTEGER,      -- agrupa tabelas em lotes para paralelismo ADF
    ativo           BOOLEAN,      -- flag para habilitar/desabilitar extração
    dt_atualizacao  TIMESTAMP
);
```

**Exemplo de dados:**

```
id | schema_fonte | tabela_fonte  | batch_flag | ativo
1  | dbo          | tb_apolice    | 1          | true
2  | dbo          | tb_sinistro   | 1          | true
3  | dbo          | tb_segurado   | 2          | true
4  | dbo          | tb_historico  | 2          | false  ← desabilitada sem alterar código
```

O ADF lê esta tabela via `Lookup` e distribui a extração por `batch_flag`:
- `batch_flag = 1`: primeiro lote (paralelo entre si)
- `batch_flag = 2`: segundo lote (executa após o primeiro)

---

### 2. Tabela de Ingestão Landing → Bronze (`landing_to_bronze`)

Define o mapeamento entre arquivos na zona landing e tabelas Bronze de destino.

```sql
CREATE TABLE controller.landing_to_bronze (
    id_tabela           INTEGER,
    path_landing        STRING,       -- path do arquivo na landing zone
    catalog_destino     STRING,       -- catalog Unity Catalog de destino
    schema_destino      STRING,       -- schema de destino
    tabela_destino      STRING,       -- nome da tabela Delta de destino
    schema_override     STRING,       -- schema explícito (NULL = inferência automática)
    estrategia_escrita  STRING,       -- "merge" ou "overwrite" ou "append"
    partition_col       STRING,       -- coluna de partição (NULL = sem partição)
    ativo               BOOLEAN
);
```

---

### 3. Tabela de Orquestração Silver (`silver_notebooks`)

Declara quais notebooks Silver executar e em que ordem/paralelismo.

```sql
CREATE TABLE controller.silver_notebooks (
    id_notebook         INTEGER,
    path_notebook       STRING,       -- path do notebook no workspace Databricks
    seq                 INTEGER,      -- nível de execução (mesmo seq = paralelo)
    timeout_minutos     INTEGER,      -- timeout máximo de execução
    ativo               BOOLEAN,
    dt_atualizacao      TIMESTAMP
);
```

**O campo `seq` é o coração da orquestração:**

```
seq=1: [notebook_clientes] [notebook_apolices] [notebook_produtos]  ← paralelo
seq=2: [notebook_sinistros] [notebook_cobertura]                    ← paralelo, após seq=1
seq=3: [notebook_fato_pricipal]                                     ← após seq=2
```

Adicionar dependência entre notebooks é uma questão de ajustar o `seq` — sem alterar o orquestrador.

---

### 4. Backup da Controller (`bckup_source`)

Snapshot versionado das outras tabelas controller, para auditoria e rollback.

```sql
CREATE TABLE controller.bckup_source (
    tabela_origem   STRING,
    snapshot_json   STRING,       -- conteúdo serializado da tabela em JSON
    dt_snapshot     TIMESTAMP,
    motivo          STRING        -- "pré-deploy", "pré-reprocessamento", etc.
);
```

---

## Como o Orquestrador Consume as Controller Tables

### No Azure Data Factory — Fase de Extração

```json
// Atividade Lookup — lê a tabela controller
{
  "name": "LookupSourceTables",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT * FROM controller.source_to_lake WHERE ativo = 1 AND batch_flag = @{pipeline().parameters.batch_num} ORDER BY id_tabela"
    }
  }
}

// Atividade ForEach — itera sobre o resultado do Lookup
{
  "name": "ForEachTable",
  "type": "ForEach",
  "typeProperties": {
    "isSequential": false,          // paralelo dentro do lote
    "batchCount": 10,               // máximo 10 em paralelo
    "items": "@activity('LookupSourceTables').output.value",
    "activities": [
      {
        "name": "CopyToLanding",
        "type": "Copy",
        "typeProperties": {
          "source": {
            "sqlReaderQuery": "@{if(equals(item().query_override, null), concat('SELECT * FROM ', item().schema_fonte, '.', item().tabela_fonte), item().query_override)}"
          },
          "sink": {
            "storeSettings": {
              "folderPath": "@item().path_destino"
            }
          }
        }
      }
    ]
  }
}
```

### No Databricks — Fase de Orquestração Silver

```python
# Notebook orquestrador — lê silver_notebooks e executa por seq
from pyspark.sql import functions as F

df_notebooks = spark.read.format("delta") \
    .load(f"{catalog}.controller.silver_notebooks") \
    .filter("ativo = true") \
    .orderBy("seq", "id_notebook")

# Agrupa por seq para controlar paralelismo
niveis = df_notebooks.select("seq").distinct().orderBy("seq").collect()

for nivel in niveis:
    seq_atual = nivel.seq
    notebooks_nivel = df_notebooks.filter(f"seq = {seq_atual}").collect()

    # Executar notebooks do mesmo nível em paralelo via threads
    from concurrent.futures import ThreadPoolExecutor

    def executa_notebook(row):
        dbutils.notebook.run(
            path=row.path_notebook,
            timeout_seconds=row.timeout_minutos * 60,
            arguments={"partition_date": partition_date}
        )

    with ThreadPoolExecutor(max_workers=len(notebooks_nivel)) as executor:
        futures = [executor.submit(executa_notebook, nb) for nb in notebooks_nivel]
        for f in futures:
            f.result()  # levanta exceção se algum notebook falhar

    print(f"Nível seq={seq_atual} concluído ({len(notebooks_nivel)} notebooks)")
```

---

## Operações Comuns via Controller Tables

### Adicionar uma nova tabela à pipeline

```sql
-- Adicionar extração do SQL Server
INSERT INTO controller.source_to_lake
(id_tabela, schema_fonte, tabela_fonte, path_destino, batch_flag, ativo)
VALUES (400, 'dbo', 'tb_nova_tabela', 'landing/tb_nova_tabela/', 2, true);

-- Adicionar mapeamento landing → Bronze
INSERT INTO controller.landing_to_bronze
(id_tabela, path_landing, catalog_destino, schema_destino, tabela_destino, estrategia_escrita, ativo)
VALUES (400, 'landing/tb_nova_tabela/', 'catalog_prod', 'bronze', 'tb_nova_tabela', 'merge', true);
```

Resultado: na próxima execução da pipeline, a nova tabela é extraída, convertida para Delta Bronze e processada automaticamente. Zero mudança de código.

### Desabilitar temporariamente uma tabela

```sql
UPDATE controller.source_to_lake
SET ativo = false
WHERE tabela_fonte = 'tb_problematica';
```

### Ajustar paralelismo do orquestrador Silver

```sql
-- Mover notebook para executar antes (seq menor)
UPDATE controller.silver_notebooks
SET seq = 1
WHERE path_notebook LIKE '%tb_clientes%';
```

---

## Quando Usar Controller-driven Architecture

Use este padrão quando:

1. A pipeline gerencia **muitas tabelas homogêneas** — a mesma lógica se aplica a dezenas ou centenas de entidades com parâmetros distintos
2. A equipe que **configura** a pipeline é diferente da equipe que **desenvolve** — analistas de dados podem adicionar tabelas sem precisar de um desenvolvedor
3. **Auditoria de configuração** é importante — o histórico de quais tabelas estavam ativas em qual versão é rastreável via `bckup_source` e via o próprio Delta Lake (Time Travel)
4. O ambiente precisa de **múltiplos ambientes** (dev/prod) com configurações distintas — basta ter schemas `controller` distintos por catalog

---

## Quando NÃO Usar

**Não use** quando:

1. A pipeline tem **poucas tabelas estáticas** (< 10) com lógicas muito distintas entre si — o overhead de manter a controller table não compensa
2. Cada tabela tem uma **lógica de transformação radicalmente diferente** — a controller parametriza configuração, não lógica. Se o código para cada tabela é diferente, a parametrização não resolve
3. A equipe não tem **disciplina de dados** — uma controller table desatualizada é pior que configuração hard-coded, porque cria falsa sensação de controle enquanto os dados ficam defasados

---

## Comparação com Alternativas

| Abordagem | Escalabilidade | Auditabilidade | Curva de Adoção |
|-----------|----------------|----------------|-----------------|
| Hard-coded por tabela | Baixa (1 artefato por tabela) | Baixa (no git) | Zero |
| Parâmetros no orquestrador | Média (limitado pela UI) | Média (via histórico do orquestrador) | Baixa |
| Controller tables (Delta) | Alta (INSERT = nova tabela) | Alta (Delta Time Travel) | Média |
| Config files (YAML/JSON) | Média (escala com arquivos) | Média (no git) | Baixa-Média |

---

## Referências

- [Delta Lake — Time Travel](https://docs.delta.io/latest/delta-utility.html)
- [Azure Data Factory — ForEach Activity](https://learn.microsoft.com/en-us/azure/data-factory/control-flow-for-each-activity)
- [Databricks — dbutils.notebook.run](https://docs.databricks.com/en/dev-tools/databricks-utils.html#dbutils-notebook)

---

*Padrão aplicado no [Case Study 01 — Controller-driven Medallion Architecture](../case-studies/01-controller-driven-medallion.md)*
