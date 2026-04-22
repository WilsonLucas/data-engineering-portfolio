# Case Study 01 — Controller-driven Medallion Architecture

**Setor:** Segurador (financial services)
**Empregador:** Dataside (consultoria)
**Papel:** Engenheiro de Dados Sênior — responsabilidade técnica integral

---

## Setor e Perfil do Projeto

Projeto end-to-end de modernização de plataforma analítica em uma seguradora nacional de médio-grande porte. A organização mantinha seus sistemas transacionais em SQL Server on-premises e extraía dados para análise por meio de pipelines ad hoc criados ao longo de anos — sem padrão arquitetural, sem catalogação centralizada, sem rastreabilidade de linhagem.

A migração para uma arquitetura Medallion moderna no Azure exigia não apenas mover dados, mas resolver o problema estrutural de como gerenciar, monitorar e evoluir centenas de tabelas ao longo do tempo sem depender de intervenção manual para cada mudança de schema ou nova fonte incorporada.

---

## Problema

**Técnico:** Como migrar 433 tabelas de SQL Server on-premises para Azure Databricks com Unity Catalog, mantendo rastreabilidade, idempotência e janelas curtas de ingestão (cargas a cada 30 minutos), sem criar janela de inconsistência para consumidores de BI downstream?

**Operacional:** Como garantir que a plataforma possa ser mantida e evoluída por um time que não tem o contexto de cada decisão de implementação — i.e., como embutir a governança da pipeline na própria pipeline?

**Escala do desafio:**
- 433 tabelas Bronze com schemas heterogêneos
- 35 tabelas Silver com regras de negócio acumuladas
- SQL Server on-premises como fonte — sem acesso direto por ferramentas cloud-native
- Exigência de equivalência semântica verificável entre a camada Silver nova e as procedures T-SQL legadas

---

## Stack Aplicada

| Componente | Tecnologia |
|------------|-----------|
| Orquestração | Azure Data Factory |
| Fonte | SQL Server on-premises |
| Gestão de segredos | Azure Key Vault |
| Storage (landing e lake) | Azure Data Lake Storage Gen2 |
| Processamento | Azure Databricks (Unity Catalog) |
| Formato de armazenamento | Delta Lake |
| Linguagem de transformação | PySpark + Spark SQL |
| Automação e deploy | Databricks CLI v0.297+ + Git Bash |

---

## Arquitetura

```
SQL Server              ADLS Gen2                 Azure Databricks
on-premises             ─────────                 ────────────────
                        landing/                  <catalog>.bronze
  [tabela_A]  ─────►  [tabela_A.csv]  ─────►    [tabela_A]  (Delta)
  [tabela_B]  ─────►  [tabela_B.csv]  ─────►    [tabela_B]  (Delta)
  [...]       ─────►  [...]           ─────►    [...]       (Delta)
  433 tabelas          (zona bruta,               433 tabelas
                        sem transformação)         433 tabelas
                                                  ──────────────
                                                  <catalog>.silver
                                                  [tabela_X] (Delta)
                                                  [tabela_Y] (Delta)
                                                  [...]
                                                  35 tabelas
                                        ▲
                                        │
                              <catalog>.controller
                              ┌─────────────────────────┐
                              │ source_to_lake     (399) │
                              │ landing_to_bronze  (360) │
                              │ silver_notebooks    (24) │
                              │ bckup_source              │
                              └─────────────────────────┘
```

**Camada Landing:** recebe os arquivos extraídos do SQL Server via ADF, no formato original, sem transformação. Funciona como zona de aterrissagem imutável — a fonte de verdade do que chegou.

**Camada Bronze:** conversão dos arquivos landing para formato Delta Lake. Schema idêntico à fonte, sem regras de negócio aplicadas. Cada tabela tem metadados de auditoria (`insert_date`, `update_date`) adicionados automaticamente.

**Camada Silver:** transformações de negócio, joins, agregações e enriquecimentos. Cada tabela Silver tem um notebook dedicado, homologado linha-a-linha contra a procedure T-SQL equivalente.

**Controller Layer:** o coração da arquitetura. Quatro tabelas Delta em `<catalog>.controller` que parametrizam toda a execução sem alterar código de notebooks.

---

## A Controller Layer — Decisão Central

A inovação arquitetural central do projeto é a separação completa entre *lógica de execução* (notebooks) e *configuração de execução* (tabelas Delta de controle).

**`source_to_lake` (399 registros):** Define quais tabelas do SQL Server devem ser extraídas, com qual query, para qual path no ADLS, e com qual flag de lote (`batch_flag`). O ADF lê essa tabela e distribui os jobs de extração em paralelo por lote — sem precisar de parâmetros hard-coded no pipeline.

**`landing_to_bronze` (360 registros):** Define o mapeamento entre arquivos landing e tabelas Bronze de destino, incluindo schema esperado, estratégia de escrita e eventuais transformações de tipo necessárias na conversão.

**`silver_notebooks` (24 registros):** Lista os notebooks Silver a serem executados pelo orquestrador, com um campo `seq` que define o nível de paralelismo intra-nível. Notebooks com o mesmo valor de `seq` podem rodar em paralelo; notebooks com `seq` distintos rodam sequencialmente entre si, respeitando dependências.

**`bckup_source`:** Snapshot versionado do estado das outras três tabelas controller, permitindo rollback e auditoria de mudanças de configuração ao longo do tempo.

```
Execução da pipeline (simplificada):

ADF Pipeline
├── Fase 1: Ler source_to_lake → distribuir extração SQL Server por batch_flag
│   ├── Lote 1: tabelas A, B, C ... (paralelo)
│   └── Lote 2: tabelas D, E, F ... (paralelo, após lote 1)
│
├── Fase 2: Ler landing_to_bronze → converter arquivos → Delta Bronze
│   └── Todos em paralelo (sem dependências entre si)
│
└── Fase 3: Ler silver_notebooks → executar notebooks por seq
    ├── seq=1: notebooks X, Y (paralelo)
    ├── seq=2: notebooks W, Z (paralelo, após seq=1)
    └── seq=3: notebook final (após seq=2)
```

---

## Decisões Técnicas-Chave

### 1. Overwrite → MERGE atômico ACID

**Problema:** A carga inicial usava `overwrite` para gravar as tabelas Silver. Isso criava uma janela de inconsistência de 5 a 15 minutos durante cada carga — qualquer query de BI executada nesse intervalo retornava dados parciais ou inconsistentes.

**Decisão:** Migrar para MERGE atômico ACID com a cláusula `WHEN NOT MATCHED BY SOURCE DELETE`.

```sql
MERGE INTO silver.tabela_destino AS target
USING (SELECT * FROM bronze.tabela_origem WHERE particao = :data) AS source
ON target.chave_negocio = source.chave_negocio
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED BY TARGET THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
```

**Trade-off avaliado:** O MERGE é mais lento que o overwrite e exige chaves de negócio estáveis. No contexto deste projeto, as tabelas Silver têm chaves bem definidas (herança das procedures T-SQL) e a eliminação da janela de inconsistência foi considerada inegociável pelo negócio.

**Resultado:** Zero reclamações de dados inconsistentes em BI após a migração para MERGE.

---

### 2. Schema Evolution com auditoria de campo

**Problema:** A migração de procedures T-SQL para PySpark revelou que a coluna de data de referência tinha nome diferente entre os dois ambientes (`dt_base` no legado vs. convenção nova).

**Decisão:** Formalizar o padrão de campos de auditoria — `insert_date` e `update_date` — e documentar a evolução de schema como parte do processo de homologação.

```python
# Configuração padrão em todos os notebooks Silver
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Campos de auditoria adicionados automaticamente
df = df.withColumn("insert_date", to_utc_timestamp(current_timestamp(), "GMT-3")) \
       .withColumn("update_date", to_utc_timestamp(current_timestamp(), "GMT-3"))
```

**Trade-off avaliado:** O `autoMerge.enabled` pode mascarar mudanças de schema não intencionais. Mitigamos com alertas de monitoramento e com o processo de homologação que valida o schema antes de promover de dev para prod.

---

### 3. Paralelismo controlado via campo `seq`

**Problema:** A execução sequencial de todos os 24 notebooks Silver era lenta (45+ minutos por ciclo) e subutilizava o cluster.

**Decisão:** Introduzir o campo `seq` na tabela `silver_notebooks` para declarar explicitamente o nível de paralelismo de cada notebook.

**Trade-off avaliado:** Paralelismo irrestrito poderia causar contenção de recursos no cluster e condições de corrida em tabelas com dependências. O campo `seq` dá controle fino: notebooks no mesmo nível rodam em paralelo, níveis distintos são barreiras de sincronização.

**Resultado:** Redução do ciclo Silver de 45+ minutos para aproximadamente 18 minutos com o mesmo tamanho de cluster.

---

### 4. Automação via Databricks CLI v0.297+

**Problema:** Deploy manual de notebooks entre ambientes (dev → prod) era lento, sujeito a erros e não rastreável.

**Decisão:** Automatizar deploy e validação via Databricks CLI com scripts Git Bash.

```bash
# Configuração necessária no Git Bash (Windows) para evitar conversão de paths
export MSYS_NO_PATHCONV=1

# Sintaxe nova CLI v0.297+ (jobs submit, não databricks runs submit)
databricks jobs submit --json '{
  "run_name": "deploy_silver_notebook",
  "existing_cluster_id": "<cluster_id>",
  "notebook_task": {
    "notebook_path": "/Shared/silver/tabela_x"
  }
}'

# Verificar output do run
databricks jobs get-run-output --run-id <run_id>
```

**Trade-off avaliado:** A CLI v0.297+ quebrou a sintaxe da versão anterior em vários pontos. A decisão foi documentar a versão mínima requerida e manter um script de setup de ambiente que valida a versão antes de executar.

---

### 5. Template padronizado de notebook Silver

**Problema:** Notebooks criados ad hoc por diferentes pessoas tinham estruturas incompatíveis, dificultando revisão, debug e onboarding.

**Decisão:** Criar um template obrigatório para todos os notebooks Silver com estrutura fixa.

```
# Estrutura obrigatória de notebook Silver:
#
# [Bloco 1] Header Markdown: nome da tabela, responsável, data, versão
# [Bloco 2] %run ../ingest_functions_uc  ← funções compartilhadas via %run
# [Bloco 3] Variáveis de configuração (catalog, schema, tabela, partition_date)
# [Bloco 4] Leitura da Bronze + transformações PySpark
# [Bloco 5] Chamada a carga_automatica() com parâmetros da tabela
```

**Resultado:** Onboarding de novos notebooks reduziu de 2-3 horas para 20-30 minutos usando o template como base.

---

## Resultados Quantificados

| Métrica | Valor |
|---------|-------|
| Tabelas Bronze catalogadas (Unity Catalog) | 433 |
| Tabelas Silver entregues | 35 |
| Tabelas Silver homologadas linha-a-linha vs. T-SQL legado | 17 |
| Tempo de ciclo Silver (antes / depois do paralelismo) | ~45 min → ~18 min |
| Janela de inconsistência para BI (antes / depois do MERGE) | 5-15 min → 0 |
| Notebooks Silver padronizados com template | 100% |

---

## Padrões Aplicados

- [Controller-driven Architecture](../technical-notes/controller-driven-architecture.md) — a base da parametrização da pipeline
- [Metodologia de Homologação](../technical-notes/homologacao-metodologia.md) — como as 17 tabelas Silver foram validadas

---

## Lições Aprendidas

**A controller layer só funciona se for a fonte de verdade.** Em alguns momentos, houve tentação de "resolver rápido" editando diretamente o notebook em vez de atualizar a tabela controller. Cada exceção a essa regra criou inconsistência entre o estado real da pipeline e o que a controller declarava. A disciplina de sempre passar pela controller é o que mantém a arquitetura sustentável.

**Homologação linha-a-linha é lenta na primeira vez e barata para sempre.** As 17 tabelas Silver foram homologadas em ~6 semanas. Parece muito — mas em produção, zero retrabalho corretivo. Cada divergência documentada no processo de homologação revelou uma assunção de negócio não documentada na procedure T-SQL, que teria sido silenciosamente propagada para a camada Silver se não houvesse o processo de validação.

**A versão da CLI importa.** A quebra de API entre versões do Databricks CLI (v0.2xx → v0.297+) causou falhas silenciosas em scripts que funcionavam na versão anterior. Fixar a versão no script de setup e documentar a versão mínima é obrigatório em projetos com automação via CLI.
