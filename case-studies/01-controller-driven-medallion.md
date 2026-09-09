# Case Study 01 — Controller-driven Medallion Architecture

**Setor:** Segurador (financial services)
**Empregador:** Dataside (consultoria)
**Papel:** Engenheiro de Dados Sênior — responsabilidade técnica integral
**Período:** Março de 2026 até hoje (em produção)

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

## Evolução do Projeto (maio a agosto de 2026)

Após a homologação das primeiras 17 tabelas Silver, o projeto entrou em produção e cresceu em três frentes: consolidação do MERGE em toda a estate, orquestração por DAG descoberto do código, e uma auditoria de arquitetura Azure no programa de especialização Microsoft.

### Cutover overwrite para MERGE em um dia

Em 23 de abril, as 17 Silver foram trocadas de overwrite para MERGE em 7 turnos e 4 lotes topológicos no mesmo dia. Uma semana depois, 9 Silver de outro engenheiro passaram pelo mesmo cutover, 3 dias antes do prazo. Em maio, com as ondas de scripts novos do cliente, a estate chegou a 31 Silver e 8 objetos Gold em MERGE; em agosto, cerca de 50 Silver e 40 Gold.

O padrão de cutover foi shadow validation: notebook e tabela paralelos com sufixo, três execuções seguidas com zero linhas gravadas na segunda e na terceira, e só então `ALTER TABLE RENAME` com DEEP CLONE de 7 dias para rollback. O protocolo de idempotência em três execuções virou o gate universal de aceite, aplicado em 100% das tabelas.

### Framework de carga em quatro versões

A função compartilhada de MERGE evoluiu de v2 a v4. A v2 trouxe comparação null-safe (`<=>`) no `WHEN MATCHED AND` e `WHEN NOT MATCHED BY SOURCE THEN DELETE`, dando semântica de snapshot atômico. A v3 passou a tratar diferença de schema em quatro casos: só adição (`ALTER TABLE ADD COLUMNS`), só remoção (habilita column mapping e faz DROP COLUMN), schema igual, e renomeação ou caso misto, que agora lança erro explícito no lugar de um sucesso silencioso que vinha pulando MERGEs. A v4 corrigiu o CREATE de Gold com chave surrogate vazia e trouxe o DELETE BY SOURCE para a Gold. Cada versão foi validada em sandbox de 5 cenários antes de promover.

### Orquestração DAG-aware descoberta do código

O ADF limita cada pipeline a 40 atividades e a estate tinha 61 nós. Em vez de declarar dependências à mão, uma ferramenta lê os notebooks reais do workspace e monta o grafo: 61 nós em 9 níveis, com 76 arestas estáticas confirmadas pela linhagem de runtime do Unity Catalog e zero falsos positivos. O ADF passou a disparar um único notebook orquestrador que executa o DAG com paralelismo máximo de 8 e retomada por id de execução. A ordem é descoberta do código, nunca declarada.

### Defeitos encontrados em produção

| Defeito | Causa raiz | Correção |
|---------|-----------|----------|
| 46.944 duplicatas estruturais na maior tabela de emissões, escondidas desde abril pelo overwrite | 37.742 chaves repetidas até 72 vezes | Dedup na Silver, visível só após o MERGE |
| 13 notebooks de RH que nunca executaram | 12 com coluna de merge "N/A" e 2 com espaço final ou NBSP (U+00A0) no nome do workspace | Corrigidos no mesmo dia, 13/13 confirmados na cadeia agendada seguinte |
| Loop de 205 updates por execução, para sempre | Perda de precisão DECIMAL(30,15) para (19,8) em coluna de cosseguro | Tipo alinhado |
| Produto cartesiano de 1,55 bilhão de linhas (esperado 161 mil) | Junção sem tratamento de versões | 3 CTEs de dedup SCD |
| 10 ids de assessoria perdidos em cascata | `COALESCE(..., 0)` upstream quebrando `IS NULL` | Dimensão de 51 para 61 ids; fato de 5 para 24 ids distintos |
| Dimensão com 70 linhas em vez de 51 | Metadado de data no GROUP BY e código SUSEP com 19 espaços à direita (SQL Server faz cast implícito, Spark não) | Trim e GROUP BY corrigidos |

### Incidentes

Em 27 de maio, um job desconhecido rodou notebooks em modo overwrite contra a estate já em MERGE, com upstream vazio, e zerou a cadeia inteira de emissões (6 Silver). Restauração topológica de emergência no mesmo dia, com um bug colateral encontrado e corrigido (apólice vinculada chegando em notação científica como string). Em julho, uma tabela foi reaproveitada por decisão do cliente após 3 alertas de risco documentados; a cadeia quebrou como previsto e foi restaurada assim que o cliente confirmou que o script tinha sido enviado por engano.

### Auditoria de arquitetura Azure (programa de especialização Microsoft)

Entre junho e julho, o projeto passou por auditoria de arquitetura com cinco assessments executados sobre a subscription do cliente:

| Assessment | Primeira rodada | Final |
|------------|-----------------|-------|
| Well-Architected Review | 49/100 | Moderado |
| FinOps Review | 96/234 | Moderado |
| Cloud Adoption Security (CASA) | 27/100 crítico | 46/100 após auditoria de evidências |
| Landing Zone Review | 53/197 crítico | 64/159 moderado, categorias críticas de 7 para 1 |
| Aderência a requisitos | 73,7% | 84,2% |

O achado de custo mais relevante: SFTP habilitado e ocioso em duas storage accounts, cobrado por hora de habilitação, cerca de R$ 26,8 mil por ano por uma feature sem uso. A estimativa da calculadora de preços foi reconciliada com o custo real em 11%, diferença explicada integralmente pelo uso de instâncias Spot em 661 das 974 horas de VM.

A lição operacional que mudou os números: sob permissão restrita a grupo de recursos, o CLI devolve vazio e não erro. Vários "zeros" do primeiro dia eram cegueira de permissão. Trocar para a conta de serviço correta reabriu Cost Management, deploys de produção, políticas e métricas, e foi o que levou o CASA de 27 para 63 e o Landing Zone de 53 para 64.

### Documentação como produto

Oito tipos de documento entregues ao cliente, gerados por código a partir do estado real da plataforma: Documento de Implementação Técnica (25 seções, do v1.0 ao v2.3), Especificação Funcional e Técnica (57 páginas, engenharia reversa de 66 notebooks), Documento de Otimização Técnica (31 recomendações em 10 áreas), relatório de Cost Management, status de requisitos, matriz de validação, plano de capacitação e cinco gabaritos bilíngues de assessment. O DIT referencia notebooks por URL estável de objeto, não por screenshot de código, e sobrevive a renomeações. Um pedido para retroagir a data de uma revisão foi recusado por escrito.

### Resultados acumulados

| Métrica | Valor |
|---------|-------|
| Silver em MERGE em produção | 31 em maio, cerca de 50 em agosto |
| Colunas na plataforma, verificadas contra a EFT | 1.910 (1.488 Silver e 422 Gold) |
| Volumetria do lake | 10,57 TiB no lake, 2,91 GiB na camada de BI |
| Maior tabela | 318 mil linhas e R$ 5,07 bilhões em emissões |
| Grafo de dependências | 61 nós, 9 níveis, 76 arestas confirmadas por linhagem, 0 falsos positivos |
| Duplicatas eliminadas | 46.944 |
| Tabelas de teste e diagnóstico removidas do catálogo | 66 |

---

## Padrões Aplicados

- [Controller-driven Architecture](../technical-notes/controller-driven-architecture.md) — a base da parametrização da pipeline
- [Metodologia de Homologação](../technical-notes/homologacao-metodologia.md) — como as 17 tabelas Silver foram validadas

---

## Lições Aprendidas

**A controller layer só funciona se for a fonte de verdade.** Em alguns momentos, houve tentação de "resolver rápido" editando diretamente o notebook em vez de atualizar a tabela controller. Cada exceção a essa regra criou inconsistência entre o estado real da pipeline e o que a controller declarava. A disciplina de sempre passar pela controller é o que mantém a arquitetura sustentável.

**Homologação linha-a-linha é lenta na primeira vez e barata para sempre.** As 17 tabelas Silver foram homologadas em ~6 semanas. Parece muito — mas em produção, zero retrabalho corretivo. Cada divergência documentada no processo de homologação revelou uma assunção de negócio não documentada na procedure T-SQL, que teria sido silenciosamente propagada para a camada Silver se não houvesse o processo de validação.

**A versão da CLI importa.** A quebra de API entre versões do Databricks CLI (v0.2xx → v0.297+) causou falhas silenciosas em scripts que funcionavam na versão anterior. Fixar a versão no script de setup e documentar a versão mínima é obrigatório em projetos com automação via CLI.
