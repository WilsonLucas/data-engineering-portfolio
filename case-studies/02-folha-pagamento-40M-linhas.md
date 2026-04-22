# Case Study 02 — People Analytics: Folha de Pagamento com 40M+ Linhas

**Setor:** Saúde suplementar (operadora de saúde)
**Empregador:** Dataside (consultoria)
**Papel:** Engenheiro de Dados Sênior — responsabilidade técnica integral

---

## Setor e Perfil do Projeto

Projeto de implantação da camada analítica de RH (People Analytics) em uma operadora de saúde de médio porte, consumida por Power BI e por stakeholders de RH e financeiro. O escopo cobria 12+ anos de histórico de folha de pagamento (2013–2026), com fonte de dados em sistema brasileiro de RH legado.

O desafio principal não era o volume — era a qualidade da fonte. Dados oriundos de sistema RH com décadas de operação acumulam quirks: encodings não-padronizados, decimais no formato BR (vírgula como separador), schemas instáveis entre competências mensais, e — o mais problemático — duplicatas legítimas por design de negócio que quebram qualquer padrão de MERGE ingênuo.

---

## Problema

**Técnico:** Como processar 40+ milhões de linhas de dados de folha de pagamento em formato .txt (ISO-8859-1, separador pipe, decimal BR), garantindo idempotência por competência mensal, sem que duplicatas legítimas na fonte quebrem a pipeline de ingestão?

**De negócio:** Como construir 8 tabelas fato de folha — cobrindo pagamentos, rescisões, encargos, benefícios, jornada, endividamento e horas extras — com regras de negócio suficientemente precisas para serem consumidas diretamente por dashboards executivos sem curadoria manual adicional?

**Escala:**
- 40M+ linhas totais (23,8M em benefícios, 10,8M em pagamentos, restante distribuído)
- 12+ anos de histórico
- 8 tabelas fato com regras de negócio distintas
- 16 bloqueios técnicos críticos diagnosticados e resolvidos em produção

---

## Stack Aplicada

| Componente | Tecnologia |
|------------|-----------|
| Orquestração | Azure Synapse Analytics (Pipelines) |
| Processamento principal | Azure Databricks (Spark Pool) |
| Formato de armazenamento | Delta Lake |
| Linguagem de transformação | PySpark + Spark SQL |
| Storage | Azure Data Lake Storage Gen2 |
| Camadas do lake | transient / raw / trusted / refined / controller |
| Configuração de cargas | Excel controller armazenado no ADLS |
| Consumo downstream | Power BI (via Synapse Serverless SQL) |

---

## Arquitetura

```
Sistema RH               ADLS Gen2                  Azure Databricks
(legado, .txt)           ─────────                  ────────────────
                         transient/                  trusted/
  folha_pag.txt ──────► [arquivo bruto]  ─────────► [Delta Bronze]
  beneficios.txt ─────► [arquivo bruto]  ─────────► [Delta Bronze]
  encargos.txt ───────► [arquivo bruto]  ─────────► [Delta Bronze]
  [...]                                              [...]
  .txt ISO-8859-1                                    schema tipado
  separador pipe                                     metadados audit
  decimal vírgula
                                                     refined/
                                                     ────────────────
                                                     [fato_folha_pag]
                                                     [fato_rescisoes]
                                                     [fato_encargos]
                                                     [fato_beneficios]
                                                     [fato_benef_dep]
                                                     [fato_jornada]
                                                     [fato_endivida]
                                                     [fato_horas_ext]
                                                     8 tabelas FATO
                                                          │
                                                          ▼
                                                    Synapse Serverless SQL
                                                    (views para Power BI)
```

**Camada Transient/Raw:** arquivos .txt recebidos no estado bruto, sem alteração. Preservação da fonte como auditoria de origem.

**Camada Trusted (Bronze):** conversão para Delta com schema inferido e tipado. Correção de encoding, decimal e separador feita nesta etapa.

**Camada Refined (Silver/Gold):** tabelas fato construídas com regras de negócio, partition por competência (`partition_date`), prontas para consumo analítico.

---

## Decisões Técnicas-Chave

### 1. Fix de decimal brasileiro — BL02

**Problema:** O sistema de RH exporta valores monetários no formato brasileiro: ponto como separador de milhar e vírgula como separador decimal (ex: `1.234,56`). Um `CAST` direto para `DECIMAL(18,2)` retorna `NULL` silenciosamente para qualquer valor acima de 999.

**Solução:**

```python
from pyspark.sql import functions as F

# Padrão aplicado em todos os campos monetários
df = df.withColumn(
    "vl_verba",
    F.col("vl_verba_raw")
     .cast("string")
     .pipe(lambda c: F.regexp_replace(c, r"\.", ""))   # remove separador de milhar
     .pipe(lambda c: F.regexp_replace(c, ",", "."))    # normaliza decimal
     .cast("decimal(18,2)")
)
```

Em SQL:
```sql
CAST(REPLACE(REPLACE(TRIM(vl_verba_raw), '.', ''), ',', '.') AS DECIMAL(18,2))
```

**Impacto:** Sem esse fix, qualquer soma ou média de valores monetários retornava `NULL` ou valores drasticamente subestimados — erro silencioso que só aparece ao comparar totais com o sistema de origem.

---

### 2. Dynamic Partition Overwrite para duplicatas legítimas — BL16

**Problema:** O arquivo `ContratadosRC.txt` contém linhas 100% idênticas por design de negócio — o mesmo funcionário pode aparecer até 21 vezes na mesma competência (ex: múltiplos vínculos ou lotações simultâneas). Um MERGE com chave composta falha silenciosamente: não consegue resolver qual das 21 linhas idênticas deve "ganhar".

**Análise do problema:**

```
Arquivo fonte — exemplo simplificado:
id_func | competencia | tipo_vinculo | vl_salario
001     | 2025-01     | EFETIVO      | 5000,00
001     | 2025-01     | EFETIVO      | 5000,00   ← idêntico ao anterior
001     | 2025-01     | EFETIVO      | 5000,00   ← idêntico (21 vezes)

MERGE com ON (id_func, competencia, tipo_vinculo, vl_salario)
→ falha: "Cannot perform a MERGE operation when multiple source rows
  match the same target row"
```

**Solução:** Dynamic Partition Overwrite — reescreve a partição inteira de competência, preservando todas as duplicatas.

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_competencia.write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("partition_date") \
    .saveAsTable("refined.fato_folha_contratados")
```

**Trade-off:** O Dynamic Partition Overwrite perde a atomicidade linha-a-linha do MERGE. Para essa tabela específica, é semanticamente correto porque o contrato de qualidade é "a partição de competência deve ser idêntica à fonte" — e não "cada linha individual deve ser rastreável por chave".

---

### 3. Timezone GMT-3 explícito — problema silencioso de auditoria

**Problema:** O `current_timestamp()` do Spark retorna UTC. Campos de auditoria (`insert_date`, `update_date`) com UTC em um sistema que opera em GMT-3 geram confusão em relatórios de auditoria que comparam timestamps com eventos do sistema de origem.

**Solução:**

```python
from pyspark.sql import functions as F

df = df.withColumn(
    "insert_date",
    F.to_utc_timestamp(F.current_timestamp(), "GMT+3")
).withColumn(
    "update_date",
    F.to_utc_timestamp(F.current_timestamp(), "GMT+3")
)
```

**Nota:** `GMT+3` na API do Spark equivale a `UTC-3` (o sinal é invertido nessa função específica). Documentar explicitamente esse comportamento contraintuitivo foi necessário para evitar regressões.

---

### 4. FSCK REPAIR após VACUUM — BL07

**Problema:** O VACUUM do Delta Lake, por padrão, retém arquivos por 7 dias (`delta.deletedFileRetentionDuration`). Em um ambiente com múltiplas reescritas por dia, os logs Delta (`_delta_log`) podem referenciar arquivos físicos que foram removidos por uma limpeza manual anterior ao período de retenção. O resultado é uma tabela cujo log diz que um arquivo existe, mas o arquivo não está no storage — queries falham com `FileNotFoundException`.

**Diagnóstico:**

```python
# Verificar integridade da tabela
spark.sql("DESCRIBE DETAIL refined.fato_folha_beneficios").show()

# Tentar SELECT simples para confirmar o erro
spark.sql("SELECT COUNT(*) FROM refined.fato_folha_beneficios").show()
# → FileNotFoundException: arquivo X não encontrado em <path>
```

**Solução:**

```sql
-- Reparar o log Delta removendo referências a arquivos inexistentes
FSCK REPAIR TABLE refined.fato_folha_beneficios
```

**Lição:** VACUUM deve sempre usar o período de retenção padrão (7 dias) ou superior, nunca ser executado com `RETAIN 0 HOURS` em produção. O FSCK REPAIR resolve o estado corrompido, mas não é substituto para práticas corretas de VACUUM.

---

### 5. Propagação de novas colunas no MERGE — BL14

**Problema:** A função `CargasAutomaticas()` usa a lista de colunas da tabela Delta de destino para construir o `UPDATE SET` do MERGE. Quando uma nova coluna é adicionada ao schema da fonte, o `autoMerge.enabled` adiciona a coluna à tabela Delta, mas o MERGE construído dinamicamente não inclui a nova coluna no `UPDATE SET` — então linhas existentes têm `NULL` na nova coluna mesmo após re-execução.

**Solução:**

```python
# Workaround: após adicionar coluna via autoMerge, fazer backfill explícito
# para registros existentes que ficaram com NULL

spark.sql("""
    ALTER TABLE refined.fato_folha_pagamento
    ADD COLUMN nova_coluna STRING
""")

spark.sql("""
    UPDATE refined.fato_folha_pagamento
    SET nova_coluna = <expressao_de_backfill>
    WHERE nova_coluna IS NULL
""")
```

**Mitigação estrutural:** Documentar o processo de adição de coluna como um procedimento de duas etapas — ALTER TABLE + backfill — e não como uma operação implícita do autoMerge.

---

## Tabelas Fato Entregues

| Tabela | Volume (aprox.) | Destaque técnico |
|--------|----------------|-----------------|
| `fato_folha_pagamento` | 10,8M linhas | Principal — verbas por competência |
| `fato_folha_pagamento_rescisoes` | — | Classificação de tipo de rescisão |
| `fato_folha_pag_encargos` | 3M linhas | Encargos patronais por competência |
| `fato_folha_pag_beneficios` | 23,8M linhas | Maior tabela — benefícios por período |
| `fato_folha_pag_beneficios_dependentes` | — | Vínculo funcionário-dependente |
| `fato_folha_jornada_trabalho` | — | Interjornada e intrajornada |
| `fato_folha_pag_endividamento` | — | Consignado e saldo devedor |
| `fato_folha_pag_horas_extras` | — | Classificação de HE por tipo |

---

## Resultados Quantificados

| Métrica | Valor |
|---------|-------|
| Total de linhas processadas | 40M+ |
| Tabelas fato entregues e homologadas | 8 |
| Anos de histórico cobertos | 12+ (2013–2026) |
| Bloqueios técnicos diagnosticados e resolvidos | 16 (BL01–BL16) |
| Linhas na maior tabela individual (benefícios) | 23,8M |
| Competências mensais processadas | ~144 meses |

---

## Padrões Aplicados

- [Dynamic Partition Overwrite](../technical-notes/dynamic-partition-overwrite.md) — solução para BL16 (duplicatas legítimas)
- [Metodologia de Homologação](../technical-notes/homologacao-metodologia.md) — validação das tabelas fato vs. sistema de origem

---

## Lições Aprendidas

**Dados de RH brasileiro têm quirks que documentação internacional não cobre.** Decimal com vírgula, encoding ISO-8859-1, separador pipe — esses não são edge cases exóticos, são o padrão de sistemas como Apdata, Datasul e RM. Todo projeto que consume dados de sistema RH nacional precisa de um checklist de normalização antes de qualquer processamento.

**Duplicatas legítimas são diferentes de erros de dados.** A reação intuitiva a duplicatas é "limpar". Neste caso, as duplicatas eram corretas — um funcionário com múltiplos vínculos aparece múltiplas vezes. Entender a semântica de negócio antes de escolher a estratégia de escrita economizou semanas de debugging.

**16 bloqueios técnicos não são falha de projeto — são engenharia de produção.** Em todo projeto com dados heterogêneos, novos problemas emergem ao encontrar dados reais. A qualidade está em diagnosticar com precisão, documentar a causa raiz e resolver de forma sustentável — não em prever todos os problemas antes de começar.

**Power BI direto do Delta Lake via Serverless SQL elimina redundância.** A integração via Synapse Serverless SQL usando views sobre as tabelas Delta evitou criar uma camada extra de materialization apenas para o BI — os dados existem uma vez, no lake, e são servidos por computação sob demanda.
