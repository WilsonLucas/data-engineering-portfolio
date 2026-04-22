# Dynamic Partition Overwrite — Padrão para Duplicatas Legítimas

**Categoria:** Padrão de Escrita Delta Lake / PySpark
**Stack:** Azure Databricks · Azure Synapse Spark Pool · Delta Lake · PySpark

---

## O Problema que Motiva

Em pipelines de dados, o comportamento padrão ao lidar com duplicatas é tratá-las como erro — afinal, se duas linhas têm a mesma chave de negócio, uma delas provavelmente está errada. O MERGE se apoia exatamente nessa premissa: para cada chave de destino, existe no máximo uma linha de origem correspondente.

O problema emerge quando a fonte produz **duplicatas legítimas por design de negócio** — não como erro, mas como a representação correta da realidade.

**Exemplos concretos:**
- Um funcionário com múltiplos vínculos empregatícios simultâneos na mesma competência (sistema de RH)
- Um produto vendido pelo mesmo fornecedor em múltiplas condições de entrega distintas, mas com a mesma chave de pedido
- Registros de auditoria que deliberadamente replicam o estado anterior de uma entidade

Quando a source contém linhas como:

```
id_func | competencia | tipo_vinculo | vl_salario
001     | 2025-01     | EFETIVO      | 5000.00
001     | 2025-01     | EFETIVO      | 5000.00   ← igual à anterior
001     | 2025-01     | EFETIVO      | 5000.00   ← igual (21 ocorrências)
```

E você tenta executar um MERGE com `ON (id_func, competencia, tipo_vinculo, vl_salario)`, o Delta Lake levanta:

```
AnalysisException: Cannot perform a MERGE operation when multiple source rows
match the same target row.
```

Se você tentar criar uma chave artificial (ex: `ROW_NUMBER()`) para deduplicar, você quebra o significado dos dados — estava correto ter 21 linhas.

---

## Quando Usar Dynamic Partition Overwrite

Use este padrão quando **todas** as condições a seguir forem verdadeiras:

1. A fonte possui duplicatas legítimas — linhas 100% idênticas ou sem chave de negócio discriminante
2. O contrato de qualidade da tabela é "a partição deve espelhar exatamente o conteúdo da fonte para aquela partição"
3. As partições são naturalmente isoladas — uma carga não afeta dados de outra partição
4. A atomicidade de partição é suficiente (não é necessário rastrear cada linha individualmente)

O caso mais comum: **tabelas particionadas por período (competência, data de referência, mês/ano)** onde cada carga mensal deve substituir integralmente a partição correspondente.

---

## Como Aplicar

### Configuração

```python
# Ativar Dynamic Partition Overwrite antes da escrita
# (pode ser configurado no nível de sessão Spark)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

### Escrita da partição

```python
df_competencia \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("partition_date") \
    .saveAsTable("schema.tabela_destino")
```

O comportamento do `dynamic`:
- Apenas as partições presentes no DataFrame de entrada são reescritas
- Partições ausentes no DataFrame (competências anteriores) são preservadas intactas
- A operação é atômica no nível de partição — ou a partição inteira é substituída, ou nenhuma mudança ocorre

### Comportamento comparado ao modo `static`

```
Modo static (padrão):
  overwrite + static → apaga TODAS as partições da tabela e reescreve

Modo dynamic:
  overwrite + dynamic → apaga APENAS as partições presentes no DataFrame
```

**Atenção:** o modo `static` com `overwrite` apaga toda a tabela — em tabelas particionadas por competência com anos de histórico, isso seria catastrófico.

---

## Implementação Típica — Pipeline por Competência

```python
from pyspark.sql import functions as F

# 1. Ler a competência a processar (parâmetro da pipeline)
competencia = dbutils.widgets.get("partition_date")  # ex: "2025-01"

# 2. Ler os dados da fonte para essa competência
df_fonte = spark.read \
    .format("csv") \
    .option("sep", "|") \
    .option("encoding", "ISO-8859-1") \
    .option("header", "true") \
    .load(f"abfss://raw@<storage>.dfs.core.windows.net/folha/{competencia}/*.txt")

# 3. Aplicar transformações e adicionar coluna de partição
df_processado = df_fonte \
    .withColumn("partition_date", F.lit(competencia)) \
    .withColumn("insert_date", F.to_utc_timestamp(F.current_timestamp(), "GMT+3"))

# 4. Configurar e escrever com Dynamic Partition Overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_processado \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("partition_date") \
    .saveAsTable("refined.fato_folha_pagamento")

print(f"Partição {competencia} reescrita com sucesso.")
```

---

## Reprocessamento de Competências Históricas

Um dos benefícios diretos do Dynamic Partition Overwrite é a simplicidade do reprocessamento histórico. Para reprocessar uma competência específica:

```python
# Reprocessar apenas 2024-03, sem afetar outras competências
competencia = "2024-03"

# Executa exatamente o mesmo código de carga normal
# A partição 2024-03 será reescrita; todas as outras ficam intactas
```

Isso simplifica pipelines de reprocessamento — não há lógica especial para "modo reprocessamento" vs. "modo carga incremental". O comportamento é o mesmo em ambos os casos.

---

## Quando NÃO Usar

**Não use Dynamic Partition Overwrite quando:**

1. **Você precisa de rastreabilidade por linha:** se o requisito de auditoria exige saber "esta linha específica foi inserida em X e modificada em Y", o MERGE com campos `insert_date`/`update_date` é a escolha correta. O Overwrite substitui toda a partição sem registrar o histórico de cada linha.

2. **A chave de negócio é estável e as duplicatas são erro:** se duplicatas na fonte são um defeito de qualidade de dados (não design intencional), corrija a qualidade na origem ao invés de usar Dynamic Partition Overwrite como contorno.

3. **As partições não são naturalmente isoladas:** se um registro pode pertencer a múltiplas partições, ou se uma carga pode afetar dados de outras partições, o modelo de "reescrever partição inteira" não é seguro.

4. **O volume por partição é muito grande e a carga é apenas de updates pontuais:** reescrever uma partição de 5GB para atualizar 100 linhas é ineficiente. Nesse caso, MERGE com chave estável é mais adequado.

---

## Comparação de Padrões de Escrita

| Estratégia | Chave Necessária | Duplicatas Legítimas | Janela de Inconsistência | Reprocessamento Simples |
|------------|-----------------|---------------------|--------------------------|------------------------|
| Overwrite (static) | Não | Sim | Sim (apaga tudo) | Não (apaga histórico) |
| MERGE (ACID) | Sim | Não | Não | Sim (por chave) |
| Dynamic Partition Overwrite | Não | Sim | Não (atômico por partição) | Sim (por partição) |

---

## Referências

- [Delta Lake — Table Batch Reads and Writes](https://docs.delta.io/latest/delta-batch.html)
- [Databricks — Dynamic Partition Overwrite](https://docs.databricks.com/en/delta/merge.html)
- [Apache Spark — Dynamic Partition Pruning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

---

*Padrão extraído do [Case Study 02 — People Analytics: Folha de Pagamento 40M+ Linhas](../case-studies/02-folha-pagamento-40M-linhas.md)*
