# Metodologia de Homologação — Zero Amostragem, Universo Idêntico, Causa Raiz

**Categoria:** Metodologia de Qualidade de Dados
**Stack:** PySpark · Spark SQL · Azure Synapse Serverless SQL · Delta Lake

---

## O Problema que Motiva

Toda migração de pipeline — de procedures T-SQL legadas para PySpark, de Oracle para Databricks, de qualquer sistema A para qualquer sistema B — enfrenta a mesma pergunta fundamental: **o novo sistema produz os mesmos dados que o legado?**

A resposta usual é validar por amostragem: pega-se N% dos registros, compara-se à mão, declara-se equivalência. O problema com amostragem é que ela só prova que as N% linhas validadas estão corretas. As (100-N)% restantes permanecem sem garantia.

Em projetos onde os dados de saída são consumidos por diretoria, alimentam decisões de negócio relevantes ou têm requisitos regulatórios, amostragem não é suficiente. A pergunta "você testou todos os registros?" precisa ter resposta afirmativa.

Esta metodologia responde a essa pergunta com estrutura, reproducibilidade e documentação de causa raiz para cada divergência encontrada.

---

## Premissas da Metodologia

1. **Zero amostragem:** toda chave primária do universo é validada, não uma amostra
2. **Universo idêntico:** o conjunto de PKs comparado deve ser o mesmo nos dois sistemas antes de comparar valores
3. **Diff por coluna:** a divergência é classificada por coluna e por tipo — não apenas "linhas diferentes"
4. **Causa raiz documentada:** cada divergência tem uma explicação, não apenas uma contagem

---

## Arquitetura da Validação (3 Blocos)

```
BLOCO 1 — Universo
┌───────────────────────────────────────────────────────────┐
│ FULL OUTER JOIN entre PKs do novo sistema e PKs do legado │
│                                                           │
│ Resultado:                                                │
│   pk_comuns          → ambos os sistemas têm esta PK     │
│   pk_apenas_novo     → novo sistema tem, legado não tem   │
│   pk_apenas_legado   → legado tem, novo sistema não tem   │
└───────────────────────────────────────────────────────────┘
            │
            ▼ (usar apenas pk_comuns para os próximos blocos)

BLOCO 2 — Diff por Coluna
┌───────────────────────────────────────────────────────────┐
│ Para cada coluna, contar divergências classificadas:      │
│                                                           │
│   null_vs_valor   → novo=NULL, legado=valor               │
│   valor_vs_null   → novo=valor, legado=NULL               │
│   valor_vs_valor  → ambos têm valor, mas diferem         │
│                                                           │
│ Resultado por coluna:                                     │
│   status = PASS (0 divergências) ou FAIL (n divergências) │
└───────────────────────────────────────────────────────────┘
            │
            ▼ (apenas para colunas com FAIL)

BLOCO 3 — Causa Raiz
┌───────────────────────────────────────────────────────────┐
│ Para cada coluna com FAIL, distribuição dos pares de      │
│ valores divergentes:                                      │
│   valor_novo | valor_legado | contagem                    │
│   "SP"       | "SÃO PAULO"  | 1.200                      │
│   NULL       | "SP"         | 340                        │
│                                                           │
│ Resultado: causa raiz identificada e categorizada         │
└───────────────────────────────────────────────────────────┘
```

---

## Implementação — Bloco 1: Universo

```python
from pyspark.sql import functions as F

# Leitura das duas fontes
df_novo = spark.read.format("delta").load("<path_novo_sistema>")
df_legado = spark.read.format("delta").load("<path_legado>")
# ou: df_legado = spark.read.jdbc(...) para bases relacionais

# FULL OUTER JOIN nas PKs
df_universo = df_novo.select("pk").alias("novo") \
    .join(
        df_legado.select("pk").alias("legado"),
        on="pk",
        how="full_outer"
    )

# Classificação
pk_comuns = df_universo.filter(
    F.col("novo.pk").isNotNull() & F.col("legado.pk").isNotNull()
)
pk_apenas_novo = df_universo.filter(F.col("legado.pk").isNull())
pk_apenas_legado = df_universo.filter(F.col("novo.pk").isNull())

# Report
print(f"PKs comuns:          {pk_comuns.count():>10,}")
print(f"PKs apenas no novo:  {pk_apenas_novo.count():>10,}")
print(f"PKs apenas no legado:{pk_apenas_legado.count():>10,}")
```

**Interpretação:**
- `pk_apenas_novo > 0`: o novo sistema processa registros que o legado não conhecia — pode ser bug ou dado novo legítimo
- `pk_apenas_legado > 0`: o novo sistema está perdendo registros — quase sempre é bug a investigar
- Ambos zerados: universo idêntico, pode avançar para o Bloco 2

---

## Implementação — Bloco 2: Diff por Coluna

```python
# Definir colunas a validar (excluir PKs e metadados de auditoria)
colunas = [c for c in df_novo.columns
           if c not in ("pk", "insert_date", "update_date", "partition_date")]

# Join interno nas PKs comuns
df_join = pk_comuns_df \
    .join(df_novo.alias("N"), on="pk") \
    .join(df_legado.alias("L"), on="pk")

resultados = []

for col in colunas:
    # Total de linhas no universo comum
    total = df_join.count()

    # Contar cada tipo de divergência
    null_vs_valor = df_join.filter(
        F.col(f"N.{col}").isNull() & F.col(f"L.{col}").isNotNull()
    ).count()

    valor_vs_null = df_join.filter(
        F.col(f"N.{col}").isNotNull() & F.col(f"L.{col}").isNull()
    ).count()

    valor_vs_valor = df_join.filter(
        F.col(f"N.{col}").isNotNull() &
        F.col(f"L.{col}").isNotNull() &
        (F.col(f"N.{col}").cast("string") != F.col(f"L.{col}").cast("string"))
    ).count()

    total_div = null_vs_valor + valor_vs_null + valor_vs_valor

    resultados.append({
        "coluna":        col,
        "total_linhas":  total,
        "divergencias":  total_div,
        "pct_divergencia": round(total_div / total * 100, 2) if total > 0 else 0,
        "null_vs_valor": null_vs_valor,
        "valor_vs_null": valor_vs_null,
        "valor_vs_valor": valor_vs_valor,
        "status":        "PASS" if total_div == 0 else "FAIL"
    })

# Exibir resultado
df_resultado = spark.createDataFrame(resultados)
df_resultado.orderBy("status", F.desc("divergencias")).show(50, truncate=False)
```

---

## Implementação — Bloco 3: Causa Raiz

```python
def analisa_causa_raiz(df_join, coluna: str, top_n: int = 20):
    """
    Para uma coluna com FAIL, retorna a distribuição dos pares divergentes.
    Permite identificar o padrão da divergência (ex: formato, encoding, regra).
    """
    return df_join.filter(
        F.col(f"N.{coluna}").cast("string") != F.col(f"L.{coluna}").cast("string")
    ).groupBy(
        F.col(f"N.{coluna}").alias("valor_novo"),
        F.col(f"L.{coluna}").alias("valor_legado")
    ).agg(
        F.count("*").alias("ocorrencias")
    ).orderBy(
        F.desc("ocorrencias")
    ).limit(top_n)

# Executar para cada coluna com FAIL
colunas_fail = [r["coluna"] for r in resultados if r["status"] == "FAIL"]

for col in colunas_fail:
    print(f"\n=== Causa raiz: {col} ===")
    analisa_causa_raiz(df_join, col).show(truncate=False)
```

**Exemplo de output e interpretação:**

```
=== Causa raiz: cd_municipio ===
+------------+-------------+-----------+
|valor_novo  |valor_legado |ocorrencias|
+------------+-------------+-----------+
|3550308     |3550308      |1200       | ← mesmo valor, mas tipo diferente?
|NULL        |3550308      |340        | ← novo sistema não está populando
|3550308     |35503080     |89         | ← legado tem zero extra no final
+------------+-------------+-----------+

Causa raiz identificada:
- Linha 1: possível divergência de tipo (INT vs. STRING), CAST resolve
- Linha 2: coluna não mapeada no novo sistema — bug de transformação
- Linha 3: legado tem erro de dado (código incorreto) — o novo sistema está correto
```

---

## Validação Cross-Environment com OPENROWSET

Em projetos onde o ambiente de dev tem acesso read-only ao storage de prod, o OPENROWSET do Synapse Serverless SQL permite validar sem mover dados:

```sql
-- Query executada no endpoint -ondemand do Synapse
-- Permite que DEV leia PROD diretamente via token AAD

SELECT pk, coluna_a, coluna_b
FROM OPENROWSET(
    BULK 'https://<storage-prod>.dfs.core.windows.net/<container>/<path>/',
    FORMAT = 'DELTA'
) WITH (
    pk       VARCHAR(50),
    coluna_a DECIMAL(18,2),
    coluna_b VARCHAR(200)
) AS prod_data
```

Em PySpark, usar o connector JDBC para ler via Serverless SQL:

```python
df_legado = spark.read \
    .format("com.microsoft.azure.synapse.spark") \
    .option("url", "jdbc:sqlserver://<workspace>-ondemand.sql.azuresynapse.net;...") \
    .option("query", "SELECT pk, coluna_a FROM <view_sobre_prod>") \
    .load()
```

---

## Template de Relatório de Homologação

Ao final da validação, o relatório deve conter:

```
RELATÓRIO DE HOMOLOGAÇÃO — <Nome da Tabela>
Data: YYYY-MM-DD
Validado por: [nome]
Ambiente novo: <endpoint/path>
Ambiente legado: <endpoint/path>

BLOCO 1 — UNIVERSO
  PKs comuns:           X,XXX,XXX
  PKs apenas no novo:   0
  PKs apenas no legado: 0
  Status: PASS

BLOCO 2 — DIFF POR COLUNA (resumo)
  Total de colunas validadas: XX
  Colunas com PASS absoluto: XX
  Colunas com FAIL:           XX

  Detalhes por coluna:
  coluna_a   PASS    (0 divergências)
  coluna_b   PASS    (0 divergências)
  coluna_c   FAIL    (1.200 divergências — 0,3% do universo)
  ...

BLOCO 3 — CAUSA RAIZ (para colunas com FAIL)
  coluna_c:
    Causa raiz: código de município sem padding de zeros à esquerda no legado.
    Decisão: novo sistema está correto (código IBGE com 7 dígitos).
    Ação: nenhuma — divergência aceita.

RESULTADO FINAL: APROVADO PARA GO-LIVE
Pendências documentadas: 1 divergência aceita em coluna_c (ver acima)
```

---

## Quando NÃO Usar Esta Metodologia

Esta metodologia pressupõe que **existe uma base espelho ou legado como referência**. Não se aplica quando:

1. Não existe sistema legado para comparar — o dado é gerado pela primeira vez
2. O legado é o sistema de origem (fonte transacional) e não uma transformação equivalente
3. A validação de negócio é mais relevante que a paridade técnica — nesses casos, regras de negócio customizadas são mais adequadas que diff linha-a-linha

---

## Referências

- [Delta Lake — Reading Delta Tables](https://docs.delta.io/latest/delta-batch.html)
- [Azure Synapse — OPENROWSET with Delta Lake](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/query-delta-lake-format)
- [PySpark — Full Outer Join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)

---

*Metodologia aplicada no [Case Study 03 — Homologação Byte-a-Byte Cross-Env](../case-studies/03-homologacao-byte-a-byte.md) e no [Case Study 01 — Controller-driven Medallion Architecture](../case-studies/01-controller-driven-medallion.md)*
