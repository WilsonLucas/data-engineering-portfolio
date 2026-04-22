# Case Study 03 — Homologação Byte-a-Byte Cross-Env com OPENROWSET

**Setor:** Público (apoio a micro e pequenas empresas)
**Empregador:** Dataside (consultoria)
**Papel:** Engenheiro de Dados Sênior — responsabilidade técnica integral

---

## Setor e Perfil do Projeto

Migração de lógica analítica legada — procedures T-SQL on-premises desenvolvidas ao longo de anos — para PySpark em Azure Synapse Analytics. O projeto atendia uma agência pública cujos dados de saída são consumidos diretamente por diretoria e stakeholders estaduais: dashboards e relatórios com impacto em decisões de alocação de recursos para micro e pequenas empresas.

Pela criticidade do consumo downstream, a organização exigiu paridade verificável e documentada entre a nova implementação PySpark e as procedures legadas — não uma estimativa de equivalência, mas evidência auditável de que cada linha, cada valor, cada chave primária do novo sistema corresponde ao legado.

---

## Problema

**Técnico:** Como homologar a migração de procedures T-SQL para PySpark quando o ambiente de desenvolvimento tem acesso read-only à produção e os dados de dev estão estruturalmente incompletos (subconjunto da base)?

**Metodológico:** Como estruturar uma validação que seja auditável, reproducível e que vá além da amostragem — especialmente quando a diretoria precisa assinar o go-live baseada nos resultados?

**Ferramental:** Como inspecionar schemas Delta, listar paths ADLS e verificar status de pipelines sem depender do Synapse Studio para cada operação do dia a dia?

---

## Stack Aplicada

| Componente | Tecnologia |
|------------|-----------|
| Processamento principal | Azure Synapse Analytics (Spark Pool) |
| Homologação cross-env | Synapse Serverless SQL (OPENROWSET + AAD passthrough) |
| Formato de armazenamento | Delta Lake |
| Storage | Azure Data Lake Storage Gen2 |
| Linguagem de transformação | PySpark + Spark SQL |
| Parsing de metadados | Python + pyarrow |
| Ferramenta CLI própria | Python (SynapseCheck / `sc`) |

---

## Arquitetura do Ambiente

```
Ambiente PROD (read-only)          Ambiente DEV
──────────────────────────         ──────────────────────────
SQL Server on-prem                 Synapse Spark Pool
  [procedures T-SQL legadas]         [notebooks PySpark (novo)]
          │                                    │
          ▼                                    ▼
  Base espelho ADLS (prod)          ADLS DEV
  (Delta/Parquet)                   (Delta)

                    ┌─────────────────────────────┐
                    │  Synapse Serverless SQL      │
                    │  (synapseXXX-ondemand)       │
                    │                              │
                    │  OPENROWSET com AAD          │
                    │  passthrough permite que     │
                    │  DEV leia PROD diretamente   │
                    └─────────────────────────────┘
                               ▲
                               │
                    Metodologia de homologação
                    (3 blocos de validação)
```

---

## A Solução do Acesso Cross-Environment

O bloqueio fundamental era: o ambiente de desenvolvimento não tinha dados suficientes para uma validação completa. A cópia de dados de prod para dev era inviável (volume e compliance). A solução foi usar o Synapse Serverless SQL com `OPENROWSET` e passthrough AAD para que o ambiente de dev lesse diretamente os dados de produção, sem movimentação de dados.

```sql
-- Query executada em DEV, lendo PROD via Serverless SQL
SELECT
    pk_coluna,
    coluna_a,
    coluna_b,
    coluna_c
FROM OPENROWSET(
    BULK 'https://<storage-prod>.dfs.core.windows.net/<container>/<path>/',
    FORMAT = 'DELTA'
) WITH (
    pk_coluna    VARCHAR(50),
    coluna_a     DECIMAL(18,2),
    coluna_b     VARCHAR(200),
    coluna_c     DATE
) AS prod_data
```

**Por que funciona:** O Synapse Serverless SQL (`-ondemand` endpoint) usa o token AAD do usuário autenticado para acessar o storage — sem mover dados, sem criar cópias intermediárias, com controle de acesso pelo RBAC do Azure.

---

## Metodologia de Homologação (Zero Amostragem)

A metodologia foi desenvolvida em três blocos independentes que juntos provam equivalência completa.

### Bloco 1 — Universo (Cobertura de Chaves Primárias)

Objetivo: garantir que o novo sistema processa exatamente o mesmo universo de registros que o legado — nem mais, nem menos.

```python
# Leitura da tabela nova (DEV/lake)
df_lake = spark.read.format("delta").load("<path_dev>")

# Leitura da base espelho (PROD via Serverless SQL)
df_espelho = spark.read \
    .format("com.microsoft.azure.synapse.spark") \
    .option("url", "jdbc:sqlserver://<serverless>-ondemand.sql.azuresynapse.net") \
    .option("query", "SELECT pk FROM <tabela_espelho>") \
    .load()

# FULL OUTER JOIN para encontrar divergências de universo
df_universo = df_lake.select("pk").alias("lake") \
    .join(df_espelho.select("pk").alias("espelho"), on="pk", how="full_outer")

pk_comuns         = df_universo.filter("lake.pk IS NOT NULL AND espelho.pk IS NOT NULL")
pk_apenas_lake    = df_universo.filter("espelho.pk IS NULL")
pk_apenas_espelho = df_universo.filter("lake.pk IS NULL")

print(f"PKs comuns: {pk_comuns.count()}")
print(f"PKs apenas no lake (novo): {pk_apenas_lake.count()}")
print(f"PKs apenas no espelho (legado): {pk_apenas_espelho.count()}")
```

**Resultado neste projeto:** 4,1M+ PKs comuns, 0 divergências de universo após correção dos filtros de data.

---

### Bloco 2 — Diff por Coluna (Validação de Valores)

Objetivo: para cada coluna, quantificar e classificar as divergências entre o lago e o espelho, usando apenas as PKs comuns.

```python
colunas_para_validar = [c for c in df_lake.columns if c != "pk"]

resultados = []

for coluna in colunas_para_validar:
    df_diff = pk_comuns.join(df_lake.select("pk", coluna).alias("L"), on="pk") \
                       .join(df_espelho.select("pk", coluna).alias("E"), on="pk")

    total = df_diff.count()

    # Classificação dos tipos de divergência
    null_vs_valor = df_diff.filter(
        F.col(f"L.{coluna}").isNull() & F.col(f"E.{coluna}").isNotNull()
    ).count()

    valor_vs_null = df_diff.filter(
        F.col(f"L.{coluna}").isNotNull() & F.col(f"E.{coluna}").isNull()
    ).count()

    valor_vs_valor = df_diff.filter(
        F.col(f"L.{coluna}").isNotNull() &
        F.col(f"E.{coluna}").isNotNull() &
        (F.col(f"L.{coluna}") != F.col(f"E.{coluna}"))
    ).count()

    divergencias = null_vs_valor + valor_vs_null + valor_vs_valor

    resultados.append({
        "coluna": coluna,
        "total": total,
        "divergencias": divergencias,
        "null_vs_valor": null_vs_valor,
        "valor_vs_null": valor_vs_null,
        "valor_vs_valor": valor_vs_valor,
        "status": "PASS" if divergencias == 0 else "FAIL"
    })
```

**Resultado neste projeto:** 76 colunas auditadas, 21 com PASS absoluto (zero divergências), demais com causa raiz documentada.

---

### Bloco 3 — Distribuição de Pares (Causa Raiz)

Objetivo: para colunas com `FAIL`, entender o padrão das divergências. Não apenas "quantas linhas divergem", mas "qual é o mapeamento de valor legado para valor novo".

```python
# Para cada coluna com divergência, gerar a distribuição dos pares
def analisa_pares(df_diff, coluna, top_n=20):
    return df_diff.filter(
        F.col(f"L.{coluna}") != F.col(f"E.{coluna}")
    ).groupBy(
        F.col(f"L.{coluna}").alias("valor_lake"),
        F.col(f"E.{coluna}").alias("valor_espelho")
    ).count() \
     .orderBy(F.desc("count")) \
     .limit(top_n)
```

**Exemplo de insight gerado:** a coluna `cd_municipio` divergia em 12.000 linhas porque o legado usava código IBGE sem zeros à esquerda, enquanto o novo sistema preservava o formato original da fonte. Causa raiz documentada → decisão de negócio: qual formato é o correto para o downstream.

---

## CLI Própria — SynapseCheck (`sc`)

Para reduzir a dependência do Synapse Studio em tarefas de diagnóstico do dia a dia, desenvolvi uma CLI Python read-only com os seguintes comandos:

```
sc config                    # Valida setup (credenciais, conectividade)
sc storage "<path>"          # Lista paths ADLS (similar a ls)
sc tables                    # Lista tabelas conhecidas no registry
sc inspect <tabela>          # Schema Delta (lê _delta_log sem Spark)
sc notebooks                 # Lista notebooks do workspace Synapse
sc status -d 7               # Status de pipeline runs dos últimos 7 dias
```

**Inovação técnica interna:** o comando `sc inspect` lê o schema Delta diretamente dos arquivos `_delta_log` via parsing do transaction log — sem precisar de uma sessão Spark ativa, em segundos ao invés de minutos de startup de cluster.

```python
import json
from pathlib import Path

def infer_schema_from_delta_log(delta_log_path: str) -> dict:
    """
    Lê o schema de uma tabela Delta diretamente do _delta_log,
    sem iniciar uma sessão Spark.
    """
    log_files = sorted(Path(delta_log_path).glob("*.json"))

    for log_file in reversed(log_files):
        with open(log_file) as f:
            for line in f:
                entry = json.loads(line)
                if "metaData" in entry:
                    schema_str = entry["metaData"]["schemaString"]
                    return json.loads(schema_str)

    raise ValueError("Schema não encontrado no delta_log")
```

**Leitura de Parquet sem cluster:** para inspeção de arquivos Parquet de 2GB+, implementei download parcial (últimos 2MB, onde o footer do Parquet está) + reconstrução via `pyarrow`, permitindo obter schema e estatísticas de coluna sem subir cluster.

```python
import pyarrow.parquet as pq
import io

def read_parquet_footer(storage_client, blob_path: str) -> pq.ParquetFile:
    """
    Lê footer de Parquet >= 2GB sem baixar o arquivo inteiro.
    Resolve o erro "Message type not found" causado por INT96 timestamps.
    """
    # Tamanho do arquivo
    blob = storage_client.get_blob_client(container=..., blob=blob_path)
    size = blob.get_blob_properties().size

    # Baixar apenas os últimos 2MB (footer + magic bytes)
    offset = max(0, size - 2 * 1024 * 1024)
    footer_bytes = blob.download_blob(offset=offset, length=2 * 1024 * 1024).readall()

    # Reconstruir via pyarrow
    buffer = io.BytesIO(footer_bytes)
    return pq.ParquetFile(buffer)
```

---

## Decisões Técnicas-Chave

### 1. OPENROWSET vs. cópia de dados para homologação

**Alternativa descartada:** copiar os dados de prod para dev para validação local.

**Motivo:** Volume (dezenas de GB), compliance (dados de negócio transitando entre ambientes), e tempo de setup. O OPENROWSET com passthrough AAD entregou o mesmo resultado sem mover um byte.

### 2. Zero amostragem — validação do universo completo

**Alternativa descartada:** validar uma amostra de 10% ou 1M de registros.

**Motivo:** Em projetos de migração com consumo por diretoria, amostragem cria uma lacuna de confiança que nunca fecha. "Validamos 10% e era equivalente" não é o mesmo que "validamos 100% e é equivalente". Para o stakeholder que vai assinar o go-live, a diferença é significativa.

### 3. CLI read-only vs. permissões elevadas

**Decisão:** A CLI `sc` foi desenvolvida como estritamente read-only — nenhum comando modifica dados ou configurações.

**Motivo:** Em ambiente de prod com acesso AAD passthrough, ter uma ferramenta que acidentalmente modifica dados seria um risco operacional. Read-only elimina essa classe de acidente.

---

## Resultados Quantificados

| Métrica | Valor |
|---------|-------|
| PKs comuns validados | 4,1M+ |
| Colunas auditadas | 76 |
| Colunas com PASS absoluto (zero divergências) | 21 |
| Colunas com divergência e causa raiz documentada | 55 |
| Ambientes cruzados na validação | 2 (dev lendo prod via OPENROWSET) |
| Movimentação de dados para homologação | 0 bytes |

---

## Padrões Aplicados

- [Metodologia de Homologação](../technical-notes/homologacao-metodologia.md) — detalhamento completo dos 3 blocos de validação

---

## Lições Aprendidas

**OPENROWSET como proxy de acesso é subutilizado.** A maioria dos engenheiros usa Serverless SQL apenas para queries ad hoc sobre arquivos ADLS. Usá-lo como ponte controlada entre ambientes — dev lendo prod sem cópia — é um padrão elegante que vale documentar e replicar.

**"Causa raiz documentada" é diferente de "erro corrigido".** Algumas das 55 colunas com divergência tinham divergências corretas — o novo sistema calculava diferente, e a diferença era intencional (melhoria de regra de negócio). Documentar a causa raiz permitiu classificar cada divergência em "corrigir", "aceitar como melhoria" ou "investigar mais". Sem essa classificação, toda divergência parece falha.

**INT96 timestamps são uma armadilha clássica do Parquet.** O erro `"Message type not found"` ao ler Parquet via pyarrow é quase sempre causado por colunas de timestamp gravadas no formato INT96 (legado do Impala). A solução é usar `read_options=pyarrow.parquet.ParquetDatasetReaderOptions(coerce_int96_timestamp_unit="ms")`. Documentado nos comentários da CLI.

**CLI própria multiplica produtividade em ambientes complexos.** O retorno sobre o investimento de 2-3 dias para construir a `sc` foi enorme — cada diagnóstico que antes levava 15 minutos de navegação no Synapse Studio passou a levar 30 segundos no terminal.
