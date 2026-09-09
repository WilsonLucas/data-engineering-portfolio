# Case Study 06 — Migração de Lakehouse Azure Synapse para Google Cloud

**Setor:** Setor público — agência de apoio a micro e pequenas empresas
**Empregador:** Consultoria de dados
**Papel:** Engenheiro de Dados — coautoria da plataforma, ownership das camadas Prata/Ouro, homologação e documentação
**Período:** Março a setembro de 2026 — em produção

---

## Setor e Perfil do Projeto

A organização mantinha um Data Lakehouse em Azure Synapse Analytics (Delta Lake) construído ao longo de anos por múltiplos times, alimentando dashboards Power BI consumidos por diretoria e unidades regionais. A decisão estratégica foi migrar a plataforma inteira para Google Cloud, aproveitando a mudança para corrigir anti-padrões estruturais que a plataforma legada havia acumulado.

Este case é a continuação natural do [Case 03](03-homologacao-byte-a-byte.md): o mesmo ambiente, primeiro homologado e refinado no Synapse, depois migrado para GCP com um contrato de camadas novo.

---

## Problema

**Técnico:** Como migrar uma plataforma de 12,4 TB, cerca de 1.260 tabelas e 504 notebooks para outra nuvem, trocando Delta Lake por Apache Iceberg e BigQuery, sem big-bang, sem quebrar o BI e sem carregar junto os defeitos de modelagem que a plataforma antiga escondia?

**Operacional:** O diagnóstico inicial assumia cerca de 62 notebooks. O levantamento real encontrou 504. A migração precisava ser governada por evidência e não por estimativa, com cada dimensão homologada contra o ambiente de origem antes de ser considerada migrada.

**Escala do desafio:**
- 12,4 TB, cerca de 1.260 tabelas, 1,65 milhão de arquivos e aproximadamente 380 GB de lixo no ambiente de origem
- 16 fontes registradas (8 ativas), a maior com 142 tabelas
- Dimensões de 80 a 90 milhões de linhas com ramos de negócio vindos de três sistemas distintos
- Regra de migração: não mudar o dataset que o BI lê no mesmo PR que altera a camada Prata

---

## Stack Aplicada

| Componente | Tecnologia |
|------------|-----------|
| Cloud | Google Cloud (projetos dev e prd, região southamerica-east1) |
| Orquestração | Cloud Composer (Airflow) com DAGs config-driven geradas por factory e Datasets data-aware |
| Processamento | Dataproc Serverless (batches PySpark) |
| Formato Bronze e Prata | Apache Iceberg (catálogo Hadoop) sobre Cloud Storage |
| Camada Ouro | Tabelas nativas BigQuery, particionadas e clusterizadas, MERGE dentro do BigQuery |
| Observabilidade | Dataset `quality` (runs, checks, watermarks, DQ checks) e monitor de frescor diário |
| Origem | SQL Server on-premise via JDBC, com CDC nativo do SQL Server |
| Segredos | Secret Manager |
| CI/CD | Azure DevOps Pipelines (Validação, DeployDev, DeployProd) com flag de segurança para produção |
| Testes | pytest sobre motor de MERGE, factory de DAGs e motor Gold |
| Origem legada | Azure Synapse Analytics (Delta Lake, Serverless SQL via OPENROWSET para medição ao vivo) |

---

## Arquitetura

```
SQL Server on-premise (CDC)
        │  JDBC, Dataproc Serverless, 01:00 BRT
        ▼
Landing (Parquet, grão dt_carga)
        │
        ▼
Bronze (Iceberg)      1 tabela = 1 tabela de origem, MERGE técnico por CDC
        │             542 tabelas publicadas como EXTERNAL no BigQuery
        ▼
Prata (Iceberg)       1 registro daquela ORIGEM, limpo
        │             proibido UNION / FULL OUTER / anti-join entre sistemas
        │             proibido gerar chave canônica
        ▼
Ouro (BigQuery)       1 entidade ou 1 fato de negócio
        │             chave surrogate, precedência e MATCH nascem aqui
        ▼
Power BI / BQ Studio  monitor de frescor às 06:00 BRT
```

O contrato de camadas foi formalizado em documento de arquitetura com alternativas explicitamente recusadas: manter o as-is, unificar o MATCH na Prata, tabela Prata empilhada com coluna de origem, tabela de aresta de identidade e o par Trusted/Curated. Cada recusa tem o motivo registrado.

---

## Decisões Técnicas-Chave

### 1. Diagnóstico as-is antes de qualquer linha de migração

Cinco workstreams de coleta read-only sobre o ambiente de origem produziram 38 fatos evidenciados. O achado de maior valor foi a causa arquitetural da dessincronia de chaves: 48 tabelas satélite recalculavam o hash da chave em vez de consultar a dimensão-mãe. A taxa de match de identidade por entidade era de 78% para pessoa física, 55% para pessoa jurídica, 5% para produto e 0% para atendimento. Esses números definiram a regra "MATCH só na Ouro" do contrato de camadas.

Validação de CPF e CNPJ foi feita em memória com verificador módulo 11. Nenhum dado pessoal foi gravado em disco durante o diagnóstico.

### 2. Prata "um registro daquela origem" em vez de dimensão unificada

A plataforma antiga misturava três sistemas na mesma dimensão via UNION e FULL OUTER JOIN, e o resultado era um id polimórfico que contaminava 13 das 22 dimensões do datamart principal. A decisão foi proibir cruzamento entre sistemas na Prata: cada tabela Prata lê exatamente uma origem. A unificação, a precedência e a chave surrogate passam a ser responsabilidade da Ouro, onde há contexto de negócio para decidir.

Relações N:N viraram uma tabela bridge por par de entidades, eliminando o id polimórfico.

### 3. Ouro nativa no BigQuery

O piloto do primeiro datamart materializou a Ouro em Iceberg. Na prática, o MERGE com watermarks em catálogo Hadoop sobre Cloud Storage expunha risco real de commit concorrente e adicionava uma camada de publicação a cada ciclo. A decisão revisada foi executar o MERGE dentro do BigQuery, com DQ gate antes do MERGE e rollback, execução no-op quando não há dado novo e monitor de frescor. A função de hash nativa do Spark não existe no BigQuery, então a chave surrogate passou a usar `FARM_FINGERPRINT`.

### 4. Datamart inteiro gerado por especificação

O segundo datamart migrado (47 tabelas Prata e 46 objetos Ouro) foi gerado a partir de um arquivo de especificação por uma ferramenta própria, com verificação na esteira de CI: editar um artefato gerado à mão quebra o build. Duas auditorias independentes aprovaram o resultado, uma contra o contrato de camadas e outra contra os dados do Bronze.

### 5. Ferramenta própria de monitoria da migração

Uma ferramenta de linha de comando coleta status via Airflow REST API, faz COUNT no BigQuery e mede o Synapse ao vivo via OPENROWSET, comparando cada dimensão contra o alvo com tolerância de 3 pontos percentuais. Um catálogo versionado de vereditos humanos serve como trilha de auditoria, e um classificador determinístico separa falhas reais de "zumbis" (task Airflow marcada como falha com batch Dataproc concluído). A varredura completa de 93 dimensões contra o Synapse passou a levar cerca de 4 minutos.

---

## Defeitos Herdados Encontrados na Migração

A homologação dimensão a dimensão revelou defeitos que a plataforma antiga produzia silenciosamente:

| Defeito | Efeito | Correção |
|---------|--------|----------|
| Predicado `LEN(codigo) = 10` herdado do Synapse | Tautologia na origem (`char(10)` com padding), zerava a dimensão no GCP após trim | Removido; explicava déficits de 28% a 77% em três dimensões de contratos |
| Dimensão projetava a PK da tabela associativa como id da unidade | 126 mil "unidades" fantasma para cerca de 64 reais | Grão restaurado |
| Produto cartesiano em dimensão de resultado | Cerca de 100 mil linhas fabricadas | Substituída por duas bridges com 4.470 e 132 vínculos reais |
| Camada Prata lendo tabelas mortas do Bronze | 37 referências a versões nunca atualizadas | Referências corrigidas e validador de referências na esteira |
| Bloco coletivo sem JOIN com inscrição | Grão errado em dimensão de atendimento | 987 mil linhas recuperadas |
| Ano cravado no SQL de uma dimensão de meta | Zera todo mês de janeiro se ninguém editar | Registrado como candidato a parâmetro |
| Filtro de ingestão derivado do WHERE da Prata | 25 vezes menos seletivo (407 milhões de linhas em vez de 16,6 milhões), carga não terminava | Regra: filtro de ingestão vem do pipeline da origem |

---

## Resultados Quantificados

| Métrica | Valor |
|---------|-------|
| Tabelas Bronze em Iceberg publicadas no BigQuery | 542 |
| Tabelas com CDC habilitado | 221 (133 + 88) sobre 233 capture instances |
| SQLs de camada Prata | Cerca de 120 |
| Objetos de camada Ouro | 52 (2 datamarts) |
| Dimensões equivalentes ao Synapse na baseline (tolerância 0,5%) | 24, com datamart piloto fechando inteiro |
| Auditoria de integridade do segundo datamart | 89/89 tabelas com PK única e sem nulos, 0 órfãos em 60 relações FK |
| Carga paralela de 30 tabelas (68,8 milhões de linhas) | 88 minutos com 6 conexões, 30/30 sem falha, 1,6 vezes mais rápida que a sequencial |
| Dimensão de atendimento após portar ramos faltantes | 44,9 milhões para 90,6 milhões de linhas (1,3% acima do Synapse) |
| Monitoria de migração | 120/120 execuções OK, 76/76 dimensões dentro da tolerância na janela de 5 dias |
| Testes automatizados | pytest sobre merge, factory e motor Gold |

**Status em setembro de 2026:** plataforma em produção em projeto GCP dedicado, com bucket, Composer e credenciais próprios por ambiente e promoção direta de desenvolvimento para produção pela esteira de CI/CD. O ciclo diário roda às 01:00 BRT com monitor de frescor às 06:00 BRT.

---

## Governança e Segurança

- Auditoria da esteira encontrou pipeline verde mas sem branch policies, sem build validation em PR e deploy de produção apontando para o projeto de desenvolvimento. Políticas de branch foram scriptadas e uma flag de segurança condicionou o deploy de produção à existência de rede, credencial e bucket de DAGs próprios, habilitados na virada para produção.
- Plano de Service Accounts com quatro identidades (Dataproc, Composer, CI/CD, engenharia) e meta de zero chaves JSON. Estado anterior: uma conta default com papel Editor e chaves eternas.
- Um segredo de identidade corporativa encontrado em 9 notebooks enviados a repositório pessoal foi reportado imediatamente durante o diagnóstico.
- Primeira mudança do projeto entregue 100% pelo fluxo git, PR, pipeline e buckets, sem toque manual no storage.

---

## Entregáveis Documentais

- Especificação Funcional e Técnica do datamart piloto (v2.1)
- Documentação Interna Técnica do datamart piloto
- Guia de Usabilidade da plataforma ("do SQL ao dado publicado"), com 21 figuras
- Proposta de arquitetura e arquitetura to-be em HTML navegável
- Cronograma de migração integrado ao board do Azure DevOps por ferramenta própria

---

## Padrões Aplicados

- [Metodologia de Homologação](../technical-notes/homologacao-metodologia.md) — aplicada dimensão a dimensão contra o Synapse ao vivo
- Contrato de camadas com alternativas recusadas e checklist de PR

---

## Lições Aprendidas

**A plataforma de origem não é a fonte da verdade; o negócio é.** Reproduzir o Synapse byte a byte teria replicado 126 mil unidades fantasma e 100 mil linhas cartesianas. Homologar contra o alvo é necessário, mas cada divergência precisa ser classificada como defeito do alvo ou defeito da migração antes de ser "corrigida".

**Homologar contra alvo móvel não converge.** Enquanto o time do ambiente antigo continua alterando dimensões, a comparação nunca fecha. Um acordo de data de corte é pré-requisito de migração, não detalhe.

**Ausência no controller não significa objeto sem uso.** Storage e workspace são compartilhados entre projetos. Nenhum objeto é removido sem confirmação cruzada, e a remoção passa por quarentena com conferência de contagem antes de apagar a origem.
