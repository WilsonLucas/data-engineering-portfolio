# Wilson Lucas — Engenheiro de Dados Sênior | Portfolio

> Arquiteturas de dados robustas, pipelines homologados em produção, automação que escala.

---

## Sobre

Engenheiro de Dados Sênior com 10+ anos de experiência construindo e sustentando plataformas de dados em ambientes on-premises e em nuvem. Atuo em consultoria itinerante, com foco em projetos de alta complexidade técnica nos setores financeiro, de saúde, público e regulado.

Minha especialidade é a interseção entre arquitetura, engenharia e governança: desenhar pipelines na arquitetura Medallion (Bronze, Silver, Gold) sobre Azure Databricks, Delta Lake e Unity Catalog; conduzir migrações críticas de Oracle e SQL Server para plataformas analíticas modernas; e garantir a qualidade dos dados por meio de homologações byte-a-byte contra bases legadas.

Entregas recentes incluem processamento e homologação de mais de 40 milhões de linhas em tabelas fato de folha de pagamento, catálogo de 433+ tabelas no Unity Catalog em ambiente do setor segurador, auditoria de 76 colunas contra base espelho on-premises com metodologia de zero amostragem, e desenvolvimento de CLI própria em Python para inspeção de ambientes Synapse e ADLS.

---

## Nota sobre Confidencialidade

> Os clientes não são citados nominalmente neste portfolio devido a NDAs e acordos de confidencialidade vigentes. Os case studies documentam arquitetura, decisões técnicas e padrões aplicados, sem expor código proprietário, dados sensíveis ou informações identificáveis de clientes. Os empregadores (Compass.UOL, Dataside) são mencionados como contexto quando relevante.

---

## Stack Principal

**Cloud e Plataformas de Dados**
- Azure Databricks · Azure Synapse Analytics · Azure Data Factory
- ADLS Gen2 · Azure Key Vault · Unity Catalog · Delta Lake
- SAP Datasphere · SAP HANA · TDV (TIBCO Data Virtualization)
- Oracle Cloud Infrastructure (OCI)

**Linguagens e Processamento**
- PySpark · Spark SQL · Python · T-SQL · PL/SQL · Bash

**Arquiteturas e Padrões**
- Medallion Architecture (Raw/Landing → Bronze → Silver → Gold)
- Controller-driven Architecture · MERGE idempotente ACID
- Schema Evolution · Dynamic Partition Overwrite
- Modelagem Dimensional (star e snowflake) · SCD Tipo 1 e 2

**ETL/ELT e Integração**
- SSIS · Informatica PowerCenter · Pentaho Data Integration (PDI)
- SAS Data Integration 9.4 · SAS Viya · Knime · SAP Information Steward

**Bancos de Dados**
- Oracle · SQL Server · PostgreSQL · MySQL · Delta Lake (Parquet)

**DevOps, CLI e Automação**
- Databricks CLI v0.297+ · Azure CLI · Git · Desenvolvimento de CLIs em Python

**Metodologia e Governança**
- Homologação byte-a-byte (zero amostragem) · Auditoria linha-a-linha
- Data Quality · Data Governance · Scrum

---

## Case Studies

| # | Projeto | Setor | Stack Central | Complexidade |
|---|---------|-------|---------------|-------------|
| 01 | [Controller-driven Medallion Architecture](./case-studies/01-controller-driven-medallion.md) | Segurador | ADF · Databricks · Unity Catalog · Delta Lake · PySpark | Alta |
| 02 | [People Analytics — Folha de Pagamento 40M+ linhas](./case-studies/02-folha-pagamento-40M-linhas.md) | Saúde (RH) | Synapse · Databricks · Delta Lake · PySpark | Muito Alta |
| 03 | [Homologação Byte-a-Byte Cross-Env com OPENROWSET](./case-studies/03-homologacao-byte-a-byte.md) | Público | Synapse Serverless SQL · Delta Lake · Python · pyarrow | Alta |
| 04 | [Migração de Camada Semântica Corporativa](./case-studies/04-migracao-camada-semantica.md) | Corporativo | TDV · SAP Datasphere · SAP HANA · Oracle · Synapse | Alta |

---

## Padrões e Metodologias

Notas técnicas aprofundadas sobre padrões reutilizáveis extraídos dos projetos reais.

| Nota Técnica | Descrição |
|--------------|-----------|
| [Controller-driven Architecture](./technical-notes/controller-driven-architecture.md) | Como parametrizar 100% de uma pipeline via tabelas Delta de controle |
| [Dynamic Partition Overwrite](./technical-notes/dynamic-partition-overwrite.md) | Padrão para lidar com duplicatas legítimas que quebram MERGE tradicional |
| [Metodologia de Homologação](./technical-notes/homologacao-metodologia.md) | Zero amostragem, universo idêntico, diff por coluna, causa raiz documentada |

---

## Trajetória Profissional (resumida)

| Período | Empresa | Papel |
|---------|---------|-------|
| Dez/2024 – Atual | Dataside (Consultoria) | Engenheiro de Dados Sênior — multi-cliente |
| Jul/2023 – Atual | Compass.UOL | Engenheiro de Dados Sênior |
| Jun/2021 – Jun/2023 | VERT | Engenheiro de Dados Sênior |
| Fev/2019 – Mai/2021 | Global Web | DBA Pleno / Consultor DBA |
| Mai/2017 – Jan/2019 | Stefanini | Analista de Dados |

---

## Formação

- Especialização em Banco de Dados e Business Intelligence — SENAC (2020)
- Bacharelado em Sistemas de Informação — UNIP (2016)

---

## Contato

- LinkedIn: [linkedin.com/in/wilson-lucas-719963b4](https://linkedin.com/in/wilson-lucas-719963b4)
- GitHub: [github.com/WilsonLucas](https://github.com/WilsonLucas)
- E-mail: wilsonlucas201@gmail.com

---

## Bio Expandida

Para narrativa detalhada sobre trajetória, filosofia de trabalho e áreas de domínio, ver [SOBRE.md](./SOBRE.md).

---

## Direitos e Licença

O conteúdo conceitual deste portfolio — arquiteturas, metodologias, padrões técnicos e decisões de design — é de autoria própria e pode ser referenciado livremente com atribuição. Este material não representa, reproduz nem expõe código proprietário de nenhum cliente.
