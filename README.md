<div align="center">

# Wilson Lucas — Engenheiro de Dados Sênior

### Arquiteturas de dados robustas · Pipelines homologados em produção · Automação que escala

[![Portfolio](https://img.shields.io/badge/🌐_Portfolio_ao_vivo-wilsonlucas.github.io-0B1F3A?style=for-the-badge&labelColor=D4A017)](https://wilsonlucas.github.io/data-engineering-portfolio/)
[![Slides](https://img.shields.io/badge/🎬_Apresentação-25_slides-00B4FF?style=for-the-badge&labelColor=0B1F3A)](https://wilsonlucas.github.io/data-engineering-portfolio/PRESENTATION/portfolio-slides.html)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-wilson--lucas-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/wilson-lucas-719963b4)

[![Azure](https://img.shields.io/badge/Azure-Databricks-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Unity_Catalog-00ADD4?style=flat-square)](https://delta.io)
[![PySpark](https://img.shields.io/badge/PySpark-Spark_SQL-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![SAP](https://img.shields.io/badge/SAP-Datasphere-0FAAFF?style=flat-square&logo=sap&logoColor=white)](https://www.sap.com)
[![10+ anos](https://img.shields.io/badge/Experiência-10%2B_anos-5AD977?style=flat-square)](#trajetória-profissional)

</div>

---

## 👀 Primeira visita? Comece por aqui

> ### 🎯 [**Abrir Portfolio Completo →**](https://wilsonlucas.github.io/data-engineering-portfolio/)
>
> Landing page visual com stack, case studies, padrões técnicos e contato.
> Recomendado para uma primeira leitura narrativa.

Alternativas rápidas:
- 🎬 **[Apresentação em slides (25 telas)](https://wilsonlucas.github.io/data-engineering-portfolio/PRESENTATION/portfolio-slides.html)** — para uma leitura de ~10 min
- 📄 **Este README** — para navegar pelos arquivos do repositório diretamente

---

## 🧭 O que você encontra aqui

| Seção | Conteúdo |
|---|---|
| [**Landing page**](https://wilsonlucas.github.io/data-engineering-portfolio/) | Portfolio visual completo |
| [**Case Studies**](https://wilsonlucas.github.io/data-engineering-portfolio/#cases) | 9 projetos documentados (8 anonimizados por NDA + 1 público de processo seletivo) |
| [**Padrões Técnicos**](https://wilsonlucas.github.io/data-engineering-portfolio/#padroes) | 3 padrões reutilizáveis extraídos dos projetos |
| [**Slides**](https://wilsonlucas.github.io/data-engineering-portfolio/PRESENTATION/portfolio-slides.html) | Apresentação em 25 slides single-file HTML |
| [**Bio expandida**](https://wilsonlucas.github.io/data-engineering-portfolio/SOBRE.html) | Narrativa de trajetória e filosofia de trabalho |

---

## 🔒 Confidencialidade

> Os clientes não são citados nominalmente devido a acordos de confidencialidade vigentes. Os case studies documentam **arquitetura, decisões técnicas e padrões aplicados** — sem expor código proprietário, dados sensíveis ou informações identificáveis. Descritores de setor (seguradora nacional, operadora de saúde, setor público, energia, bens de consumo, corporativo) substituem nomes reais em todo o material.

---

## 📂 Case Studies

Os links abaixo abrem a versão renderizada via GitHub Pages (HTML estilizado com CSS). Para o código-fonte em Markdown, navegue até `case-studies/` ou `technical-notes/` neste repositório.

| # | Projeto | Setor | Stack Central |
|---|---------|-------|---------------|
| 01 | [Controller-driven Medallion Architecture](https://wilsonlucas.github.io/data-engineering-portfolio/case-studies/01-controller-driven-medallion.html) | Seguradora nacional | Azure Data Factory · Databricks · Unity Catalog · Delta Lake |
| 02 | [Folha de Pagamento — 40M+ Linhas](https://wilsonlucas.github.io/data-engineering-portfolio/case-studies/02-folha-pagamento-40M-linhas.html) | Operadora de saúde | Synapse · Databricks · Delta Lake · PySpark |
| 03 | [Homologação Byte-a-Byte com CLI Própria](https://wilsonlucas.github.io/data-engineering-portfolio/case-studies/03-homologacao-byte-a-byte.html) | Setor público | Synapse Serverless SQL · OPENROWSET · Python · pyarrow |
| 04 | [Migração de Camada Semântica Corporativa](https://wilsonlucas.github.io/data-engineering-portfolio/case-studies/04-migracao-camada-semantica.html) | Corporativo | TDV · SAP Datasphere · SAP HANA · Oracle |
| 05 | [Medallion + Unity Catalog em Databricks Free Edition](https://wilsonlucas.github.io/data-engineering-portfolio/case-studies/05-medallion-databricks-free-edition.html) ⭐ **público** | Case técnico de processo seletivo (2026) | Databricks Free Edition · Unity Catalog · Delta Lake · PySpark · pytest |
| 06 | [Migração de Lakehouse Azure Synapse para Google Cloud](https://wilsonlucas.github.io/data-engineering-portfolio/case-studies/06-migracao-synapse-gcp-lakehouse.html) | Setor público | BigQuery · Apache Iceberg · Dataproc Serverless · Cloud Composer · CDC |
| 07 | [Virtualização de SAP Datasphere em Azure Databricks](https://wilsonlucas.github.io/data-engineering-portfolio/case-studies/07-virtualizacao-sap-datasphere-databricks.html) | Energia (óleo e gás) | Lakehouse Federation · Unity Catalog · Azure DevOps CI/CD · SAP Datasphere |
| 08 | [FinOps em Azure baseado em evidência](https://wilsonlucas.github.io/data-engineering-portfolio/case-studies/08-finops-azure-otimizacao-custos.html) | Energia (geração hidrelétrica) | Azure Cost Management · Azure Monitor · PowerShell · Azure CLI |
| 09 | [Validação de paridade KNIME → PySpark](https://wilsonlucas.github.io/data-engineering-portfolio/case-studies/09-validacao-paridade-knime-pyspark.html) | Bens de consumo (LATAM) | Databricks · PySpark · pandas · KNIME |

### Padrões Técnicos

| Padrão | Quando usar |
|---|---|
| [Controller-driven Architecture](https://wilsonlucas.github.io/data-engineering-portfolio/technical-notes/controller-driven-architecture.html) | Parametrizar 100% de uma pipeline via tabelas Delta de controle |
| [Dynamic Partition Overwrite](https://wilsonlucas.github.io/data-engineering-portfolio/technical-notes/dynamic-partition-overwrite.html) | Lidar com duplicatas legítimas que quebram MERGE tradicional |
| [Metodologia de Homologação](https://wilsonlucas.github.io/data-engineering-portfolio/technical-notes/homologacao-metodologia.html) | Validar migrações com zero amostragem + causa raiz documentada |

---

## 🛠️ Stack Principal

<table>
<tr>
<td valign="top" width="33%">

**☁️ Azure Lakehouse**
- Azure Databricks · Lakehouse Federation
- Azure Synapse Analytics
- Azure Data Factory
- ADLS Gen2 · Azure Key Vault
- Unity Catalog · Delta Lake
- Azure DevOps Pipelines

**🌐 Google Cloud Lakehouse**
- BigQuery · Apache Iceberg
- Dataproc Serverless
- Cloud Composer (Airflow)
- Cloud Storage · Secret Manager

**💻 Linguagens**
- PySpark · Spark SQL
- Python · pandas · pytest
- T-SQL · PL/SQL · SQL ANSI
- Bash · PowerShell

</td>
<td valign="top" width="33%">

**🏗️ Arquiteturas & Padrões**
- Medallion Architecture
- Controller-driven Architecture
- MERGE idempotente ACID
- Schema Evolution
- Dynamic Partition Overwrite
- Modelagem Dimensional
- SCD Tipo 1 e 2 · CDC

**🧭 Semântica & BI**
- SAP Datasphere · SAP HANA
- TDV (TIBCO Data Virtualization)
- SAP Information Steward
- Power BI

</td>
<td valign="top" width="33%">

**🔧 ETL/ELT Legado**
- SSIS · Informatica PowerCenter
- Pentaho Data Integration (PDI)
- SAS DI 9.4 · SAS Viya · Knime

**🗄️ Bancos**
- Oracle · SQL Server
- PostgreSQL · MySQL

**🤖 DevOps, FinOps & Automação**
- Databricks CLI v0.297+
- Azure CLI · gcloud · Git
- CLIs internas em Python
- Azure Cost Management · Well-Architected · Landing Zone Review
- Elastic Stack (ELK)

</td>
</tr>
</table>

---

## 📊 Em números

<div align="center">

| 40M+ | 542 | 433 | 4,1M | 10+ |
|:---:|:---:|:---:|:---:|:---:|
| **linhas processadas** | **tabelas Iceberg no BigQuery** | **tabelas em Unity Catalog** | **PKs validadas** | **anos de experiência** |
| Folha de pagamento · 12 anos de histórico | Migração Synapse → GCP · 221 com CDC | Seguradora · 50 Silver em MERGE | Zero amostragem · 76 colunas | 4 empresas · 6 setores |

</div>

---

## 🗓️ Trajetória Profissional

| Período | Empresa | Papel |
|---|---|---|
| Jul/2023 – Atual | **Dataside** (Consultoria) | Engenheiro de Dados Sênior — multi-cliente |
| Jun/2021 – Jun/2023 | **VERT** | Engenheiro de Dados Sênior |
| Fev/2019 – Fev/2023 | **Global Web** | DBA Pleno → Consultor DBA |
| Mai/2017 – Jan/2019 | **Stefanini** | Analista de Dados |

**Formação:**
- Especialização em Banco de Dados e Business Intelligence — SENAC (2020)
- Bacharelado em Sistemas de Informação — UNIP (2016)

---

## 📬 Contato

<div align="center">

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/wilson-lucas-719963b4)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/WilsonLucas)
[![E-mail](https://img.shields.io/badge/E--mail-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:wilsonlucas201@gmail.com)

**Brasília, Distrito Federal · Brasil** — aberto a conversas em 2026

</div>

---

## 🧰 Como este repositório foi construído

Este portfolio é um site estático gerado a partir de markdown. Os HTMLs dos case studies não são commitados: o GitHub Actions executa `build_site.py` a cada push e publica o resultado no GitHub Pages em cerca de 1 minuto.

```bash
git add . && git commit -m "docs: update case studies" && git push
``` O design system (navy/cyan/gold) está em [`assets/style.css`](./assets/style.css) e é compartilhado entre landing page, case studies e technical notes.

---

<div align="center">

**Licenciado sob [MIT](./LICENSE)** · Conteúdo conceitual e anonimizado · Sem exposição de dados de clientes

Feito com ❤️ e `PySpark` em Brasília

</div>
