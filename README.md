<div align="center">

# Wilson Lucas — Engenheiro de Dados Sênior

### Arquiteturas de dados robustas · Pipelines homologados em produção · Automação que escala

[![Portfolio](https://img.shields.io/badge/🌐_Portfolio_ao_vivo-wilsonlucas.github.io-0B1F3A?style=for-the-badge&labelColor=D4A017)](https://wilsonlucas.github.io/data-engineering-portfolio/)
[![Slides](https://img.shields.io/badge/🎬_Apresentação-19_slides-00B4FF?style=for-the-badge&labelColor=0B1F3A)](https://wilsonlucas.github.io/data-engineering-portfolio/PRESENTATION/portfolio-slides.html)
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
- 🎬 **[Apresentação em slides (19 telas)](https://wilsonlucas.github.io/data-engineering-portfolio/PRESENTATION/portfolio-slides.html)** — para uma leitura de ~10 min
- 📄 **Este README** — para navegar pelos arquivos do repositório diretamente

---

## 🧭 O que você encontra aqui

| Seção | Conteúdo |
|---|---|
| [**Landing page**](https://wilsonlucas.github.io/data-engineering-portfolio/) | Portfolio visual completo |
| [**Case Studies**](./case-studies/) | 4 projetos reais documentados (anonimizados por NDA) |
| [**Padrões Técnicos**](./technical-notes/) | 3 padrões reutilizáveis extraídos dos projetos |
| [**Slides**](./PRESENTATION/) | Apresentação em 19 slides single-file HTML |
| [**Bio expandida**](./SOBRE.md) | Narrativa de trajetória e filosofia de trabalho |

---

## 🔒 Confidencialidade

> Os clientes não são citados nominalmente devido a acordos de confidencialidade vigentes. Os case studies documentam **arquitetura, decisões técnicas e padrões aplicados** — sem expor código proprietário, dados sensíveis ou informações identificáveis. Descritores de setor (seguradora nacional, operadora de saúde, setor público, corporativo) substituem nomes reais em todo o material.

---

## 📂 Case Studies

| # | Projeto | Setor | Stack Central |
|---|---------|-------|---------------|
| 01 | [Controller-driven Medallion Architecture](./case-studies/01-controller-driven-medallion.md) | Seguradora nacional | Azure Data Factory · Databricks · Unity Catalog · Delta Lake |
| 02 | [Folha de Pagamento — 40M+ Linhas](./case-studies/02-folha-pagamento-40M-linhas.md) | Operadora de saúde | Synapse · Databricks · Delta Lake · PySpark |
| 03 | [Homologação Byte-a-Byte com CLI Própria](./case-studies/03-homologacao-byte-a-byte.md) | Setor público | Synapse Serverless SQL · OPENROWSET · Python · pyarrow |
| 04 | [Migração de Camada Semântica Corporativa](./case-studies/04-migracao-camada-semantica.md) | Corporativo | TDV · SAP Datasphere · SAP HANA · Oracle |

### Padrões Técnicos

| Padrão | Quando usar |
|---|---|
| [Controller-driven Architecture](./technical-notes/controller-driven-architecture.md) | Parametrizar 100% de uma pipeline via tabelas Delta de controle |
| [Dynamic Partition Overwrite](./technical-notes/dynamic-partition-overwrite.md) | Lidar com duplicatas legítimas que quebram MERGE tradicional |
| [Metodologia de Homologação](./technical-notes/homologacao-metodologia.md) | Validar migrações com zero amostragem + causa raiz documentada |

---

## 🛠️ Stack Principal

<table>
<tr>
<td valign="top" width="33%">

**☁️ Cloud & Data Lakehouse**
- Azure Databricks
- Azure Synapse Analytics
- Azure Data Factory
- ADLS Gen2 · Azure Key Vault
- Unity Catalog · Delta Lake
- Oracle Cloud Infrastructure

**💻 Linguagens**
- PySpark · Spark SQL
- Python · T-SQL · PL/SQL
- SQL ANSI · Bash

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

**🤖 DevOps & Automação**
- Databricks CLI v0.297+
- Azure CLI · Git
- CLIs internas em Python
- Elastic Stack (ELK)

</td>
</tr>
</table>

---

## 📊 Em números

<div align="center">

| 40M+ | 433 | 4,1M | 16 | 10+ |
|:---:|:---:|:---:|:---:|:---:|
| **linhas processadas** | **tabelas catalogadas** | **PKs validadas** | **bloqueios resolvidos** | **anos de experiência** |
| Folha de pagamento · 12 anos de histórico | Unity Catalog · Bronze → Silver | Zero amostragem · 76 colunas | BL01–BL16 documentados | 5 empresas · 4 setores |

</div>

---

## 🗓️ Trajetória Profissional

| Período | Empresa | Papel |
|---|---|---|
| Dez/2024 – Atual | **Dataside** (Consultoria) | Engenheiro de Dados Sênior — multi-cliente |
| Jul/2023 – Atual | **Compass.UOL** | Engenheiro de Dados Sênior |
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

Este portfolio é um site estático gerado a partir de markdown. Para regenerar os HTMLs após editar um `.md`:

```bash
python build_site.py
git add . && git commit -m "docs: update case studies" && git push
```

O GitHub Pages faz redeploy automático em ~1 minuto. O design system (navy/cyan/gold) está em [`assets/style.css`](./assets/style.css) e é compartilhado entre landing page, case studies e technical notes.

---

<div align="center">

**Licenciado sob [MIT](./LICENSE)** · Conteúdo conceitual e anonimizado · Sem exposição de dados de clientes

Feito com ❤️ e `PySpark` em Brasília

</div>
