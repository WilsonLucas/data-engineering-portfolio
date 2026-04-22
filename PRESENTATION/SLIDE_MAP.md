# Slide Map: portfolio-wilson

**Palette:** AIDE (navy #0B1F3A / cyan #00B4FF / gold #D4A017 / off-white)
**Total slides:** 19
**Chunks:** 3
**Content source:** portfolio/README.md · portfolio/SOBRE.md · case-studies/01-04 · technical-notes/01-03

**Nota de adaptação — barra de identidade:** Este é um portfólio pessoal, não um deck AIDE. A barra topo deve exibir "WILSON LUCAS · ENGENHEIRO DE DADOS SÊNIOR" (esquerda), "Arquiteturas · Pipelines · Governança" (centro), "Portfolio 2026" (direita). NÃO usar a barra AIDE BRASIL padrão.

---

## Chunk 1 — Abertura e Identidade (slides 1–6)

### Slide 1 — Capa (title)
- Tipo: title (sem branding AIDE)
- Badge topo: `PORTFÓLIO · 2026`
- Título principal (shimmer-text): "Wilson Lucas"
- Subtítulo: "Engenheiro de Dados Sênior"
- Tags: Azure Databricks · Delta Lake · Unity Catalog · PySpark · 10+ anos
- SVG: Não

### Slide 2 — Perfil Resumido (stat-cards)
- 3 stat-cards: 10+ anos / 4 setores / 433+ tabelas
- Heading: "10+ anos construindo plataformas de dados"
- Subtítulo: "Consultoria itinerante em ambientes de alta complexidade — financeiro, saúde, público e corporativo."
- Bottom panel: "Da confiabilidade de dados de missão crítica ao data lakehouse moderno: o mesmo rigor aplicado em cada etapa."

### Slide 3 — Nota de Confidencialidade (callout)
- Ícone cadeado SVG gold 48px
- Heading: "Clientes anonimizados por NDA"
- Corpo: "Os case studies documentam arquitetura, decisões técnicas e padrões aplicados, sem expor código proprietário, dados sensíveis ou informações identificáveis."
- Tags: NDA vigente · Padrões documentados · Sem dados sensíveis

### Slide 4 — Stack Técnica (card-anatomy 6 categorias)
6 linhas coloridas:
- 01 cyan — Cloud e Data Lakehouse — Azure Databricks · Azure Synapse · ADLS Gen2 · Delta Lake · Unity Catalog
- 02 blue — Orquestração e Ingestão — Azure Data Factory · Databricks CLI v0.297+
- 03 gold — Processamento e Linguagens — PySpark · Spark SQL · Python · T-SQL · PL/SQL
- 04 purple — Camada Semântica e BI — SAP Datasphere · TDV · SAP HANA · Power BI
- 05 green — Ferramentas Legadas e ETL — Oracle · SQL Server · SSIS · Informatica PowerCenter · PDI · SAS
- 06 orange — Qualidade e Governança — Homologação byte-a-byte · SAP Information Steward · Knime

### Slide 5 — Visão Geral dos 4 Case Studies (tier-cards 2x2)
4 cards:
- CASE 01 FINANCIAL SERVICES (cyan): "Controller-driven Medallion" — 433 tabelas · 17 Silver · MERGE ACID
- CASE 02 PEOPLE ANALYTICS / RH (green): "Folha de Pagamento 40M+ Linhas" — 8 fatos · 40M+ · 16 BLs · 2013–2026
- CASE 03 SETOR PÚBLICO (gold): "Homologação Byte-a-Byte" — 4,1M PKs · 76 colunas · CLI SynapseCheck
- CASE 04 CORPORATIVO (purple): "Migração Camada Semântica" — Oracle → TDV → SAP Datasphere

### Slide 6 — Divider "Case Studies"
- Ghost-number 01
- Heading: "Case Studies"

---

## Chunk 2 — Case Studies (slides 7–12)

### Slide 7 — Case 1: Controller-driven Medallion (SVG architecture)
- Setor: SEGURADORA NACIONAL · FINANCIAL SERVICES
- Diagrama SVG: SQL Server → Landing → Bronze → Silver + controller layer abaixo (4 tabelas)
- Métricas: 433 Bronze · 17 Silver · ciclo 45→18 min · inconsistência 0
- ViewBox: 1100×460

### Slide 8 — Case 2: Folha de Pagamento 40M+ (pipeline + stats)
- Setor: OPERADORA DE SAÚDE · PEOPLE ANALYTICS / RH
- Pipeline 5 estágios: Fonte RH → Transient/Raw → Trusted → Refined → Synapse
- 3 stats: 40M+ linhas / 8 fatos / 16 BLs
- ViewBox pipeline: 1000×220

### Slide 9 — Case 3: Homologação Byte-a-Byte (method + CLI)
- Setor: SETOR PÚBLICO · MPE
- Esquerda: 3 blocos (Universo / Diff / Causa Raiz) com OPENROWSET
- Direita: CLI SynapseCheck com 6 comandos
- ViewBox: 960×400

### Slide 10 — Case 4: Migração Camada Semântica (SVG horizontal)
- Setor: CORPORATIVO · GRANDE PORTE
- 3 estágios: Oracle → TDV → SAP Datasphere
- ViewBox: 1050×380

### Slide 11 — Resultados dos Cases (timeline-grid)
- 4 colunas com dot colorido por case
- Cada coluna: Ano · Título · Tagline · Detail · Stat pill

### Slide 12 — Divider "Padrões e Resultados"
- Ghost-number 02
- Heading: "Padrões Técnicos"

---

## Chunk 3 — Padrões, Resultados, Fechamento (slides 13–19)

### Slide 13 — Padrão 1: Controller-driven (comparison)
- Esquerda (red dim): "N tabelas = N artefatos"
- vs
- Direita (cyan glow): "N tabelas = N linhas na controller"
- Stats: 10 commits vs 433 tabelas zero mudança

### Slide 14 — Padrão 2: Dynamic Partition Overwrite (comparison + code)
- Problema: duplicatas legítimas quebram MERGE
- Solução: `spark.sql.sources.partitionOverwriteMode=dynamic`
- Tabela comparativa: Overwrite / MERGE ACID / DPO

### Slide 15 — Padrão 3: Homologação Zero Amostragem (phase-flow)
- Fase 1 Universo (cyan): FULL OUTER JOIN, 4,1M PKs
- Fase 2 Diff por Coluna (gold): 76 colunas, 21 PASS absoluto
- Fase 3 Causa Raiz (green): 55 colunas documentadas

### Slide 16 — Resultados Consolidados (stat-cards grandes)
5 big numbers:
- 40M+ linhas · 433 tabelas · 4,1M PKs · 16 BLs · 10+ anos

### Slide 17 — Divider "Filosofia de Trabalho"
- Ghost-number 03
- Heading: "Filosofia de Trabalho"

### Slide 18 — Filosofia (tier-cards 4 princípios)
- Documentação é produto (gold)
- Homologação antes do go-live (cyan)
- Automação como multiplicador (green)
- Padrões reduzem carga cognitiva (purple)

### Slide 19 — Contato e Fechamento (closing-quote + CTA)
- Quote: "Engenharia de dados é engenharia de software aplicada a dados."
- Contato:
  - LinkedIn: linkedin.com/in/wilson-lucas-719963b4
  - GitHub: github.com/WilsonLucas (repos privados)
  - E-mail: wilsonlucas201@gmail.com
- Tag CTA: "Aberto a conversas · 2026"
