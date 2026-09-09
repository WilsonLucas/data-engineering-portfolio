# Case Study 05 — Medallion + Unity Catalog em Databricks Free Edition

**Setor:** Consultoria de tecnologia (case técnico de processo seletivo, 2026)
**Empregador:** N/A — projeto pessoal de processo seletivo
**Papel:** Engenheiro de Dados Sênior — solução end-to-end individual

---

## Setor e Perfil do Projeto

Case técnico aplicado em processo seletivo para vaga de Engenheiro de Dados Sênior em consultoria especializada em Databricks. Diferente dos demais case studies deste portfolio (anonimizados por NDA de clientes da consultoria), **este case é integralmente público e reproduzível** — o repositório no GitHub está aberto, os dados são sintéticos sem informação sensível e qualquer recrutador pode clonar e rodar localmente em um workspace Free Edition.

O contexto fictício do enunciado: empresa de serviços com operação nacional cujos dados estão distribuídos em 9 fontes brutas heterogêneas (ERP, CRM, API, planilhas, sistema legado, atendimento, logística), sem base consolidada para consumo analítico. O candidato deve estruturar a solução end-to-end e entregar tabelas finais prontas para um Analista de BI montar dashboards.

---

## Problema

**Técnico:** Como ingerir 9 fontes em 5 formatos heterogêneos (CSV `;`, CSV `,`, JSON aninhado, NDJSON, XLSX, TXT pipe-delimited), tratar qualidade variável dos dados (51 issues mapeadas) sem destruir registros problemáticos, modelar em star schema dimensional com SCD2 demonstrativa, e aplicar governança Unity Catalog completa — tudo em **Databricks Free Edition** (substituiu Community Edition em jun/2025; só serverless, sem DLT, sem Workflows agendados, sem RBAC granular).

**Operacional:** Como entregar não apenas uma solução funcional mas uma solução que demonstre profundidade arquitetural em vaga sênior — incluindo decisões justificadas (ADRs), catálogo vivo, observabilidade, testes automatizados e narrativa visual.

**Escala do desafio:**
- 403 pedidos, 995 itens, 72 produtos, 180 clientes pós-dedup, 7 canais, 6 regiões canônicas, 40 vendedores, 325 entregas, 270 ocorrências
- 9 fontes em 5 formatos diferentes
- Reconciliação end-to-end Bronze=Silver=Gold = R$ 1.707.675,84 (zero divergência aceita)
- Prazo: 4 dias corridos

---

## Stack Aplicada

| Componente | Tecnologia |
|------------|-----------|
| Plataforma | Databricks Free Edition (serverless) |
| Catalog | Unity Catalog (catalog `workspace`) |
| Storage | UC Managed Volumes (substitui DBFS legado) |
| Formato | Delta Lake 3.x (com `delta.columnMapping.mode=name`) |
| Engine | Photon Spark 4.1 (ANSI SQL mode estrito) |
| Linguagem | PySpark + Spark SQL + Python 3.10+ |
| Orquestração | Databricks Multi-task Jobs (DAG JSON, 13 tasks) |
| Versionamento | Git + GitHub Pages |
| Qualidade | pytest + chispa offline (Spark local[1]) |
| Lint/format | black + ruff (configurado em pyproject.toml) |

---

## Arquitetura

```
9 fontes brutas (UC Volume: workspace.landing.sources)
   ├── erp_pedidos_cabecalho.csv (CSV ;)
   ├── erp_pedidos_itens.csv     (CSV ,)
   ├── cadastro_produtos_api_dump.json (JSON aninhado)
   ├── crm_clientes_export.xlsx
   ├── comercial_canais.xlsx
   ├── vendedores.csv
   ├── legado_regioes_pipe.txt
   ├── logistica_entregas.json
   └── atendimento_ocorrencias.ndjson
        │
        ▼
workspace.bronze  (9 tabelas string-typed, ADR-001)
        │
        ▼
workspace.silver  (9 tabelas tipadas + DQ flags + quarantine pattern, ADR-005)
        │
        ▼
workspace.gold    (6 dims SCD1 + 1 SCD2 + 4 facts + 1 view consolidada, ADR-002 e ADR-003)
        │
        ▼
Analista BI (vw_kpi_business pré-joinada + dim_cliente_history range join)
```

**Camada Landing:** UC Volume managed hospedando os 9 arquivos originais. Substitui DBFS legado.

**Camada Bronze:** ingestão 1:1 sem transformação. Tipos string preservam formato original (decisão documentada em ADR-001 — preserva valor para auditoria forense; casts ficam restritos ao Silver com `try_cast` ANSI-safe via `F.expr`).

**Camada Silver:** parsing multi-formato (datas em yyyy-MM-dd, dd/MM/yyyy, ISO 8601 com 'T'; decimais BR com vírgula), dedup determinístico via window functions com `row_number`, padronização de enums, DQ flags inline (`_dq_status` + `_dq_reasons` array). Pattern quarantine para `_dq_status='rejected'` documentado em ADR-005.

**Camada Gold:** star schema dimensional. 6 dimensões SCD Type 1 (`dim_cliente`, `dim_produto`, `dim_canal`, `dim_regiao`, `dim_vendedor`, `dim_data` enriquecida com 12 feriados nacionais BR 2025) + 1 SCD Type 2 demonstrativa (`dim_cliente_history` com hash MD5 sobre 4 colunas tracking + MERGE pattern Delta + range join sem surrogate key). 4 facts (pedido, item, entrega, ocorrência) com FK informational e CHECK constraint enforced. 1 view consolidada `vw_kpi_business` pré-joinada para BI.

---

## Decisões Técnicas-Chave (5 ADRs Nygard PT-BR)

### ADR-001 — Bronze como string-typed

Em ANSI SQL mode estrito (default no Photon Spark 4.1 do Free Edition), qualquer cast falho lança `CAST_INVALID_INPUT` em runtime — interrompendo o job. `inferSchema=True` em CSV é não-determinístico em datasets pequenos. Adotamos Bronze string-typed: ler tudo como STRING preservando o conteúdo original byte-a-byte; tipos finais são responsabilidade exclusiva do Silver via `try_cast`, `try_to_date`, `try_to_timestamp` chamados via `F.expr` (funções não expostas como wrappers Python).

### ADR-002 — SCD1 padrão + SCD2 demonstrativa em tabela paralela

`dim_cliente` permanece SCD1 (overwrite, estado atual). `dim_cliente_history` é tabela paralela SCD2 com hash MD5 sobre `(segmento, UF, cidade, status)`. `effective_date = _ingestion_timestamp` (documentado como surrogate para business date desconhecida). FK natural com range join (`fact.data_id BETWEEN h.effective_date AND h.end_date`) — surrogate key documentada como follow-up para produção. MERGE pattern Delta para ingestões subsequentes.

### ADR-003 — Medallion 3-camadas + landing com schemas curtos

Schemas namespaced no catalog `workspace`: `bronze`, `silver`, `gold`, `landing`. Sem prefixo redundante — pattern canônico do Databricks Engineering Blog. Versão anterior usava `case_<projeto>_*` (poluía o Catalog Explorer com prefixo desnecessário em ambiente single-tenant). Refatorado durante o sprint aproveitando re-run obrigatório pós-bug-fix.

### ADR-004 — Free Edition vs Premium (limitações conscientes)

Sem DLT (Delta Live Tables), sem Workflows agendados, sem RBAC granular além do owner do workspace, sem `system.access.audit_log`. Cada limitação tem mitigação arquitetural documentada: DLT substituída por DQ flags + quarantine manuais (ADR-005); Workflows substituídos por `databricks jobs submit` on-demand; RBAC documentado via tags UC. Roadmap explícito de migração para Premium incluído.

### ADR-005 — DQ flags + Quarantine pattern (equivalente arquitetural a DLT)

Classificação por **TIPO** de issue (não por contagem): PK/FK/null obrigatório → `rejected`; formato/enum não canônico → `warning`; sem issues → `clean`. Quarantine pattern: split sobre o `bronze_df` ANTES dos casts; `silver.quarantine_<entity>` preserva colunas originais string-typed do bronze + 4 metadados (`_quarantine_timestamp`, `_quarantine_reason`, `_source_file`, `_ingestion_timestamp`). Auditoria forense de cada registro rejeitado mantém valor original.

---

## Bug Raiz Descoberto Durante Auditoria Forense

Durante o sprint de uplift via SDD (`/brainstorm` → `/define-m` → `/design-m` → `/build` → `/ship`), 2 specialists em paralelo identificaram bugs aparentemente independentes em direções opostas:

- **silver_pedidos** marcava `rejected = 0` artificialmente (pattern `otherwise(F.lit("warning"))` impedia o terceiro estado de ser atingido)
- **silver_ocorrencias** marcava 270/270 tickets como `rejected` (pattern `F.lit(F.concat(...))` força não-null serializado, enchendo o array de razões)

Investigação SQL direta revelou a **causa raiz comum**: `F.array_remove(arr, NULL)` em SQL retorna NULL (porque `NULL = NULL` é NULL, não TRUE — semântica three-valued logic). Estava em todos os 6 silvers e mascarava qualquer lógica DQ subsequente.

```sql
-- Bug latente em produção
SELECT array_remove(array(NULL, NULL, 'real'), NULL)
-- retorna NULL (array inteiro), não ['real']

-- Fix correto
SELECT array_compact(array(NULL, NULL, 'real'))
-- retorna ['real']
```

**Resolução:** substituir `F.array_remove(arr, None)` por `F.array_compact(arr)` em 6 silvers via regex multi-line. Distribuição DQ pós-fix: silver_pedidos com 24 rejected reais, silver_ocorrencias com 8 rejected, 193 warning, 69 clean — distribuição realista pela primeira vez.

**Lição aprendida:** code review futuro deve grep por `F.lit(F.<func>(...))` como red flag (uso indevido de `F.lit` em expressões Column).

---

## Governança Unity Catalog (Catálogo Vivo)

Aplicada via script idempotente `01_apply_governance.py` cobrindo 28 tabelas:

| Item | Status |
|------|--------|
| COMMENT ON TABLE em todas as tabelas | 100% (28/28) |
| COMMENT ON COLUMN em colunas business gold | 100% |
| Tags UC fixas (owner, layer, classification, pii, data_domain) | 5 em todas as 28 |
| TBLPROPERTIES Delta retention (`logRetentionDuration=30 days`) | Aplicado em gold |
| CHECK constraint enforced em fact_pedido | `chk_net_amount_nonneg` |
| Pattern `DROP CONSTRAINT IF EXISTS` antes de `ADD CONSTRAINT` | Garante idempotência |

Vocabulário controlado das 5 tags documentado em `docs/NAMING_CONVENTIONS.md`. Fallback automático via função `fallback_comment(table, column)` para colunas business não mapeadas explicitamente — garante 100% de cobertura mesmo em tabelas com schemas evolutivos.

---

## Resultado

| Métrica | Valor |
|---------|-------|
| Pipeline end-to-end wall-clock | ~6 minutos (Free Edition serverless, 13 tasks) |
| Reconciliação Bronze=Silver=Gold | R$ 1.707.675,84 (mantida pós rename schemas + 3 bug fixes) |
| Tabelas com COMMENT non-empty | 28/28 (100%) |
| Tags UC fixas por tabela | 5 em todas |
| ADRs Nygard PT-BR | 5 |
| Documentos de apoio | 8 (TABLES, GLOSSARY, NAMING_CONVENTIONS, BI_RUNBOOK, data_model com bus matrix, data_quality, business_questions, architecture) |
| Testes pytest+chispa | 15 casos (Camada 1 Python puro + Camada 2 Spark local) |
| HTML deliverables (GitHub Pages) | 2 (slides 12 páginas + diagrama interativo) |

---

## Diferenciais vs Solução Mínima Esperada

O enunciado do case pedia uma solução funcional com notebooks + documentação técnica + resumo executivo. Esta entrega adicionou:

- **5 ADRs Nygard PT-BR** documentando decisões com Contexto/Decisão/Consequências
- **Bus Matrix Kimball** mapeando dimensões conformed × facts no `data_model.md`
- **SCD Type 2 demonstrativa** com hash MD5 + range join + MERGE pattern (paralela a SCD1)
- **`dim_data` enriquecida** com 12 feriados nacionais BR 2025 + `eh_dia_util` derivado + `semana_iso`
- **Governança UC viva** via script idempotente (COMMENT + TAG + CHECK constraint)
- **Suite pytest + chispa offline** com 15 casos cobrindo funções utils + DQ logic
- **Slides HTML magazine-quality** (12 páginas) renderizados via GitHub Pages
- **Diagrama de arquitetura interativo** mostrando as 28 tabelas com info de granularidade
- **Data dictionary, glossário, naming conventions, BI runbook** (4 docs adicionais)

---

## Repositório Público

**Código completo:** [github.com/WilsonLucas/case-data-engineer](https://github.com/WilsonLucas/case-data-engineer)

**Slides renderizados:** [wilsonlucas.github.io/case-data-engineer/docs/slides/case_levva.html](https://wilsonlucas.github.io/case-data-engineer/docs/slides/case_levva.html)

**Diagrama interativo:** [wilsonlucas.github.io/case-data-engineer/docs/architecture.html](https://wilsonlucas.github.io/case-data-engineer/docs/architecture.html)

---

## Por Que Este Case Está no Portfolio

Os demais case studies deste portfolio são anonimizados por NDA de clientes da consultoria — o recrutador lê descrições mas não consegue clicar e ver código real. **Este case inverte essa limitação:** dados sintéticos sem NDA, exigência de publicação em GitHub público é parte do próprio enunciado, e o recrutador pode clonar o repositório e reproduzir o pipeline em qualquer Databricks Free Edition.

Demonstra na prática: Medallion architecture, Unity Catalog governance, modelagem dimensional Kimball com SCD2, padrões de DQ não-destrutivos, ANSI mode handling, idempotência via Delta Lake, multi-task DAG orchestration, testes pytest, ADRs Nygard, e workflow SDD (Spec-Driven Development) para auditoria forense.
