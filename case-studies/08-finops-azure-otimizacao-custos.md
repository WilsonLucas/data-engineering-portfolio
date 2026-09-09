# Case Study 08 — FinOps em Azure: Otimização de Custos Baseada em Evidência

**Setor:** Energia — concessionária de geração hidrelétrica
**Empregador:** Consultoria de dados
**Papel:** Engenheiro responsável único — coleta, análise, documentação e execução das mudanças
**Período:** Julho a agosto de 2026 (Fase 1 executada, Fase 2 em deliberação)

---

## Setor e Perfil do Projeto

Uma concessionária de geração de energia mantinha uma assinatura Azure dominada por workloads de chatbot e IA, com ambientes de desenvolvimento e produção convivendo em grupos de recursos duplicados. O custo era estável mês a mês, o que indicava recursos provisionados 24 horas por dia independentemente de uso.

A consultoria foi contratada para reduzir o gasto. A abordagem escolhida foi tratar o problema como engenharia: medir antes de recomendar, provar ociosidade estrutural em vez de sazonal, e executar apenas mudanças com reversibilidade documentada.

---

## Problema

**Técnico:** Como identificar o que está ocioso em uma estate 100% PaaS (sem máquinas virtuais clássicas), onde os levers convencionais de Reserved Instances e Savings Plans não se aplicam, e onde dependências entre aplicações podem estar escondidas em variáveis de ambiente, imagens de container e cofres de segredo?

**Operacional:** O Azure Monitor limita cada consulta a cerca de 30 dias e retém 93 dias de métricas. Uma janela curta não distingue ociosidade estrutural de um mês fraco. A coleta precisava ser desenhada em blocos e a execução precisava passar por aprovação escrita do cliente.

**Escala do desafio:**
- Cerca de R$ 11,7 mil por mês de baseline, R$ 70 mil no primeiro semestre
- 10 aplicações, 5 registros de container, 6 a 7 instâncias de busca cognitiva ao mesmo preço, 2 grupos de recursos de produção duplicados

---

## Stack Aplicada

| Componente | Tecnologia |
|------------|-----------|
| Coleta de custo | Azure Cost Management API (ActualCost) |
| Coleta de métricas | Azure Monitor metrics API, 90 dias em 3 blocos de 30 |
| Automação | PowerShell e Azure CLI, scripts read-only e scripts de execução separados |
| Reversibilidade | Exportação ARM template por recurso antes de excluir, lock `CanNotDelete` reversível |
| Serviços analisados | Azure Cognitive Search, App Service Plans, VPN Gateway, PostgreSQL, API Management, Container Registry, Cosmos DB, NAT Gateway, Private Endpoints, Azure OpenAI |
| Documentação | HTML gerado para PDF via Chrome headless, convenção própria de documento de levantamento e de implementação |

---

## Método

```
Baseline de custo (6,5 meses)
        │
        ▼
Levantamento v1 (30 dias de métricas)
        │   janela insuficiente para descartar sazonalidade
        ▼
Levantamento v2 (90 dias, 3 blocos de 30)
        │   ociosidade confirmada como estrutural
        │   contra-evidência encontrada: planos B1 em uso a 68-75% de memória
        ▼
Mapeamento de dependências (10 apps, 5 registries)
        │   app settings, connection strings, runtime, pull/push em 90 dias
        ▼
Fase 1: exclusões com risco zero, aprovação escrita, ARM export, verificação
        │
        ▼
Fase 2: consolidações e rightsizing, em deliberação com o cliente
```

Cada script valida a assinatura antes de qualquer lote, porque o contexto padrão da máquina pode reverter para outro tenant.

---

## Achados

| Achado | Evidência | Implicação |
|--------|-----------|------------|
| Desenvolvimento custa mais que produção | R$ 4,7 mil contra R$ 3,0 mil por mês | Um VPN Gateway de R$ 2 mil por mês estava no grupo de desenvolvimento, o recurso mais caro da assinatura |
| Busca cognitiva é 30% da conta | 6 a 7 instâncias ao mesmo preço | Duplicação clara, candidata a consolidação |
| Produção em tier compartilhado | Apps de produção do chatbot em plano D1 Shared dentro do grupo de desenvolvimento | Sem SLA, grupo errado: novo item de Fase 2 |
| Plano Premium com CPU média de 3,5% | Hospeda a API do bot de WhatsApp ao vivo | Rightsizing tratado como sensível a produção apesar do nome do grupo de recursos |
| 4 App Service Plans vazios | `numberOfSites = 0` verificado contra web apps, function apps e slots, mais 90 dias de métricas | Excluídos na Fase 1 |

O mapeamento de dependências mudou a classe de risco de uma recomendação: o plano Premium subutilizado parecia candidato óbvio a redução até a inspeção mostrar que servia a API de produção.

---

## Resultados Quantificados

| Métrica | Valor |
|---------|-------|
| Quick wins quantificados | R$ 4,4 mil a R$ 5,7 mil por mês (40% a 48% da conta) |
| Economia total estimada no levantamento v2 | Cerca de 70% |
| Fase 1 executada | 4 App Service Plans vazios excluídos (1 Premium P1mv3 e 3 B1) |
| Economia ativada na Fase 1 | Cerca de R$ 1,5 mil por mês, zero impacto em workload |
| Reversibilidade | ARM export por plano antes da exclusão |
| Lock aplicado | Grupo de recursos duplicado com zero consultas em 90 dias, reversível em um comando |
| Aplicações mapeadas | 10, com valores sensíveis redigidos nas evidências |

---

## Achado Relacionado em Outro Cliente

No mesmo período, uma auditoria de arquitetura Azure em uma seguradora encontrou o protocolo SFTP habilitado e ocioso em duas storage accounts, cobrado por hora de habilitação: cerca de R$ 2,2 mil por mês, ou R$ 26,8 mil por ano, por uma feature sem uso. A auditoria também reconciliou a estimativa da calculadora de preços com o custo real em 11% de diferença, explicada integralmente pelo uso de instâncias Spot que a calculadora não modela. Detalhes no [Case 01](01-controller-driven-medallion.md).

---

## Decisões Notáveis

**Reserved Instances e Savings Plans despriorizados com justificativa.** A conta é 100% PaaS. Os levers são tier, SKU, consolidação e desligamento, não compromisso de compute.

**Triplicar a janela de medição antes de recomendar.** O levantamento v1 com 30 dias foi refeito com 90 dias especificamente para descartar sazonalidade. O resultado confirmou ociosidade estrutural e, ao mesmo tempo, retirou da lista planos que pareciam ociosos mas rodavam a 98% a 100% de CPU em picos.

**Limitação declarada por escrito.** Referências via Key Vault, variáveis de imagem de container ou endpoints fixos em código são invisíveis ao método de mapeamento. Por isso, exclusões da Fase 2 mantêm confirmação funcional prévia como pré-requisito.

**Convenção de documento portável.** Levantamento e implementação ganharam códigos sequenciais por tipo e ano, disclaimer de estimativa em toda cifra e um prompt reutilizável para o padrão viajar a outros clientes.

---

## Lições Aprendidas

**Zero em uma consulta não é ausência.** Sob permissão restrita a grupo de recursos, o CLI devolve vazio e não erro. Vários "zeros" do primeiro dia de auditoria eram cegueira de permissão, não fato. Trocar para a conta de serviço correta reabriu Cost Management, métricas e políticas.

**O nome do grupo de recursos não define o risco.** Um recurso "de desenvolvimento" pode ser produção. Só o mapeamento de dependências responde.

**Economia executada vale mais que economia estimada.** R$ 1,5 mil por mês ativados com zero impacto e reversão pronta pesam mais na conversa com o cliente do que os 70% projetados.
