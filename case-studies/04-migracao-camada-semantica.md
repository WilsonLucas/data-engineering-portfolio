# Case Study 04 — Migração de Camada Semântica Corporativa

**Setor:** Corporativo (grande empresa, ecossistema SAP + Oracle)
**Empregador:** Compass.UOL
**Papel:** Engenheiro de Dados Sênior — ponto focal técnico

---

## Setor e Perfil do Projeto

Projeto de modernização de arquitetura de acesso a dados em uma organização corporativa de grande porte, com múltiplas áreas consumidoras de dados (analytics, BI, relatórios regulatórios) e histórico de décadas de regras de negócio consolidadas em fontes heterogêneas.

A organização tinha duas frentes de trabalho simultâneas: (1) consolidação de fontes legadas dispersas em uma camada de virtualização (Oracle → TDV), e (2) modernização da camada semântica corporativa, migrando do TDV para SAP Datasphere como nova plataforma de governança e acesso a dados.

O desafio não era apenas técnico — era de continuidade de contrato. Dezenas de áreas consumidoras dependiam de entidades e métricas que tinham sido construídas e refinadas ao longo de anos. Qualquer ruptura nesse contrato, mesmo técnica, traduzia-se em retrabalho para squads de analytics e BI que usavam esses dados como insumo de suas entregas.

---

## Problema

**Arquitetural:** Como migrar uma camada semântica corporativa que acumula anos de regras de negócio sem interromper o contrato de dados com os consumidores downstream?

**Técnico (frente 1):** Como consolidar múltiplas fontes Oracle legadas heterogêneas — com schemas inconsistentes, nomenclaturas divergentes e regras de joining acumuladas ao longo de anos — em uma camada unificada via TDV?

**Técnico (frente 2):** Como replicar no SAP Datasphere o comportamento semântico exato das entidades TDV, preservando a experiência dos consumidores de BI e analytics que já conhecem e confiam nos dados que recebem?

**Operacional:** Como manter a estabilidade da base Oracle de missão crítica — que serve múltiplas áreas e é o coração do ambiente produtivo — enquanto a migração acontece em paralelo?

---

## Stack Aplicada

| Componente | Papel no Projeto |
|------------|-----------------|
| Oracle (on-premises) | Fonte principal de dados transacionais / operacionais |
| TDV (TIBCO Data Virtualization) | Camada de virtualização e consolidação de fontes |
| SAP Datasphere | Nova plataforma semântica corporativa (destino da migração) |
| SAP HANA | Banco de dados analítico subjacente ao SAP Datasphere |
| SAP Information Steward | Qualidade de dados e monitoramento de consistência |
| Azure Synapse Analytics | Processamento complementar de dados analíticos |
| Azure Databricks | Transformações em escala para alimentação de camadas analíticas |
| Knime | Automação de fluxos de qualidade e integração de dados |

---

## Arquitetura

```
Fontes legadas                 Camada de Virtualização        Camada Semântica Nova
──────────────                 ──────────────────────         ─────────────────────
Oracle DB 1                    TDV                            SAP Datasphere
  [esquema_A]  ──────────────► [view_consolidada_X]  ──────► [entidade_semântica_X]
  [esquema_B]  ──────────────► [view_consolidada_Y]  ──────► [entidade_semântica_Y]
Oracle DB 2                    [view_consolidada_Z]  ──────► [entidade_semântica_Z]
  [esquema_C]  ──────────────►
SAP HANA                                                             │
  [tabela_D]   ──────────────►                                       ▼
                                                           Consumidores downstream
                                                           ─────────────────────
                                                           Power BI / BI tools
                                                           Squads de analytics
                                                           Relatórios regulatórios

                    ┌──────────────────────────────────────┐
                    │  SAP Information Steward             │
                    │  (monitoramento de qualidade         │
                    │   ao longo do pipeline)              │
                    └──────────────────────────────────────┘

                    ┌──────────────────────────────────────┐
                    │  Azure Synapse + Databricks          │
                    │  (processamento para alimentação     │
                    │   de camadas analíticas específicas) │
                    └──────────────────────────────────────┘
```

**TDV como camada intermediária:** a decisão de manter o TDV como estágio intermediário antes do SAP Datasphere não foi arquitetural apenas — foi pragmática. O TDV já estava em produção e as views construídas nele representavam contratos implícitos de dados que os consumidores conheciam. Migrar direto de Oracle para SAP Datasphere teria exigido reescrever toda a semântica de uma vez. A abordagem em dois estágios permitiu validar a consolidação Oracle → TDV antes de avançar para TDV → Datasphere.

---

## Decisões Técnicas-Chave

### 1. Abordagem incremental vs. big bang

**Alternativa descartada:** migrar todas as entidades de uma vez (big bang), com um corte de data único.

**Decisão adotada:** migração incremental por domínio de negócio, com validação e homologação de cada domínio antes de avançar para o próximo. Consumidores do domínio migrado fazem o corte para o SAP Datasphere enquanto outros ainda consomem do TDV.

**Justificativa:** Em um ambiente com dezenas de consumidores, um big bang cria um ponto único de falha massiva. A abordagem incremental limita o raio de impacto de qualquer problema para um domínio por vez.

---

### 2. Preservação do contrato de dados como restrição primária

**Princípio aplicado:** a migração é bem-sucedida apenas quando o consumidor downstream não percebe diferença no comportamento dos dados — mesmas granularidades, mesmas métricas calculadas da mesma forma, mesmos filtros implícitos.

**Implicação técnica:** antes de migrar qualquer entidade TDV para SAP Datasphere, a entidade equivalente no Datasphere era validada side-by-side com a original TDV por pelo menos uma competência completa de dados. Apenas após validação com PASS o consumidor era instruído a migrar seu endpoint.

---

### 3. Sustentação da base Oracle como fundação do projeto

**Contexto:** Assumir a sustentação da principal base Oracle do ambiente como ponto focal da equipe significou ser o responsável por estabilidade, disponibilidade e rastreabilidade de dados críticos durante todo o período da migração.

**Por que é tecnicamente relevante:** manter uma base Oracle legada estável enquanto você conecta novas camadas sobre ela — TDV, Synapse, Databricks — exige entender profundamente os contratos implícitos que a base mantém: views que encapsulam regras de negócio não documentadas, procedures que executam lógicas acumuladas ao longo de anos, índices e configurações de performance que não têm documentação formal.

**Abordagem:** mapeamento defensivo de todas as dependências antes de qualquer mudança. Nenhuma alteração no ambiente Oracle sem análise de impacto documentada.

---

### 4. Monitoramento de qualidade com SAP Information Steward

**Desafio:** com fontes heterogêneas sendo consolidadas no TDV e depois migradas para o Datasphere, como garantir que a qualidade dos dados se mantém ao longo de todo o pipeline?

**Solução:** regras de qualidade implementadas no SAP Information Steward cobrindo completude (campos obrigatórios não nulos), consistência (valores dentro de domínios esperados) e integridade referencial (FKs respeitadas entre entidades relacionadas). Alertas automáticos acionados quando qualquer regra é violada.

---

### 5. Knime para automação de fluxos de integração

**Contexto:** alguns fluxos de integração de dados não se encaixavam na arquitetura principal (TDV → Datasphere) por terem requisitos específicos de transformação ou por serem feeds pontuais de sistemas externos.

**Solução:** Knime como ferramenta de automação visual para esses fluxos — conectando Oracle, ADLS e SAP HANA com lógica de transformação encapsulada em workflows auditáveis.

---

## Dimensão do Ambiente

| Aspecto | Escala |
|---------|--------|
| Fontes heterogêneas consolidadas | Oracle (múltiplos schemas) + SAP HANA + outras |
| Entidades na camada TDV | Dezenas (com regras de negócio acumuladas) |
| Áreas consumidoras | Múltiplas (analytics, BI, relatórios regulatórios) |
| Tempo de operação do ambiente legado | Décadas |
| Profundidade técnica da sustentação | Missão crítica (SLA de disponibilidade) |

---

## Padrões Aplicados

- Migração incremental por domínio com validação side-by-side
- Mapeamento defensivo de dependências antes de alterações
- Monitoramento contínuo de qualidade com regras automatizadas

---

## Lições Aprendidas

**Em migrações de camada semântica, a complexidade está nas regras implícitas.** Toda view do TDV que "apenas filtra por status = ativo" esconde a pergunta: o que acontece com registros inativos? Os consumidores os veem? Eles precisam ser excluídos? Essas perguntas não têm resposta na documentação — têm resposta nos comportamentos implícitos que o usuário de BI construiu ao longo de anos. Descobrir essas regras antes de migrar é a maior parte do trabalho.

**Sustentação e modernização não são opostos — são complementares.** Manter a base Oracle estável não era um obstáculo à modernização — era o que tornava a modernização segura. Uma plataforma que falha durante a migração não tem crédito para migrar mais. A confiança construída pela sustentação de qualidade é o capital que financia as mudanças arquiteturais.

**SAP Datasphere tem uma curva de adoção.** A plataforma combina conceitos de virtualização (views sobre fontes externas), persistência (tabelas replicadas) e modelagem semântica (espaços analíticos) de uma forma que não é intuitiva para quem vem de TDV ou de um data warehouse tradicional. O onboarding de consumidores exigiu treinamento e documentação específicos para cada padrão de uso.

**Ecossistemas heterogêneos exigem neutralidade técnica.** Trabalhar com Oracle, SAP, Azure e ferramentas de qualidade ao mesmo tempo — sem preferência ideológica por nenhum — é uma competência em si. O melhor design de integração respeita os pontos fortes de cada componente ao invés de forçar tudo em uma única pilha.
