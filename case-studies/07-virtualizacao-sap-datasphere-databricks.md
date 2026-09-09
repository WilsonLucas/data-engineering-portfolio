# Case Study 07 — Virtualização de SAP Datasphere em Azure Databricks com CI/CD em Três Ambientes

**Setor:** Corporativo de grande porte — energia (óleo e gás)
**Empregador:** Dataside (consultoria)
**Papel:** Engenheiro de Dados — ownership end-to-end da demanda
**Período:** Maio a setembro de 2026, em produção desde setembro

---

## Setor e Perfil do Projeto

Plataforma corporativa de dados de uma empresa integrada de energia, organizada em dezenas de times de dados com catálogos próprios no Unity Catalog, esteira central de CI/CD em Azure DevOps e portal de governança onde cada Data Product precisa ser registrado. O time de dados de refino consome dados de manutenção industrial (ordens de manutenção e confirmações de operação de SAP PM) curados em SAP Datasphere.

A demanda tinha três entregas: virtualizar uma tabela curada do Datasphere em Databricks sem copiar dados, construir a arquitetura que expõe apenas as linhas do segmento de refino, e registrar o Data Product no portal de governança.

Este case complementa o [Case 04](04-migracao-camada-semantica.md), que documenta a modernização da camada semântica no mesmo ambiente.

---

## Problema

**Técnico:** Como expor uma tabela do SAP Datasphere em Databricks, filtrada por um segmento de negócio cuja lista de centros é mantida em planilha por área usuária, com promoção automatizada por três ambientes, sem copiar dados e dentro das regras de uma plataforma que nunca havia virtualizado SAP naquele time?

**Operacional:** O workspace aplica lista de IPs permitidos e a máquina do engenheiro estava bloqueada. O ciclo inicial de trabalho era editar no SQL Editor, tirar screenshot, enviar pelo celular. Credenciais de conexão dependiam de chamado em ServiceNow, permissões de criação de volume não existiam em produção e o ambiente de homologação não tem Datasphere correspondente.

---

## Stack Aplicada

| Componente | Tecnologia |
|------------|-----------|
| Plataforma | Azure Databricks (workspaces dev, hmg e prd), compute Serverless |
| Governança | Unity Catalog, 78 catálogos no metastore, naming por time e ambiente |
| Virtualização | Lakehouse Federation via `CREATE CONNECTION ... TYPE JDBC` e `remote_query()` contra SAP Datasphere |
| Origem | SAP Datasphere (espaços de ingestão, curadoria e consumo), SAP BW on-premise via Cloud Connector |
| Tabela de referência | Delta Lake, carga full a partir de planilha ingerida por trilho oficial de SharePoint |
| Segredos | Databricks Secrets com backend Azure Key Vault por ambiente |
| Orquestração | Databricks Job multi-task (3 tasks), executado como Managed Identity do ambiente |
| CI/CD | Azure DevOps Repos e Pipelines: CI com pacote versionado e CD encadeado DeployToDev, DeployToHmg, DeployToPrd |
| Linguagens | PySpark, Python, SQL |

---

## Arquitetura

```
SAP BW on-premise
      │  Cloud Connector
      ▼
SAP Datasphere PRD
  espaço de ingestão -> espaço de curadoria -> espaço do time de refino
      │  JDBC (connection por ambiente), sem cópia de dados
      ▼
<catalogo_ambiente>.biin.vw_ordem_manutencao            view federada, sem filtro
<catalogo_ambiente>.biin.tb_segmento_unidade_centro     Delta, full overwrite a partir de planilha
      ▲
      │  SharePoint -> ingestor oficial da plataforma -> Volume bruto
      ▼
<catalogo_ambiente>.biin_consumo.vw_ordem_manutencao_ref   semi-join, SEGMENTO = 'REFINO'
```

Um único job com três tasks encadeadas (tabela de referência, view federada, view de consumo) roda em Serverless e notifica falhas. Tempo de execução em regime: 1 a 2 minutos, até 7 minutos após inatividade do ambiente.

---

## Decisões Técnicas-Chave

### 1. Virtualização sem cópia, não materialização

O desenho inicial aprovado previa materialização em Delta com MERGE agendado, escolhido sobre Materialized Views nativas e sobre DLT com justificativa escrita. Ao realinhar a natureza da demanda com o líder técnico, a materialização foi arquivada como evolução futura de performance e a entrega passou a ser uma view federada. Matar um workstream inteiro já iniciado, com registro do motivo, evitou entregar algo diferente do que foi pedido.

### 2. Ponte Git em vez de screenshots

Com o IP bloqueado, o fluxo de trabalho foi substituído por uma ponte validada: editar em clone local, subir feature branch no Azure DevOps, fazer pull na pasta Git do workspace. Screenshots ficaram restritos a telas exclusivas de interface.

### 3. Contingência de sprint com reversão agendada

Sem permissão de criação de volume em produção, a sprint foi fechada com uma versão contendo os 13 centros de refino fixos em código, em branch dedicada e com o semi-join comentado para reversão. Assim que o trilho oficial de ingestão de SharePoint ficou disponível, a versão seguinte restaurou a tabela de referência dinâmica e eliminou o bloqueio de permissão.

### 4. Usuário do Datasphere derivado de constante

A execução em produção falhou por um segredo inexistente: o nome do usuário estava em uma chave de convenção local, fora do padrão da plataforma, que define apenas a chave de senha. A correção removeu a dependência derivando o usuário técnico a partir do espaço e do time, mantendo só a senha no cofre.

### 5. Rota Oracle descartada e documentada

Uma tentativa de extrair a tabela de referência de um Oracle on-premise via JDBC falhou e virou registro de decisão: o driver precisa estar em Volume, não em Maven; o workspace tem saída para internet mas não alcança a rede on-premise; o compute é apenas Serverless, sem cluster clássico em VNet. Três lições duráveis para o time.

---

## Incidentes e Segurança

**Vazamento de credencial contido em minutos.** Um branch de snapshot do workspace continha um notebook antigo com segredo de aplicação de SharePoint em texto claro e foi enviado ao Azure DevOps. O branch remoto foi apagado imediatamente, a rotação foi escalada no dia seguinte e o episódio virou regra escrita para o time.

**Falha em produção isolada como problema interno do SAP.** No primeiro run de produção, a autenticação Databricks para Datasphere funcionou, mas a view falhou dentro do Datasphere por queda de comunicação com o Cloud Connector do BW on-premise. O problema foi reproduzido no Data Preview do próprio Datasphere e rastreado pela linhagem em três espaços, provando que era 100% interno ao SAP. A correção foi aplicada no mesmo dia e o go-live seguiu.

**"Produção rejeita artefatos snapshot".** Builds de produção precisam usar versão Patch, Minor ou Major da esteira. Lição registrada após a primeira tentativa de promoção.

---

## Trabalho Complementar: View Curada no Datasphere

Antes da virtualização, uma segunda demanda no mesmo ambiente foi criar uma view curada de confirmações de operação em SAP Datasphere, com cerca de 50 campos brutos de origem renomeados para o padrão de nomenclatura do Data Product.

O método usado foi um "registro Rosetta": uma chave de negócio presente nos dois lados, cujos valores desambiguam colunas que os nomes sozinhos não resolvem (cinco campos de data contra cinco campos de data, três códigos diferentes de centro de trabalho, pares quantidade e unidade). Combinado com matching semântico por prefixo, isso produziu uma matriz de 52 linhas mapeadas: 34 correspondências, 2 vazios e 16 descartes.

Insistir no DDL real em vez de screenshots capturou 4 erros de transcrição antes da entrega. Campos sem correspondência ficaram comentados na posição original, transformando o artefato em pedido formal de inclusão ao time upstream.

---

## Resultados Quantificados

| Métrica | Valor |
|---------|-------|
| Notebooks de produção entregues | 3, mais 1 de parâmetros compartilhados |
| Ambientes com deploy verde | 3 (dev, hmg, prd) |
| PRs mesclados | 3 |
| Recorte de refino validado | 5.605 de 5.625 linhas em dev, com a diferença de semântica entre grupo de planejamento e área operacional provada como regra de negócio, não bug |
| Tabela de referência | 129 linhas, 13 centros de refino |
| Tempo de execução do job em regime | 1 a 2 minutos |
| Conexões JDBC existentes no metastore | Cerca de 130, nenhuma do time, confirmando primeira virtualização SAP da área |
| Chamados de credencial conduzidos | 1, aberto e resolvido em 24 horas |
| View curada no Datasphere | 34 colunas no padrão do Data Product, 4 erros de transcrição evitados |

---

## Entregáveis Além do Código

- Proposta de padrão de notebooks para o time: layout de diretórios, nomenclatura, ordem fixa de células, regra "notebook de produção é limpo", idempotência, segredos e matriz de escolha de compute.
- Runbook de atualização da tabela de referência: arquitetura, pré-requisitos, regras de layout da planilha, procedimento em 4 passos, tabela de 6 sintomas e links diretos para os jobs.
- Cinco documentos de referência transcritos do ambiente (repositório, convenções, virtualização SAP, consumo e ingestão de SharePoint).

---

## Lições Aprendidas

**Alinhar a natureza da entrega antes de otimizar.** Um workstream de materialização bem justificado ainda é o workstream errado se a demanda pede virtualização. A pergunta "isso é cópia ou é view?" deveria ter sido a primeira.

**Contingência precisa de data de reversão.** Centros fixos em código são aceitáveis por uma sprint, com branch dedicada, semi-join comentado e versão seguinte já planejada. Sem isso, a contingência vira arquitetura.

**Documentar o caminho que não deu certo.** A rota Oracle falhou, mas as três lições sobre driver, rede e compute economizam dias para o próximo engenheiro do time.
