# Case Study 09 — Validação de Paridade em Migração KNIME para Databricks

**Setor:** Bens de consumo — multinacional, operação LATAM (5 países)
**Empregador:** Consultoria de dados
**Papel:** Engenheiro de validação independente — ferramental de comparação, diagnósticos e escalonamento de alinhamentos de negócio
**Período:** Agosto a setembro de 2026 (em andamento)

---

## Setor e Perfil do Projeto

O processo de enriquecimento e alocação de custos de estoque para a América Latina rodava em um workflow KNIME que consome extrações SAP e planilhas mestras mantidas à mão, produzindo um relatório consolidado de cinco países. O projeto migra o fluxo inteiro para notebooks PySpark em Databricks, com critério de aceite de paridade bug a bug: o Databricks precisa reproduzir o KNIME, inclusive nas decisões questionáveis, antes de qualquer melhoria.

O papel neste case não foi escrever os notebooks migrados, e sim assumir a frente de validação: comparar mês a mês a saída Databricks contra a saída dourada do KNIME, diagnosticar cada divergência até a causa e devolver ao time de migração um veredito acionável.

---

## Problema

**Técnico:** Como comparar uma planilha XLSX gerada pelo KNIME com um CSV gerado pelo Spark de forma que diferenças de encoding, escape de aspas, representação numérica e ordem de linhas não sejam confundidas com diferenças de lógica de negócio?

**Operacional:** O fluxo tem 17 etapas sequenciais, cada uma com escopo de país diferente (LATAM completo, LATAM sem Uruguai, só Brasil, Brasil e México), e o ciclo de validação envolve três pessoas em dois lados: pré-validação local, execução no ambiente do cliente, saída em pasta compartilhada, validação independente.

**Escala do desafio:**
- 17 módulos, cerca de 19 notebooks de produção, até 6 versões arquivadas por módulo
- Tabelas de 225 mil a 830 mil linhas por período, com 32 a 39 colunas
- Dois ciclos de fechamento validados por país para as etapas de alocação

---

## Stack Aplicada

| Componente | Tecnologia |
|------------|-----------|
| Origem da migração | KNIME (workflow `.knwf`) |
| Destino | Databricks, notebooks PySpark com schemas explícitos e window functions |
| Entradas e saídas | XLSX e CSV, com toda saída tipada como string para paridade com o KNIME |
| Validador | Python e pandas, CLI própria |
| Sincronização | Script próprio contra a API de compartilhamento anônimo do OneDrive, incremental, sem apagar nada local |
| Relatórios | Relatório padrão do time acrescido de seção de diagnóstico, mais dashboards HTML por módulo e período |

---

## O Validador

A ferramenta central é um comparador XLSX versus CSV orientado a causa raiz. Os detalhes de engenharia que fazem diferença:

**Normalização de formato antes de comparar.** Delimitador detectado a partir do cabeçalho (pipe, ponto e vírgula, vírgula, tabulação). Cascata de encodings de UTF-8 com BOM até Latin-1. O escape de aspas internas do Spark (barra invertida) é convertido para o padrão CSV de aspas duplas sem tocar em barras literais dos dados, porque códigos de produto contêm barras.

**Discriminação de artefatos de encoding.** O validador detecta mojibake pelos marcadores típicos de texto UTF-8 lido como cp1252 e tenta reverter. O relatório informa separadamente quantas divergências desaparecem ao reverter o mojibake e quantas são idênticas ignorando acentos. Isso separa artefato de codificação de diferença real de lógica, que é a única que interessa ao time de migração.

**Canonicalização numérica.** Uma função de forma canônica garante que `1E-4` e `0.0001` comparem como iguais, exatamente, sem tolerância arbitrária.

**Contagem de chaves duplicadas e órfãos por lado.** Cada lado é auditado por chave: duplicatas, presença só no XLSX, presença só no CSV.

O resultado é um relatório com a seção de diagnóstico que aponta para onde olhar: coluna com gap, quantidade de linhas divergentes, órfãos e a hipótese de causa.

---

## Resultados Quantificados

| Métrica | Valor |
|---------|-------|
| Relatórios de validação gerados no projeto | 133 |
| Relatórios com seção de diagnóstico produzida pelo validador | 118 (89%) |
| Dashboards HTML de diagnóstico | 39 |
| Execuções nos dois ciclos de fechamento em validação | 117 de 133, por país |
| Módulos aprovados sem divergência | Materiais compartilhados em dois períodos: 0 de 38 colunas com gap, 0 linhas divergentes, 0 órfãos |
| Divergências reais encontradas | 122 execuções reprovadas, entre elas 57 mil linhas divergentes em 34 de 39 colunas em uma etapa de alocação e 14 mil linhas em uma única coluna em outra |
| Volumes validados | Hierarquia de produto de 225 mil a 229 mil linhas; materiais de 809 mil a 829 mil linhas |

Os números de reprovação não indicam falha da validação. Indicam que a frente está cumprindo o papel: cada reprovação traz a coluna, a contagem e a causa provável, e o time de migração corrige por iteração.

---

## Decisões Notáveis

**Paridade bug a bug como critério, melhoria como fase 2.** Planilhas de entrada feitas à mão exigem tratamento que beira "raspar Excel dentro do Python". A tentação de corrigir a lógica durante a migração foi explicitamente adiada para uma segunda versão, ainda não alinhada com o cliente. Migrar e melhorar ao mesmo tempo torna impossível saber se uma divergência é bug ou melhoria.

**Materializar cada intermediário.** Toda leitura de Excel ou CSV é gravada em CSV temporário e relida antes de seguir. Custa I/O, mas elimina a classe inteira de diferenças por inferência de tipo entre leitores.

**Sincronização sem cliente instalado.** O script de sincronização usa o mesmo fluxo de token anônimo que o navegador usa para pastas compartilhadas, com critério incremental por tamanho, filtro por subpasta e modo de simulação. Garantia de não apagar nada localmente.

---

## Lições Aprendidas

**Separar encoding de lógica é metade do trabalho de validação.** Sem essa separação, um relatório com 10 mil divergências vira ruído e o time de migração para de confiar no validador.

**Igualdade numérica é uma decisão, não um default.** Comparar texto de números é errado; comparar float com tolerância esconde erros de arredondamento de negócio. A forma canônica exata é o meio-termo que aguenta auditoria.

**Validação independente precisa de ferramental próprio.** Reusar o notebook de quem migrou para validar a migração reproduz os mesmos vieses. O validador em pandas, fora do Spark, é a segunda opinião.
