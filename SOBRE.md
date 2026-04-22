# Wilson Lucas — Bio Expandida

## Trajetória

Comecei como estagiário de BI em 2015, conectando bancos de dados relacionais a ferramentas de visualização com Pentaho e MySQL. Em dois anos, já entendia que o problema não era o relatório — era a confiança nos dados que o alimentavam. Essa percepção guiou toda a trajetória seguinte.

A passagem pela Global Web foi o período de formação técnica densa: administração de banco de dados Oracle e SQL Server em ambientes de missão crítica no setor público federal, pipelines de integração com SSIS e Informatica PowerCenter, e o primeiro contato com a realidade de dados que não podem errar — aqueles publicados sob a Lei de Acesso à Informação, onde qualquer inconsistência vira processo. Aprendi que dados de qualidade não são um detalhe de implementação — são o produto.

Na VERT, o salto foi arquitetural. Azure Databricks substituiu o ETL tradicional, PySpark substituiu o T-SQL de transformação, e a escala dos projetos — CRM nacional em instituição financeira pública de grande porte, inteligência fiscal estadual — exigiu uma nova forma de pensar pipelines: menos carga batch pontual, mais ingestão contínua, idempotente e auditável.

A Compass.UOL trouxe um desafio diferente: sustentação de missão crítica em um ambiente Oracle corporativo consolidado ao longo de anos, ao mesmo tempo em que se conduz a modernização da camada semântica para SAP Datasphere. É o tipo de trabalho onde a habilidade de não quebrar o que funciona é tão valiosa quanto a habilidade de construir o que é novo.

Na Dataside, a atuação em modelo de consultoria itinerante é o que melhor representa onde estou hoje: cada projeto começa do zero em termos de contexto de negócio, mas o método é consistente — diagnóstico rigoroso do ambiente, arquitetura baseada em padrões comprovados, implementação auditada, transferência de conhecimento.

---

## Filosofia de Trabalho

**Documentação é produto, não afterthought.** Cada pipeline que entrego tem sua lógica documentada — não para cumprir processo, mas porque sei que em seis meses serei eu mesmo depurando algo que não lembro mais. Controller tables, comentários de bloco em notebooks, logs de bloqueios técnicos resolvidos — tudo isso é parte do entregável.

**Homologação antes de go-live, sempre.** Não existe "parece certo". Existe "validado linha-a-linha contra a fonte de verdade". Essa postura pode tornar o ciclo mais longo, mas elimina retrabalho corretivo em produção, que é ordens de grandeza mais caro. A metodologia de zero amostragem que desenvolvi ao longo dos projetos — universo idêntico, diff por coluna com classificação de tipo de divergência, causa raiz documentada — nasceu dessa convicção.

**Automação como multiplicador.** Tarefas repetitivas viram script. Scripts viram CLI. CLI vira ferramenta de equipe. O tempo gasto criando uma boa abstração retorna com juros quando o time consegue fazer em segundos o que antes levava minutos de navegação em portais.

**Padrões reduzem carga cognitiva.** Um template de notebook Silver padronizado, um controller table bem definido, uma convenção de nomenclatura consistente — tudo isso significa que quando você abre qualquer arquivo do projeto às 11h da noite para depurar um problema, o contexto está onde você espera. Padronização não é burocracia; é respeito pelo futuro de quem vai manter o código.

---

## Áreas de Domínio

**Arquitetura Medallion e Data Lakehouse.** É onde passo a maior parte do tempo projetando. A transição de Bronze (fidelidade total à fonte) para Silver (regras de negócio aplicadas, schema estável) para Gold (modelo dimensional consumível por BI) não é automática — cada camada tem seus próprios contratos de qualidade. Aprendi a respeitar esses contratos sem atalhos.

**Migração de legado para plataformas modernas.** Procedures T-SQL escritas há 10 anos não foram escritas para ser migradas — foram escritas para funcionar. Migrar com paridade semântica verificável é um trabalho de arqueologia técnica tanto quanto de engenharia. Cada divergência entre o legado e a nova plataforma conta uma história sobre assunção implícita de negócio que nunca foi documentada.

**People Analytics e dados de RH.** Datasets de folha de pagamento têm características únicas: encodings não-padrão de sistemas brasileiros legados (ISO-8859-1, decimal com vírgula), verbas com semântica de negócio muito específica (interjornada, consignado, benefícios tributáveis vs. isentos), e exigência de auditabilidade por competência. O trabalho sobre 40M+ linhas de histórico de folha reforçou minha capacidade de lidar com dados de RH em escala.

**Governança e Unity Catalog.** A catalogação de 433+ tabelas com controle de acesso por ambiente (dev vs. prod) em Unity Catalog mudou a maneira como penso sobre discovery de dados e lineage. Não é só uma feature do Databricks — é a diferença entre um data lake que vira data swamp e uma plataforma onde as pessoas confiam no que encontram.

---

## Trade-offs Técnicos Recorrentes

**Overwrite vs. MERGE:** O overwrite é simples, rápido e correto para a maioria dos casos — mas cria uma janela de inconsistência durante a execução. Em ambientes onde consumidores de BI rodam queries a qualquer momento, essa janela é inaceitável. A migração para MERGE atômico ACID com `WHEN NOT MATCHED BY SOURCE DELETE` elimina a janela, mas exige chaves de negócio estáveis e aumenta a complexidade da manutenção. Escolho MERGE quando a janela de inconsistência é um risco real e as chaves são confiáveis.

**Dynamic Partition Overwrite vs. MERGE para duplicatas legítimas:** Quando a fonte produz linhas 100% idênticas por design de negócio — não por erro — o MERGE tradicional com chave baseada em valores de negócio falha: não consegue distinguir as duplicatas para resolver o conflito. O Dynamic Partition Overwrite reescreve a partição inteira, o que é semanticamente correto para esses casos. O trade-off é que perde o ACID transacional linha-a-linha, então uso apenas quando a atomicidade de partição é suficiente.

**Cluster Spark vs. ferramentas leves (pyarrow, SQL sem Spark):** Para inspeção de metadados, schemas e amostras pequenas, subir um cluster Spark é desperdício. Quando possível, uso pyarrow para ler footers de Parquet, Azure CLI para listar paths ADLS, e Synapse Serverless SQL para queries pontuais. Guardo o cluster para o que só o cluster resolve: transformações em escala de dezenas de milhões de linhas.

---

## O que Procuro em Próximos Projetos

Projetos onde a complexidade técnica é real — não "conectar API A no dashboard B", mas arquitetar uma plataforma que vai sobreviver a múltiplos times e anos de evolução de negócio. Ambientes onde documentação e homologação rigorosa são valorizados, não vistos como overhead. Times que entendem que engenharia de dados é engenharia de software aplicada a dados — e não apenas operação de ferramenta.

---

*Para detalhes de contato e links profissionais, ver [README.md](./README.md).*
