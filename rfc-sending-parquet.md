RFC — Ingestão assíncrona de Parquet via API Gateway, Lambda e S3 com processamento em janelas no Glue (Write-and-Audit)

Status: Proposta
Versão: 5.1
Intenção: Definir uma solução resiliente, stateless e escalável para ingestão de arquivos Parquet gerados por uma aplicação em ECS em outra conta AWS, com processamento assíncrono em janelas no Glue, disponibilização dos dados na camada SOR e execução de Data Quality sem bloqueio da disponibilidade do dado.

⸻

1. Contexto

Existe uma aplicação em ECS que precisa publicar arquivos Parquet em um Data Lake localizado em outra conta AWS. O fluxo de ingestão deve ser simples, de baixa fricção operacional e com comportamento fire-and-forget do ponto de vista do produtor.

A solução precisa atender a estes princípios:

* não exigir IAM cross-account na conta do ECS;
* não exigir presigned URL;
* não depender de sessão de upload;
* não depender de manifesto de finalização;
* não bloquear a aplicação produtora com o processamento analítico;
* não transformar a Lambda em um componente stateful;
* manter o pipeline de dados resiliente a retries e reprocessamentos.

O upload é secundário em relação ao fluxo principal da aplicação produtora. Portanto, o produtor deve apenas gerar arquivos Parquet válidos, enviá-los para a plataforma e seguir com seu fluxo principal.

Quando o conteúdo produzido ultrapassar o limite de transporte suportado pela camada de exposição da API, o produtor deve fragmentar o conjunto em múltiplos arquivos Parquet completos antes do envio. Não há envio de fragmentos binários de um único Parquet. O que entra na plataforma são arquivos autocontidos, válidos e independentes.

⸻

2. Objetivo

Definir uma arquitetura em que:

* a aplicação produtora permaneça desacoplada do Data Lake;
* a ingestão aconteça via API simples e síncrona na borda, mas com processamento assíncrono;
* a Lambda permaneça stateless;
* cada arquivo enviado seja um Parquet completo;
* a camada de landing aceite múltiplos arquivos relacionados ao mesmo contexto lógico;
* o Glue consolide small files, materialize a camada SOR, atualize o catálogo e execute Data Quality;
* o Data Quality atue como observabilidade ativa e governança, sem indisponibilizar o dado;
* a arquitetura seja fácil de operar, evoluir e reprocessar.

⸻

3. Não objetivos

Esta RFC não propõe:

* sessão de upload;
* controle de partes de um Parquet na API;
* montagem de arquivo na Lambda;
* manifesto de finalização;
* workflow síncrono entre ingestão e processamento;
* DynamoDB para estado de upload;
* presigned URLs;
* quarentena automática do dado em caso de falha de qualidade;
* bloqueio da camada SOR até a aprovação do Data Quality.

⸻

4. Decisão

A arquitetura adotada é baseada em três camadas funcionais:

1. Ingestão: API Gateway + Lambda recebendo arquivos Parquet válidos.
2. Persistência: S3 como Landing Zone imutável.
3. Processamento: Glue em janela, com consolidação, catalogação e Data Quality não bloqueante.

O contrato de ingestão é propositalmente simples: cada requisição representa um arquivo Parquet completo. A API nunca recebe bucket, key física ou path final. Esses elementos são resolvidos internamente pela plataforma.

A Lambda tem responsabilidade exclusiva de validação estrutural, padronização da key e persistência do objeto no S3. Ela não mantém estado entre requisições.

O Glue roda de forma assíncrona e periódica, consumindo os arquivos já publicados, consolidando small files, materializando a camada SOR e executando as regras de Data Quality. Em caso de falha das regras, a plataforma gera alerta e incidente, mas não remove a disponibilidade do dado para consumo.

Este modelo é conhecido como Write-and-Audit: escreve primeiro, audita depois. A camada SOR é a fonte oficial de consumo, e o Data Quality atua como mecanismo de observabilidade e governança ativa, não como barreira de publicação.

⸻

5. Arquitetura proposta

Account origem (Produtor)                              Account destino (Plataforma de Dados)
  |                                                     |
ECS Task                                                |
  |                                                     |
  |  gera DataFrame e serializa em Parquet              |
  |                                                     |
  +--> envia arquivo Parquet completo                   |
        via HTTPS multipart/form-data                   |
        com fire-and-forget lógico                      |
  |                                                     |
  v                                                     |
API Gateway                                             |
  |                                                     |
  v                                                     |
Lambda stateless                                        |
  |                                                     |
  v                                                     |
S3 Landing Zone (arquivos imutáveis)                    |
  |                                                     |
  |  execução em janela / watermark                     |
  v                                                     |
Glue Job / Workflow agendado                            |
  |                                                     |
  +--> consolidação de small files                      |
  +--> materialização da camada SOR                    |
  +--> atualização do Glue Catalog                     |
  +--> Glue Data Quality não-bloqueante                |
  |                                                     |
  v                                                     |
Consumo analítico e governança operacional              |

Visão de fluxo

* o produtor gera um ou mais arquivos Parquet completos;
* cada arquivo é enviado individualmente;
* a API responde rapidamente com sucesso quando a persistência física foi concluída;
* o Glue processa os arquivos em janela, sem depender de sinal de finalização;
* os dados são disponibilizados na camada SOR;
* o Data Quality avalia o conteúdo e gera incidentes quando necessário.

⸻

6. Responsabilidades por componente

6.1 ECS produtor

Responsável por:

* serializar o DataFrame em Parquet;
* particionar logicamente o conjunto em múltiplos arquivos quando necessário;
* enviar cada arquivo individualmente;
* tratar retry de forma simples;
* não acoplar a vida da mensagem principal ao sucesso do upload do arquivo secundário, quando aplicável.

A aplicação produtora não conhece path físico, bucket, estrutura de S3 ou regras de catalogação.

6.2 API Gateway

Responsável por:

* autenticação e autorização;
* limitação de taxa;
* roteamento;
* aplicação dos limites de payload da camada de exposição;
* aceitar tráfego binário quando configurado para isso.

A API não executa regra de negócio nem processamento analítico.

6.3 Lambda

Responsável por:

* validar metadados obrigatórios;
* validar integridade estrutural da requisição;
* validar que o arquivo enviado é um Parquet válido;
* construir a key S3 conforme padrão da plataforma;
* persistir o objeto na Landing Zone;
* responder com sucesso assim que a gravação física for concluída.

A Lambda permanece stateless. Ela não guarda sessão, não mantém catálogo de upload, não reconstrói arquivos e não coordena finalização.

6.4 S3 Landing Zone

Responsável por:

* receber os arquivos na forma imutável;
* servir como ponto de entrada bruta;
* permitir reprocessamentos;
* preservar histórico operacional de ingestão.

A Landing Zone não é a camada final de consumo.

6.5 Glue agendado

Responsável por:

* ler os arquivos disponíveis na Landing Zone conforme janela de execução;
* ignorar arquivos muito recentes quando necessário, utilizando watermark operacional;
* consolidar small files;
* escrever a camada SOR;
* atualizar o Glue Catalog;
* executar Data Quality;
* publicar resultados e incidentes quando houver anomalia.

6.6 Camada SOR

Responsável por:

* ser o primeiro ponto oficial de disponibilização do dado para consumo;
* conter dados materializados e otimizados;
* ser idempotente do ponto de vista operacional;
* permanecer disponível mesmo quando Data Quality encontrar problemas.

6.7 Data Quality

Responsável por:

* validar contrato, completude e consistência;
* emitir métricas e alertas;
* gerar incidentes quando houver violação de regra;
* nunca bloquear a disponibilidade da camada SOR.

⸻

7. Contrato da API

Endpoint principal

POST /v1/datasets/files

Content-Type

multipart/form-data

Partes esperadas

* metadata: JSON com contexto lógico do envio;
* file: arquivo Parquet completo.

Metadados mínimos

* dataset: nome lógico do dataset;
* partition: dados de particionamento lógico;
* interactionId: identificador corporativo preferencial para rastreabilidade;
* sourceFileName: nome lógico do arquivo de origem;
* schemaVersion: versão do contrato quando aplicável;
* correlationId: identificador opcional de rastreio técnico;
* sequence: ordem lógica, quando o produtor enviar múltiplos arquivos relacionados.

Regras de validação

* o arquivo deve ser um Parquet válido;
* o payload deve respeitar os limites da camada de exposição da API;
* o cliente não pode informar bucket, prefixo físico ou key final;
* metadados obrigatórios ausentes resultam em rejeição da requisição;
* a resposta deve ser rápida e não depender de processamento analítico.

Respostas esperadas

* 202 Accepted ou 200 OK, conforme padrão operacional definido, quando a persistência física for concluída;
* 400 Bad Request para contrato inválido;
* 401/403 para autenticação ou autorização;
* 413 Payload Too Large quando o payload exceder o limite da camada exposta;
* 500/503 para falhas transitórias ou indisponibilidade.

⸻

8. Organização no S3

A organização física deve ser baseada em metadados de negócio e em contexto operacional, não em detalhes técnicos do produtor.

Estrutura recomendada

landing/
  dataset=<dataset>/
    ano_mes_dia=<yyyymmdd>/
      interaction_id=<interactionId>/
        <sourceFileName>.parquet

Princípios da estrutura

* particionamento por uma única coluna concatenada de data, no formato ano_mes_dia=<yyyymmdd>;
* isolamento por dataset;
* rastreabilidade por interactionId;
* objetos imutáveis;
* sem overwrite acidental;
* layout previsível para processamento posterior;
* alinhamento com leitura em batch e execução por janelas.

Sobre o Interaction ID

O interactionId deve ser a chave preferencial de rastreio corporativo, por ser mais reconhecido e útil operacionalmente no contexto da organização.

O sourceFileName pode refletir o mesmo identificador ou uma derivação dele, por exemplo:

* interaction_<interactionId>_part_001.parquet
* interaction_<interactionId>_part_002.parquet

Caso o processo de origem tenha também um identificador técnico de mensagem, ele pode ser usado como complemento, mas não como referência principal do domínio.

⸻

9. Estratégia de processamento no Glue

O Glue é executado em janelas agendadas. A frequência pode ser horária, diária ou outra adequada ao volume e à criticidade do dado.

Em cada execução, o pipeline deve:

1. listar os objetos elegíveis no landing;
2. aplicar watermark operacional quando necessário;
3. ler os Parquet disponíveis;
4. consolidar arquivos pequenos em arquivos maiores;
5. materializar a camada SOR;
6. atualizar o Glue Catalog;
7. executar Data Quality;
8. emitir métricas e eventos de governança.

Watermark operacional

O uso de watermark evita capturar arquivos que ainda estejam em fase de publicação ou propagação. O objetivo não é coordenar upload, e sim proteger a execução por janela de ler um conjunto ainda instável.

Consolidar small files

A consolidação é responsabilidade do Glue, não da API. Isso reduz overhead de leitura, melhora o desempenho analítico e evita degradação por excesso de arquivos pequenos.

Write-and-Audit

A regra estrutural da plataforma é:

* o dado entra;
* o dado é materializado na SOR;
* o Data Quality avalia depois;
* se houver falha, gera-se incidente;
* o dado não é retirado de consumo por padrão.

Esse modelo privilegia disponibilidade, rastreabilidade e operação contínua.

⸻

10. Resiliência

A solução é resiliente por construção, porque separa ingestão e processamento.

Propriedades desejadas

* a falha do Data Quality não bloqueia o consumo;
* a indisponibilidade temporária do Glue não impede a ingestão;
* retries do produtor são seguros;
* a Lambda não retém estado;
* os objetos no S3 são imutáveis;
* o pipeline pode ser reprocessado por janela.

Falhas esperadas e comportamento

Falha na API ou Lambda

A requisição é rejeitada e o produtor pode reprocessar o envio.

Falha no upload de um arquivo

O arquivo pode ser reenviado sem dependência de sessão.

Falha no Glue

A janela seguinte reprocessa o conjunto elegível.

Falha no Data Quality

A camada SOR permanece disponível e um incidente é aberto para análise.

⸻

11. Idempotência e reprocessamento

O sistema assume consistência eventual com objetos determinísticos.

Estratégia

* o nome do objeto deve ser estável e previsível;
* o particionamento físico deve permitir reprocessamento sem ambiguidade;
* o interactionId deve ajudar a correlacionar reenvios;
* a camada de processamento deve tolerar leituras repetidas e janelas sobrepostas.

Diretriz

A solução deve favorecer repetição segura em vez de mecanismos complexos de deduplicação em tempo real.

⸻

12. Observabilidade

A observabilidade deve existir em múltiplas camadas.

API Gateway

* quantidade de requisições;
* latência;
* erros;
* throttling;
* payload rejeitado.

Lambda

* número de arquivos persistidos;
* falhas de validação;
* falhas de persistência em S3;
* latência de gravação.

S3 / Landing

* objetos recebidos por partição;
* crescimento de small files;
* volume por dataset e por dia.

Glue

* duração por execução;
* volume de arquivos processados;
* volume consolidado;
* falhas por etapa.

Data Quality

* regras aprovadas;
* regras quebradas;
* tendência de anomalias;
* incidentes gerados.

Negócio

* volume por interactionId;
* rastreabilidade por dataset e partição;
* capacidade de correlacionar origem, ingestão e consumo.

⸻

13. Decisões explícitas desta RFC

* A Lambda permanece completamente stateless.
* Não existe sessão de upload.
* Não existe manifesto de finalização.
* Não existe controle de partes de um Parquet na API.
* O produtor envia arquivos Parquet completos.
* O cliente não controla bucket, prefixo ou key física.
* O Glue processa em janela e não em tempo real.
* A camada SOR é disponibilizada antes do Data Quality bloquear qualquer coisa, porque o Data Quality não bloqueia.
* O padrão adotado é Write-and-Audit.
* O interactionId é o identificador corporativo preferencial.
* A organização física no S3 usa ano_mes_dia como uma única coluna concatenada no formato yyyymmdd.
* Small files são consolidado pelo Glue.
* O dado permanece disponível mesmo em caso de falha de qualidade.

⸻

14. Resultado esperado

A solução resultante entrega:

* ingestão simples e rápida;
* acoplamento mínimo entre produtores e plataforma;
* persistência confiável no landing;
* arquitetura amigável a retries;
* consolidação de small files no ponto certo da cadeia;
* disponibilidade da camada SOR sem bloqueio;
* data quality com postura de governança ativa, não de indisponibilização;
* rastreabilidade por interactionId;
* organização física por ano_mes_dia;
* uma base evolutiva para crescer sem refatorar o contrato principal.

⸻

15. Conclusão

Esta arquitetura trata ingestão e processamento como etapas distintas, com boundaries claros. A API e a Lambda recebem apenas o necessário para persistir arquivos Parquet válidos com baixo acoplamento; o Glue assume a responsabilidade de consolidar, catalogar e auditar os dados em janelas; e a plataforma aplica Data Quality como mecanismo de observabilidade e governança, sem comprometer a disponibilidade da camada SOR.

O resultado é uma solução simples na borda, forte na operação e evolutiva na plataforma.