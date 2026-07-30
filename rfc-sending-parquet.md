RFC — Ingestão assíncrona de Parquet via API Gateway, Lambda e S3 com processamento em janelas no Glue

Status: Proposta
Versão: 1.0
Intenção: definir uma solução simples, stateless, resiliente e de baixo acoplamento para ingestão de arquivos Parquet vindos de ECS em outra conta AWS, com processamento posterior em janela pelo Glue.

1. Contexto

Existe uma aplicação em ECS, em outra conta AWS, que produz DataFrames e precisa publicar esses dados em um Data Lake sem depender de IAM cross-account, presigned URL, infra adicional na conta de origem ou coordenação síncrona entre ingestão e processamento.

A solução precisa ser fire-and-forget: o produtor envia o arquivo, recebe confirmação rápida e segue em frente. O processamento pesado, a consolidação de small files, a catalogação e o data quality acontecem depois, em uma janela assíncrona.

Há ainda uma restrição objetiva de transporte: o payload do API Gateway é limitado a 10 MB, então qualquer DataFrame que exceda esse tamanho precisa ser dividido pelo produtor em múltiplos arquivos Parquet completos antes do envio. 

2. Objetivo

Definir uma arquitetura em que:

* o ECS envia arquivos Parquet completos;
* a API aceita cada arquivo como uma unidade independente de ingestão;
* a Lambda apenas valida, padroniza e grava no S3;
* o Glue roda de forma assíncrona e periódica;
* o Glue consolida small files, materializa a camada curada, atualiza o Glue Catalog e executa regras de qualidade;
* a solução permaneça stateless na borda de ingestão.

3. Não objetivos

Esta RFC não propõe:

* sessão de upload;
* manifesto de finalização;
* controle de partes de um mesmo Parquet na Lambda;
* DynamoDB para controlar estado de upload;
* presigned URL;
* processamento síncrono;
* catálogo ou data quality em tempo real.

4. Decisão

A arquitetura adotada é:

ECS (outra conta)
    -> API Gateway
    -> Lambda stateless
    -> S3 Landing Zone
    -> Glue agendado
    -> Glue Catalog
    -> Glue Data Quality
    -> Camada Curada

A decisão central é tratar a integração como ingestão de arquivos completos e não como remontagem de objetos. Cada requisição carrega um Parquet válido. O cliente não controla bucket, prefixo ou key física; esses detalhes são resolvidos pela Lambda.

Para o conteúdo enviado, o produtor deve dividir previamente DataFrames maiores em múltiplos arquivos Parquet completos. Não existe envio de “chunks” de bytes de um único Parquet. O que entra na plataforma é uma coleção de arquivos válidos, cada um autocontido.

A API responde com 202 Accepted assim que o arquivo é validado e persistido no S3. O restante do fluxo é assíncrono.

5. Arquitetura proposta

Account origem (ECS)
  |
  |  HTTPS multipart/form-data
  v
API Gateway
  |
  v
Lambda stateless
  |
  |  grava objeto imutável
  v
S3 Landing Zone
  |
  |  janela de processamento
  v
Glue job / workflow agendado
  |
  +--> consolidação de small files
  +--> materialização da camada curada
  +--> atualização do Glue Catalog
  +--> execução do Glue Data Quality
  |
  v
Consumo analítico

6. Responsabilidades por componente

6.1 ECS produtor

Responsável apenas por:

* serializar o DataFrame em Parquet;
* dividir o conjunto de dados em múltiplos arquivos, quando necessário;
* enviar cada arquivo para a API;
* lidar com retry de forma idempotente no cliente.

Não é responsabilidade do ECS conhecer bucket, prefixo, particionamento físico ou qualquer detalhe da camada de governança.

6.2 API Gateway

Responsável por:

* autenticação e autorização;
* roteamento;
* throttling e proteção de borda;
* limite de payload.

A API não contém lógica de negócio. O limite de payload de 10 MB é um dado importante de projeto e define o contrato de upload. 

6.3 Lambda

Responsável por:

* validar headers, metadados e conteúdo;
* validar que o payload representa um Parquet completo;
* montar a key S3 de forma padronizada;
* gravar o objeto no landing;
* publicar métricas e logs;
* responder rapidamente com 202.

A Lambda permanece stateless. Ela não armazena estado de sessão, não controla finalização de upload e não coordena lotes.

6.4 S3 Landing Zone

Responsável por:

* armazenar os arquivos recebidos como objetos imutáveis;
* ser a fonte de entrada bruta do pipeline;
* permitir reprocessamento futuro sem dependência da API.

6.5 Glue agendado

Responsável por:

* ler os objetos do landing conforme janela de execução;
* consolidar small files;
* escrever a camada curada;
* atualizar tabelas no Glue Catalog;
* executar as regras de qualidade.

A AWS Glue Data Quality é um serviço serverless baseado em DQDL, e tarefas agendadas podem ser criadas com integração ao EventBridge; além disso, resultados de execução podem emitir eventos para alerta e monitoramento. 

7. Contrato da API

Endpoint principal

POST /v1/datasets/files

Content-Type

multipart/form-data

Partes esperadas

* metadata — JSON com os atributos de negócio;
* file — arquivo Parquet completo.

Metadados mínimos

* dataset: nome lógico do dataset;
* partition: informação de particionamento de negócio;
* sourceFileName: nome lógico do arquivo de origem;
* schemaVersion: versão do contrato, se aplicável;
* correlationId: identificador opcional de rastreio;
* sequence: ordem lógica do arquivo dentro do lote, quando houver.

Regras de validação

* o arquivo deve ser um Parquet válido;
* o payload deve respeitar o limite do API Gateway;
* o cliente não pode informar bucket nem path físico;
* o particionamento físico é resolvido internamente;
* campos obrigatórios ausentes resultam em 400.

Respostas

* 202 Accepted — arquivo recebido e persistido;
* 400 Bad Request — contrato inválido;
* 401/403 — autenticação/autorização;
* 413 Payload Too Large — requisição acima do limite;
* 500/503 — falha transitória ou indisponibilidade.

8. Organização no S3

A key física é construída pela plataforma, por exemplo:

landing/
  dataset=<dataset>/
  partition_date=<yyyy-mm-dd>/
  ingestion_date=<yyyy-mm-dd>/
  <generated_name>.parquet

Princípios da estrutura:

* objetos imutáveis;
* key previsível para operação;
* separação entre particionamento de negócio e ingestão;
* isolamento por dataset;
* ausência de overwrite acidental.

O cliente não controla o prefixo final. Ele informa apenas o contexto lógico do dado; a plataforma decide a forma física.

9. Estratégia de processamento no Glue

O Glue executa em janela, por agenda. A cadência pode ser horária ou diária, conforme o volume e a criticidade do dado.

Em cada execução, o job deve:

1. identificar os arquivos elegíveis no landing;
2. ler todos os Parquet daquela janela ou partição;
3. consolidar small files em arquivos maiores;
4. escrever a camada curada em formato otimizado;
5. atualizar o Glue Catalog;
6. executar as regras de Glue Data Quality.

Como não existe finalização explícita, o sistema assume consistência eventual: arquivos enviados após o início de uma execução são processados na próxima janela. Isso mantém a borda de ingestão simples e preserva a robustez operacional.

10. Resiliência

A solução é resiliente por construção:

* a ingestão é desacoplada do processamento;
* a API responde rápido e não espera Glue;
* cada arquivo é independente;
* a Lambda não mantém sessão;
* a camada landing é imutável;
* o Glue pode ser reexecutado sem depender de estado de upload.

Falhas transitórias na API ou na Lambda são tratadas com retry no produtor. Falhas de processamento no Glue não interrompem a ingestão; apenas afetam a janela corrente.

11. Idempotência e reprocessamento

A idempotência deve ser tratada de forma simples, sem estado de sessão:

* o produtor pode usar correlationId por arquivo;
* o nome físico no S3 deve ser determinístico o suficiente para evitar colisão indevida;
* reenvios de um mesmo arquivo devem ser tratados como operação segura;
* o pipeline de Glue deve tolerar reexecução sobre a mesma partição.

Quando o reenvio ocorrer, a decisão operacional é privilegiar previsibilidade sobre “inteligência” na borda. A plataforma deve aceitar que o landing é uma área de entrada e não um estado final.

12. Segurança e governança

A solução preserva governança porque:

* não exige IAM na conta do ECS;
* não expõe bucket nem path físico ao produtor;
* centraliza validação e persistência na conta da plataforma;
* permite aplicar autenticação, autorização e rate limiting na borda.

A Lambda deve operar com permissão mínima, restrita ao bucket e aos prefixes necessários.

13. Observabilidade

Deve haver monitoramento em três níveis:

* API Gateway: taxa de 4xx/5xx, latência, throttling;
* Lambda: falhas, duração, número de requisições e objetos gravados;
* Glue: sucesso/fracasso do job, tempo de execução, métricas de data quality.

Quando as regras de qualidade forem executadas, os eventos e resultados podem ser integrados ao EventBridge para alerta e automação. 

14. Decisões explícitas desta RFC

1. A ingestão é assíncrona.
2. A API retorna 202 Accepted.
3. Cada requisição carrega um Parquet completo.
4. Não existe manifesto de finalização.
5. Não existe sessão de upload.
6. Não existe DynamoDB para estado de ingestão.
7. O Glue processa em janela.
8. O Glue consolida small files.
9. O Glue atualiza catálogo e qualidade.
10. O cliente não controla bucket nem path físico.

15. Resultado esperado

Com esta abordagem, a plataforma ganha:

* baixa latência na borda;
* menor acoplamento;
* simplicidade operacional;
* boa escalabilidade;
* governança centralizada;
* caminho claro para evolução futura sem refatorar o contrato básico de ingestão.

16. Conclusão

A solução recomendada é uma landing zone assíncrona baseada em arquivo Parquet completo, com API Gateway e Lambda apenas como porta de entrada e com o Glue assumindo a consolidação, a catalogação e o data quality em janela. O modelo é simples, stateless, resiliente e compatível com o limite de 10 MB do API Gateway, desde que o produtor faça a divisão prévia de conjuntos maiores em múltiplos Parquet válidos. 