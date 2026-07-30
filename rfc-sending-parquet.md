RFC — Ingestão assíncrona de Parquet via API Gateway, Lambda e S3 com processamento em janelas no Glue (Write-and-Audit)

Status: Proposta
Versão: 5.2
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

Restrições de transporte da solução

A arquitetura é limitada pelas capacidades da camada de exposição da AWS:

* o Amazon API Gateway suporta payloads de até 10 MB por requisição HTTP;
* a invocação síncrona da AWS Lambda suporta payloads de até 6 MB;
* durante a integração entre API Gateway e Lambda, o conteúdo binário é entregue codificado em Base64, aumentando o tamanho efetivo do payload transmitido.

Como consequência, o tamanho máximo do arquivo Parquet produzido pela aplicação deve ser significativamente inferior ao limite nominal da Lambda. O produtor deve considerar esse overhead de codificação ao definir a estratégia de fragmentação, gerando arquivos suficientemente pequenos para garantir que, após a serialização do multipart/form-data e da codificação Base64 realizada pela integração, o payload permaneça dentro dos limites suportados.

Por esse motivo, a plataforma adota como diretriz operacional que os arquivos gerados pelo produtor possuam tamanho inferior ao limite máximo da Lambda, preservando margem de segurança para o overhead de transporte.

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
* a resposta deve ser rápida e não depender de processamento analítico;
* durante a integração entre API Gateway e Lambda, o conteúdo binário será recebido pela Lambda codificado em Base64, devendo esse overhead ser considerado pelo produtor no dimensionamento do tamanho máximo de cada arquivo enviado.

Diretriz de fragmentação

Embora o API Gateway aceite payloads de até 10 MB, a integração síncrona com a Lambda é limitada a 6 MB e ainda adiciona overhead devido à codificação Base64 do conteúdo binário. Assim, o produtor deve fragmentar o DataFrame em arquivos Parquet menores que esse limite teórico, mantendo margem suficiente para acomodar:

* o envelope multipart/form-data;
* os metadados da requisição;
* a expansão causada pela codificação Base64.

Essa margem operacional reduz o risco de rejeições por extrapolação de payload e torna o processo de ingestão mais previsível e resiliente.

⸻

13. Decisões explícitas desta RFC

* A Lambda permanece completamente stateless.
* Não existe sessão de upload.
* Não existe manifesto de finalização.
* Não existe controle de partes de um Parquet na API.
* O produtor envia arquivos Parquet completos.
* O cliente não controla bucket, prefixo ou key física.
* O Glue processa em janela e não em tempo real.
* A camada SOR é disponibilizada antes da execução do Data Quality, seguindo o padrão Write-and-Audit.
* O padrão adotado é Write-and-Audit.
* O interactionId é o identificador corporativo preferencial.
* A organização física no S3 utiliza uma única coluna de particionamento denominada ano_mes_dia, contendo o valor concatenado no formato yyyymmdd.
* O produtor deve considerar os limites combinados do API Gateway (10 MB), da invocação síncrona da Lambda (6 MB) e o overhead da codificação Base64 ao definir o tamanho máximo dos arquivos Parquet enviados.
* Small files são consolidados pelo Glue.
* O dado permanece disponível mesmo em caso de falha de qualidade.

Observação: Todas as demais seções da RFC permanecem inalteradas em relação à versão 5.1. Esta versão 5.2 introduz apenas o detalhamento dos limites de transporte entre API Gateway e Lambda e a diretriz de dimensionamento dos arquivos produzidos.