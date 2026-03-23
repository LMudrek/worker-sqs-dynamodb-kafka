#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="dotnet-sqs-dynamo-kafka"
AWS_REGION="us-east-1"
LOCALSTACK_ENDPOINT="http://localhost:4566"
QUEUE_NAME="my-queue"
TABLE_NAME="conversation-table"
KAFKA_TOPIC="conversations"

echo "===> Subindo infraestrutura (Kafka + LocalStack)"
docker compose up -d

echo "===> Aguardando LocalStack ficar pronto"
until curl -s ${LOCALSTACK_ENDPOINT}/_localstack/health | grep -q 'sqs'; do
  sleep 2
done

echo "===> Criando SQS"
aws --endpoint-url=${LOCALSTACK_ENDPOINT} sqs create-queue \
  --queue-name ${QUEUE_NAME} \
  --region ${AWS_REGION} || true

QUEUE_URL="${LOCALSTACK_ENDPOINT}/000000000000/${QUEUE_NAME}"

echo "===> Criando DynamoDB"
aws --endpoint-url=${LOCALSTACK_ENDPOINT} dynamodb create-table \
  --table-name ${TABLE_NAME} \
  --attribute-definitions \
    AttributeName=PartitionKey,AttributeType=S \
    AttributeName=SortKey,AttributeType=S \
  --key-schema \
    AttributeName=PartitionKey,KeyType=HASH \
    AttributeName=SortKey,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region ${AWS_REGION} || true

echo "===> Inserindo dados no DynamoDB"
aws --endpoint-url=${LOCALSTACK_ENDPOINT} dynamodb put-item \
  --table-name ${TABLE_NAME} \
  --region ${AWS_REGION} \
  --item '{
    "PartitionKey": {"S": "user#1"},
    "SortKey": {"S": "MENSAGEM#1710000000000"},
    "Id": {"S": "abc123"},
    "Content": {"S": "Olá"},
    "Role": {"S": "cliente"},
    "Timestamp": {"N": "1710000000000"}
  }'

aws --endpoint-url=${LOCALSTACK_ENDPOINT} dynamodb put-item \
  --table-name ${TABLE_NAME} \
  --region ${AWS_REGION} \
  --item '{
    "PartitionKey": {"S": "user#1"},
    "SortKey": {"S": "MENSAGEM#1710000001000"},
    "Id": {"S": "abc123"},
    "Content": {"S": "Oi, como posso ajudar?"},
    "Role": {"S": "assistente"},
    "Timestamp": {"N": "1710000001000"}
  }'

echo "===> Enviando mensagem para SQS"
aws --endpoint-url=${LOCALSTACK_ENDPOINT} sqs send-message \
  --queue-url ${QUEUE_URL} \
  --region ${AWS_REGION} \
  --message-body '{
    "partitionKey": "user#1",
    "id": "abc123",
    "timestamp": 1710000002000
  }'

echo "===> Rodando aplicação .NET"
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=${AWS_REGION}
export AWS_ENDPOINT_URL=${LOCALSTACK_ENDPOINT}

export App__SqsQueueUrl=${QUEUE_URL}
export App__DynamoTableName=${TABLE_NAME}
export App__KafkaBootstrapServers=localhost:9092
export App__KafkaTopic="ainda-nao"

cd SqsWorkerKafka
dotnet restore
dotnet run