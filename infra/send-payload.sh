#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="dotnet-sqs-dynamo-kafka"
AWS_REGION="us-east-1"
LOCALSTACK_ENDPOINT="http://localhost:4566"
QUEUE_NAME="my-queue"
QUEUE_URL="http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/my-queue"
TABLE_NAME="conversation-table"
KAFKA_TOPIC="conversations"

echo "===> Enviando mensagem para SQS"
aws --endpoint-url=${LOCALSTACK_ENDPOINT} sqs send-message \
  --queue-url ${QUEUE_URL} \
  --region ${AWS_REGION} \
  --message-body '{
    "partitionKey": "user#1",
    "id": "abc123",
    "timestamp": 1710000002000
  }'