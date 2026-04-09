package com.example.dynamodb.service;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MotorQueryService {

    private final DynamoDbAsyncClient dynamoClient;
    private final String tableName;

    // Controle de fan-out (evita saturação)
    private final ExecutorService executor;
    private final int maxConcurrency = 10;

    public MotorQueryService(DynamoDbAsyncClient dynamoClient, String tableName) {
        this.dynamoClient = dynamoClient;
        this.tableName = tableName;
        this.executor = Executors.newFixedThreadPool(maxConcurrency);
    }

    /**
     * Cenário 1: macro IN (A, B, C)
     */
    public List<Map<String, AttributeValue>> queryByMacros(String id, List<String> macros) {
        List<CompletableFuture<List<Map<String, AttributeValue>>>> futures = macros.stream()
                .map(macro -> CompletableFuture.supplyAsync(() -> queryByMacro(id, macro), executor))
                .collect(Collectors.toList());

        return futures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Cenário 2: macro + categoria (lookup direto)
     */
    public Optional<Map<String, AttributeValue>> queryByMacroAndCategory(String id, String macro, String categoria) {
        String sk = buildSortKey(macro, categoria);

        QueryRequest request = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk AND SK = :sk")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(id).build(),
                        ":sk", AttributeValue.builder().s(sk).build()
                ))
                .limit(1)
                .build();

        try {
            QueryResponse response = dynamoClient.query(request).get();

            if (response.items().isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(response.items().get(0));

        } catch (Exception e) {
            throw new RuntimeException("Erro ao consultar macro+categoria", e);
        }
    }

    /**
     * Cenário 3: sem filtro → usa whitelist
     */
    public List<Map<String, AttributeValue>> queryWithWhitelist(String id, List<String> allowedMacros) {
        return queryByMacros(id, allowedMacros);
    }

    /**
     * Query base por macro (usando begins_with)
     */
    private List<Map<String, AttributeValue>> queryByMacro(String id, String macro) {
        String prefix = buildPrefix(macro);

        QueryRequest request = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk AND begins_with(SK, :sk)")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(id).build(),
                        ":sk", AttributeValue.builder().s(prefix).build()
                ))
                .build();

        List<Map<String, AttributeValue>> results = new ArrayList<>();

        try {
            QueryResponse response = dynamoClient.query(request).get();
            results.addAll(response.items());

            // Paginação
            while (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
                request = request.toBuilder()
                        .exclusiveStartKey(response.lastEvaluatedKey())
                        .build();

                response = dynamoClient.query(request).get();
                results.addAll(response.items());
            }

        } catch (Exception e) {
            throw new RuntimeException("Erro ao consultar macro: " + macro, e);
        }

        return results;
    }

    private String buildPrefix(String macro) {
        return "MOTOR#" + macro + "#";
    }

    private String buildSortKey(String macro, String categoria) {
        return "MOTOR#" + macro + "#" + categoria;
    }

    public void shutdown() {
        executor.shutdown();
    }
}