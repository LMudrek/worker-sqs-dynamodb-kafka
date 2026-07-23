# Aurora Considerations — Vector Search Table (pgvector, sem índice vetorial)

Tabela: PK `id`; coluna secundária `ref_id` (não-PK, índice B-tree, filtro de igualdade); ~10 colunas de atributos; `halfvec(3072)` para similaridade de cosseno (`ORDER BY embedding <=> :query LIMIT k`), sem índice ANN. Padrão de acesso: alto volume de `UPDATE` in-place, leitura indexada por `ref_id`, busca vetorial via sequential scan completo.

## Paralelismo de query (`max_parallel_workers_per_gather`, `max_parallel_workers`)

Sem índice ANN, a latência da busca por similaridade depende do quanto o *sequential scan* é paralelizado. `max_parallel_workers_per_gather` limita workers por query; `max_parallel_workers`, o total no cluster[^1].

**Takeaways:**
- Cada worker é um processo de backend completo e aloca seu próprio `work_mem` — dimensionar considerando buscas vetoriais concorrentes, não só o tamanho da tabela[^2].
- Validar com `EXPLAIN (ANALYZE, BUFFERS)` se o planner está escolhendo parallel seq scan para a busca vetorial e index scan para os filtros por `ref_id`.

## `work_mem`

O `ORDER BY <=> LIMIT k` mantém o top-k em memória durante o scan; `work_mem` insuficiente gera *spill* para disco temporário[^3].

**Takeaways:**
- Multiplicar `work_mem` pelo número de conexões concorrentes esperadas; usar Amazon RDS Proxy para não estourar a memória da instância[^1].
- Ativar `log_temp_files` temporariamente para confirmar se há *spill* nas buscas vetoriais[^4].

## Autovacuum (`autovacuum_vacuum_scale_factor`, `autovacuum_vacuum_cost_limit`, `autovacuum_vacuum_cost_delay`)

Sem índice ANN, toda busca vetorial lê a tabela inteira — bloat de `UPDATE` infla diretamente o custo de cada scan, não só o espaço em disco[^5].

**Takeaways:**
- Ajustar por tabela (`ALTER TABLE ... SET (autovacuum_vacuum_scale_factor = 0.05)`), abaixo do default (0.2), dado o volume de updates[^5].
- Baixar `autovacuum_vacuum_cost_delay` / subir `autovacuum_vacuum_cost_limit` para o vacuum acompanhar o ritmo de updates[^5].
- `pg_repack` para bloat já acumulado (reorganiza com lock mínimo)[^5].
- Monitorar `n_dead_tup` em `pg_stat_user_tables` para essa tabela.

## `effective_cache_size` / `shared_buffers`

`halfvec(3072)` ocupa ~6 KB por linha só nessa coluna (3072 × 2 bytes), acima dos 1024–1536 dimensões típicas dos benchmarks da AWS — o *working set* precisa caber em cache para o scan completo permanecer rápido.

**Takeaways:**
- `effective_cache_size` em ~75% da memória da instância[^1].
- `BufferCacheHitRatio` é o primeiro sinal de que a tabela deixou de ser servida do cache[^1].
- Aurora Optimized Reads (cache em NVMe local) antes de escalar a instância, se o *working set* não couber em memória[^1].

## Quando considerar um índice ANN (HNSW)

Índices HNSW degradam com `UPDATE`/`DELETE` (sem compactação in-place) e exigem `REINDEX CONCURRENTLY` periódico[^1]. Critérios de decisão:

- Tamanho da tabela acima de ~10.000–50.000 vetores, faixa em que o seq scan paralelo deixa de ser competitivo[^1].
- Recall aproximado é aceitável (HNSW não garante 100%)[^1].
- Se optar por criar: `maintenance_work_mem` precisa cobrir o grafo completo no build (pgvector emite `NOTICE` quando não cabe), e `hnsw.ef_search` deve ser setado por sessão/query (default 40, AWS recomenda iniciar em 100)[^1].

## Observabilidade

- `BufferCacheHitRatio` (>99% esperado) e `ReadIOPS` sustentado alto (indica scan caindo para storage)[^1].
- `aurora_stat_statements(true)` filtrando por `<=>`, `<#>`, `<->`, ordenado por tempo total / pico de memória[^1].
- `n_dead_tup` em `pg_stat_user_tables`[^5].

---

## Footnotes

[^1]: Aichholzer, S., & Pareek, V. (2026, June 25). *Running pgvector in production on Amazon Aurora PostgreSQL*. AWS Database Blog. <https://aws.amazon.com/blogs/database/running-pgvector-in-production-on-amazon-aurora-postgresql/>
[^2]: Amazon Web Services. (n.d.). *Initial troubleshooting for common PostgreSQL performance issues in Aurora PostgreSQL — Parallel query resource exhaustion*. AWS Documentation. <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/PostgreSQL.InitialTroubleshooting.html>
[^3]: Amazon Web Services. (n.d.). *Tuning memory parameters for Aurora PostgreSQL — work_mem*. AWS Documentation. <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.BestPractices.Tuning-memory-parameters.html>
[^4]: Amazon Web Services. (n.d.). *Tuning memory parameters for Aurora PostgreSQL — log_temp_files*. AWS Documentation. <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.BestPractices.Tuning-memory-parameters.html>
[^5]: Amazon Web Services. (n.d.). *Initial troubleshooting for common PostgreSQL performance issues in Aurora PostgreSQL — Autovacuum tuning*. AWS Documentation. <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/PostgreSQL.InitialTroubleshooting.html>
