Parametrizar:
- Autovacuum params

Prever:

- Buffer cache hit ratio + latencia consultas (NOW)
- Datadog agent VS database insights (NOW - hipotese)
- Vacuum blockers e eficiencia (?)
    - [pgstattuple](https://aws.amazon.com/pt/blogs/database/improve-postgresql-performance-using-the-pgstattuple-extension/)
- Transaction ID wraparound (NOW - Cloudwatch MaximumUsedTransactionIDs)

Acionar:
- Autoscaling (NOW - latency degradation)
- Aurora optimized reads (AFTER - latencia degradation)
- Upscaling instance (AFTER - latencia degradation)
- Uso RDS Proxy / PgBouncer (AFTER hipotese se melhora - hoje a App limita conexões, e se forem múltiplas? E se precisar aumentar o limite app?)

Referencias:
- [AWS Autovacuum](https://aws.amazon.com/pt/blogs/database/understanding-autovacuum-in-amazon-rds-for-postgresql-environments/)