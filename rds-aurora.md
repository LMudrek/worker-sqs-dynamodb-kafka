# Aurora Considerations

## `shared_buffers`

Aurora PostgreSQL requires a larger `shared_buffers` than community PostgreSQL recommends. Because Aurora's storage driver replaces the file-system-level cache used by standard PostgreSQL, there is no secondary caching layer to fall back on — undersizing `shared_buffers` directly degrades performance [^1].

The default value is calculated as roughly 75% of available instance memory:

$$
\text{shared\\_buffers} = \frac{\text{DBInstanceClassMemory}}{12038} - 50003
$$

**This default is the recommended starting point.** AWS's own benchmarking shows the cost of deviating from it: reducing `shared_buffers` to 15% of memory on a db.r5.2xlarge produced a 14% drop in throughput and a 14% increase in `VolumeReadIOPS` under a select-only pgbench workload [^1]. Any change to this parameter must be load-tested before being applied to production.

**Takeaways:**
- If the workload's working set doesn't fit in `shared_buffers`, Aurora fetches more pages from storage — driving up `VolumeReadIOPS` and I/O cost [^1].
- Use `BufferCacheHitRatio` to monitor cache efficiency. A sustained low ratio means the working set needs to shrink (archiving, indexing, partitioning, query tuning) — or the instance class needs to grow [^1].
- Aurora preserves the buffer cache across restarts and failovers (*survivable buffer cache*), which speeds up recovery — but **changing `shared_buffers` clears this cache**, causing a cold-restart performance hit [^1].

## `huge_pages`

Huge pages are a memory management feature that reduces overhead when the DB instance works with large contiguous memory allocations — such as those used by `shared_buffers` [^2]. `huge_pages` is on by default and **cannot be disabled** on all Aurora PostgreSQL instance classes, except `db.t3.medium`, `db.t3.large`, `db.t4g.medium`, and `db.t4g.large`, which don't support it [^2].

**Takeaways:**
- Avoid `db.t3.medium`, `db.t3.large`, `db.t4g.medium`, and `db.t4g.large` for production workloads.

---

[^1]: Kumar, S., & Subramanian, G. (2021, April 12). *Amazon Aurora PostgreSQL parameters, Part 1: Memory and query plan management*. AWS Database Blog. Amazon Web Services. https://aws.amazon.com/blogs/database/amazon-aurora-postgresql-parameters-part-1-memory-and-query-plan-management/

[^2]: Amazon Web Services. (n.d.). *Performance and scaling for Amazon Aurora PostgreSQL — Huge pages for Aurora PostgreSQL*. AWS Documentation. Amazon Web Services. https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Managing.html
