# Aurora Considerations

## `shared_buffers`

According to AWS documentation on Amazon Aurora PostgreSQL parameter tuning [^1], the `shared_buffers` parameter can be calculated as:

$$
\text{shared\\_buffers} = \frac{\text{DBInstanceClassMemory}}{12038} - 50003
$$

As noted in the AWS Database Blog [^1]:

> "If the working set for your workload can't fit in the shared buffers, the Aurora instance needs to fetch more pages from storage. This increase in I/O shows in VolumeReadIOPS ([Billed] Volume Read IOPS on the Amazon RDS management console) and results in a higher Aurora I/O bill. You can review BufferCacheHitRatio to judge the efficiency of the shared buffers utilization. If this metric is consistently lower, you should work on reducing your working set, for example by archiving old data, adding indexes, implementing table partitioning, and tuning queries. If you can't tune your workload further, consider increasing the instance class to allocate more memory for shared buffers."

- heavy_pages

---

[^1]: Amazon Web Services. "Amazon Aurora PostgreSQL Parameters, Part 1: Memory and Query Plan Management." *AWS Database Blog*. Accessed July 22, 2026. https://aws.amazon.com/pt/blogs/database/amazon-aurora-postgresql-parameters-part-1-memory-and-query-plan-management/
