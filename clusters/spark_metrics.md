## Spark UI Cheat Sheet (Databricks): What to Watch and Why

Use this as a quick guide while analyzing a running job in the Spark UI. Focus first on job/stage critical path and skew, then confirm executor health and SQL operator hotspots.

---

## 1) Most important metrics to check (first pass)

1. Stage/task skew indicators

- Explanation: Long tail tasks and uneven data cause slow stages and shuffle hotspots.
- Target: Max task duration <= 3x median; shuffle read per task within a narrow range; few retries.
- Lower than target: Very uniform tasks, good; watch total task count isn’t too low (under-parallelized).
- Higher than target: Skew likely; repartition, key salting, enable AQE, adjust partitions.

2. Shuffle read/write size and time

- Explanation: Shuffle dominates many jobs; remote reads and spill slow execution.
- Target: Shuffle fetch wait time < 10% of task time; minimal spill; healthy network locality.
- Lower than target: Great; ensure you’re not under-partitioned (too little parallelism).
- Higher than target: Network or skew; increase partitions (or allow AQE to coalesce), fix skew, consider broadcast joins.

3. GC time (executor) percent

- Explanation: Time spent in garbage collection vs compute.
- Target: < 5–10% of executor runtime.
- Lower than target: Good; memory pressure is low.
- Higher than target: Memory pressure; reduce row width, cache less, increase memory or tune partitions; consider Kryo, off-heap, or avoiding wide rows.

4. Scheduler delay / task deserialization / result serialization

- Explanation: Overheads that don’t do useful work.
- Target: Each < 2–5% of task time.
- Lower than target: Good.
- Higher than target: Driver overload, too many tiny tasks, heavy closures/serialization; increase partition size, reduce small files, simplify UDFs.

5. Failed/retried tasks

- Explanation: Reliability and wasted time.
- Target: Failures ≈ 0; retries rare.
- Lower than target: Good.
- Higher than target: Intermittent I/O, OOM, or skew; check logs, memory, and input quality.

6. Executor utilization

- Explanation: Are cores busy doing compute instead of waiting?
- Target: Active tasks ≈ total cores; GC% low; no frequent executor loss.
- Lower than target: Underutilized; increase partitions or cluster size fit.
- Higher than target: Queues and long waits; increase parallelism or cluster resources, remove bottlenecks.

7. SQL operator hotspots (SQL tab)

- Explanation: Specific scans/joins/aggregations dominate time.
- Target: Scans show good selectivity; broadcast fits threshold; shuffle exchanges reasonable.
- Lower than target: Good.
- Higher than target: Missing filters/pushdown, bad join strategy, excessive shuffles/spill; rewrite queries or adjust configs.

---

## 2) Important metrics by Spark UI tab

### Jobs tab

- Job Duration

  1.  Quick: Total wall time of a job (all stages on the critical path).
  2.  Target: As low as possible given data size; stable across runs.
  3.  Smaller: Faster job; if much smaller than expected, verify completeness (no partial work?).
  4.  Higher: Regression; drill into Stages -> Tasks to find skew, shuffle, or GC issues.

- Stages (count) and Status

  1.  Quick: Number of stages and whether any are failed/active.
  2.  Target: Few failures; reasonable stage count for your plan.
  3.  Smaller: Simple pipeline; ensure enough parallelism inside stages.
  4.  Higher: Many short stages indicate excessive shuffles/checkpoints; consider reducing unnecessary wide deps.

- Event Timeline gaps
  1.  Quick: Idle periods between tasks/stages.
  2.  Target: Minimal gaps; good concurrency.
  3.  Smaller: Good scheduling and resource use.
  4.  Higher: Driver bottleneck, cluster scaling delays, or skew; check scheduler delay and active executors.

---

### Stages tab

- Stage Duration and Critical Path

  1.  Quick: Time for each stage; the slowest stage defines your critical path.
  2.  Target: Balanced stage times; no single stage dominates.
  3.  Smaller: Healthy; verify stage still has enough tasks (not under-parallelized).
  4.  Higher: Focus here; inspect Task metrics for skew, shuffle wait, spill, or GC.

- Task Duration Distribution (p50/p95/p99/max)

  1.  Quick: Variability of task times in the stage.
  2.  Target: max/median <= 3x; p99 close to p50.
  3.  Smaller: Uniform work distribution.
  4.  Higher: Skew; repartition by better key, salt hot keys, pre-aggregate, or use AQE skew join.

- Shuffle Read Size and Fetch Wait Time

  1.  Quick: Bytes read in shuffle and time waiting to fetch blocks.
  2.  Target: Fetch wait < 10% of task time; balanced read size across tasks.
  3.  Smaller: Less shuffle pressure; ensure not starved by too few partitions.
  4.  Higher: Network bottleneck or skew; increase partitions, enable AQE, tune join strategies, optimize file sizes.

- Shuffle Write Size and Write Time

  1.  Quick: Bytes written to shuffle and time to commit them.
  2.  Target: Write time small vs compute; partitions not too large (>512MB) or too tiny (<16–64MB).
  3.  Smaller: Efficient; beware of too many tiny files (high task count).
  4.  Higher: Disk/network pressure; coalesce, adjust partitioning, or add throughput.

- Spill (Memory and Disk)

  1.  Quick: Data that couldn’t fit in memory during shuffle/sort/agg.
  2.  Target: Zero or minimal.
  3.  Smaller: Good memory sizing and partitioning.
  4.  Higher: Memory pressure; increase partitions, reduce row width, use map-side pre-agg, consider larger executors.

- Task Deserialization Time

  1.  Quick: Time to deserialize task closures and dependencies.
  2.  Target: < 2–5% of task time.
  3.  Smaller: Fine.
  4.  Higher: Large closures or classpath issues; reduce UDF complexity, broadcast configs/luts instead of shipping large objects.

- Result Serialization Time

  1.  Quick: Time to serialize task results back to the driver/exchange.
  2.  Target: < 2–5% of task time.
  3.  Smaller: Fine.
  4.  Higher: Big task outputs; reduce collect(), write to storage, or aggregate before returning.

- Getting Result Time

  1.  Quick: Time transferring results to the driver.
  2.  Target: Near-zero for non-collect actions.
  3.  Smaller: Fine.
  4.  Higher: Driver is a bottleneck; avoid collect/show on large data, increase driver memory/network.

- GC Time

  1.  Quick: Time spent in GC within tasks.
  2.  Target: < 5–10%.
  3.  Smaller: Healthy memory behavior.
  4.  Higher: Memory pressure; reduce caching, widen partitions, avoid super-wide rows, consider Kryo or off-heap.

- Failed/Killed/Retried Tasks
  1.  Quick: Reliability within the stage.
  2.  Target: ≈ 0; few retries.
  3.  Smaller: Good.
  4.  Higher: Skew, OOM, transient I/O; review logs and inputs, consider raising max failures only after fixing root cause.

---

### Tasks tab

- Locality Level (PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY)

  1.  Quick: Data locality for tasks.
  2.  Target: Mostly PROCESS/NODE_LOCAL.
  3.  Smaller (lower locality values): More ANY/RACK_LOCAL indicates remote reads; can be bad for IO.
  4.  Higher (better locality): Good; if still slow, look at compute or GC.

- Executor Run Time vs Overheads

  1.  Quick: Compute time vs scheduler/serialization/fetch wait.
  2.  Target: Run time dominates; overheads each < 5–10%.
  3.  Smaller (overheads low): Efficient execution.
  4.  Higher: Too many tiny tasks, heavy serialization, or slow shuffle; increase partition size, reduce small files, optimize joins.

- Peak Execution Memory (per task)
  1.  Quick: Memory needed by the task.
  2.  Target: Below executor memory headroom; no frequent spills.
  3.  Smaller: Good; room to coalesce partitions if needed.
  4.  Higher: Risk of OOM/spill; increase partitions or executor memory, reduce row width.

---

### SQL tab (Databricks Runtime)

- Scan (FileScan) Read Size and Output Rows

  1.  Quick: Bytes read and rows produced by each scan.
  2.  Target: High selectivity (output << input); predicate/file pruning effective.
  3.  Smaller (bytes read): Good pruning; verify not starved by too few partitions.
  4.  Higher: Missing filters/pushdown, small-file problem; add partition filters, Z-ORDER, optimize file sizes (128–512MB), use CDF/pruning.

- ShuffleExchange (size and partition count)

  1.  Quick: Data shuffled and number of shuffle partitions.
  2.  Target: Reasonable partitions such that post-shuffle partition size ~ 128–256MB; AQE coalesces small ones.
  3.  Smaller: Less shuffle pressure; ensure enough parallelism (at least ~2–4× total cores for CPU-bound stages).
  4.  Higher: Heavy shuffle; fix join order, filter earlier, consider broadcasting small tables; address skew.

- BroadcastExchange (size)

  1.  Quick: Data size broadcast to executors for broadcast joins.
  2.  Target: <= spark.sql.autoBroadcastJoinThreshold (see Environment tab).
  3.  Smaller: Good; fast joins.
  4.  Higher: Broadcast disabled or table too large; Spark falls back to shuffle join; filter or aggregate to enable broadcast where appropriate.

- Aggregate/Sort (time, spill)

  1.  Quick: Operator time and whether it spilled to disk.
  2.  Target: No spill; time proportional to input size.
  3.  Smaller: Efficient pipeline; codegen helps.
  4.  Higher: Memory pressure or skew; pre-aggregate, increase partitions, consider two-phase agg, or adjust memory.

- Operator Output Rows vs Input Rows
  1.  Quick: Selectivity of each operator.
  2.  Target: Expected reduction where filters/joins apply.
  3.  Smaller (more reduction): Good selectivity.
  4.  Higher (less reduction): Weak predicates or join selectivity; reconsider join keys, filters, and statistics.

---

### Executors tab

- Active Tasks vs Cores

  1.  Quick: Concurrency level.
  2.  Target: Active tasks ≈ total cores during busy stages.
  3.  Smaller: Underutilization; increase partitions or reduce skew; check scheduling/backpressure.
  4.  Higher: Queued tasks; consider more cores or better partition sizing.

- GC Time (% per executor)

  1.  Quick: Fraction of time executors spend in GC.
  2.  Target: < 5–10%.
  3.  Smaller: Healthy.
  4.  Higher: Memory pressure; reduce caching, repartition, avoid wide rows, or increase memory.

- Input/Shuffle Read/Write per Executor

  1.  Quick: Work distribution across executors.
  2.  Target: Roughly even distribution.
  3.  Smaller: Possible imbalance; confirm data distribution.
  4.  Higher: Hot executors indicate skew or locality issues; repartition/salt keys.

- Failed Tasks / Executor Lost

  1.  Quick: Stability at executor level.
  2.  Target: ≈ 0.
  3.  Smaller: Good.
  4.  Higher: Node issues, OOM, or network; consider autoscaling health, instance type, or memory tuning.

- Storage Memory and Cached Blocks
  1.  Quick: Memory used for caching and number of cached blocks per executor.
  2.  Target: Enough headroom to avoid eviction; no frequent cache churn.
  3.  Smaller: Fine; possibly under-using cache.
  4.  Higher: Evictions/churn; reduce cache footprint or increase memory.

---

### Storage tab

- Cached RDD/DataFrame Count and Fraction Cached

  1.  Quick: What’s cached and how completely.
  2.  Target: Critical datasets fully cached if repeatedly used.
  3.  Smaller: Partial caching; may cause inconsistent performance.
  4.  Higher: If memory-limited, consider selective caching or checkpointing instead.

- Cached Size and Evictions
  1.  Quick: Total size in memory and number of evictions.
  2.  Target: Evictions ≈ 0.
  3.  Smaller: Fits in memory; good.
  4.  Higher: Memory pressure; reduce cache level (e.g., MEMORY_ONLY -> MEMORY_AND_DISK), drop unused caches, or scale memory.

---

### Environment tab (sanity checks for metrics context)

- spark.sql.adaptive.enabled (AQE)

  1.  Quick: Adaptive execution toggles skew handling and partition coalescing.
  2.  Target: true for most ETL/SQL workloads.
  3.  false: You’ll miss skew mitigation and auto partition tuning.
  4.  true: If instability occurs, investigate specific AQE sub-settings, not disabling entirely.

- spark.sql.shuffle.partitions

  1.  Quick: Baseline shuffle parallelism for SQL.
  2.  Target: Use AQE to coalesce; otherwise size so you have ~2–4× total cores for CPU-bound stages.
  3.  Smaller: Underutilization; long tasks.
  4.  Higher: Too many tiny tasks/files; increase size per partition or enable AQE.

- spark.sql.autoBroadcastJoinThreshold
  1.  Quick: Max size to broadcast a table.
  2.  Target: Value that fits your executors; defaults vary; verify here.
  3.  Smaller: Missed broadcast opportunities; joins become shuffle-based.
  4.  Higher: Risk of memory pressure during broadcast; monitor GC and spill.

---

## Practical rules-of-thumb and fixes

- Skew detection and mitigation

  - If max/median task duration > 5–10x, or if a few tasks read much more shuffle data, you have skew.
  - Mitigate with AQE (skew join), repartition by higher-cardinality key, key salting, map-side pre-aggregation.

- Partition sizing

  - Aim for post-shuffle partition size ~ 128–256MB. With AQE on, let it coalesce small partitions automatically.
  - Keep active tasks ≈ 2–4× total cores for CPU-heavy stages.

- Shuffle health

  - Keep shuffle fetch wait < 10% of task time; reduce remote reads by improving locality and skew.
  - Avoid massive single shuffles; pipeline filters and pre-aggregations earlier.

- Memory and GC

  - Keep GC < 5–10% of runtime. If higher: reduce cached data, increase partitions, use narrower rows, prefer columnar sources.
  - Watch spill metrics; persistent spill indicates memory pressure.

- Driver safety

  - Avoid collect/show on large datasets; use writes or aggregates. High “Getting Result Time” means driver pressure.

- Databricks tips
  - Ensure Photon/DBR selection matches workload type (SQL/ETL often benefits from Photon).
  - Use Auto Optimize/Z-ORDER on Delta for better pruning and smaller shuffles.
  - Regularly OPTIMIZE + VACUUM to fix small-file issues impacting scans/shuffles.

---

## Quick triage flow while the job is running

1. Jobs -> Identify the slowest job and stage.
2. Stages -> Check task duration distribution and shuffle read/write/ spill.
3. Tasks -> Inspect overheads (scheduler delay, fetch wait, serialization) and locality.
4. SQL -> Find the hottest operators (scan, exchange, join, agg) and confirm strategy/selectivity.
5. Executors -> Verify utilization, GC%, and even data distribution.
6. Storage -> Confirm caches are effective and not evicting.

Use the targets above to decide whether to repartition, broadcast, filter earlier, enable/tune AQE, or adjust cluster sizing.
