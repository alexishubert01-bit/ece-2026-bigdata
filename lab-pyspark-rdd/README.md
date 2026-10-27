# Lab: Introduction to Spark's RDD API

Author: <your name>
Environment: Onyxia `vscode-pyspark`, PySpark 4.1.1, `local[*]`, datasets `bronze/users.csv` (7 KB) and `bronze/orders.csv` (324 KB).

## 1. RDD creation and partitions

Results: `users_lines` and `orders_lines` both have 2 partitions; `orders_lines.repartition(8)` has 8.

- **What decides the default number of partitions of `textFile()`?** Spark creates one partition per file block, but the files here are far smaller than a block. The default `minPartitions` of `textFile()` is `min(defaultParallelism, 2)`, hence 2 partitions whatever the number of cores.
- **`repartition()` vs `coalesce()`:** `repartition()` always shuffles and can increase or decrease the partition count. `coalesce()` does not shuffle by default (it merges neighbouring partitions) and is meant to reduce the count.

## 2. Lazy evaluation and lineage

`orders_data.toDebugString()` shows the lineage `HadoopRDD -> MapPartitionsRDD -> PythonRDD` with 2 partitions, no data. `orders_data.count()` returns 2829 (header excluded).

- **Second `count()`: new stage?** Yes, a new job and stage. Without `cache()` Spark re-reads `orders.csv` and re-applies the filter at every action.
- **Where would `.cache()` change that?** On `orders_data` before the first `count()`: the second action would read the partitions from memory. In this lab, the `.cache()` on `orders_rdd` plays that role.

## 3. The cost of no schema

`users_lines.count()` returns 101 = 1 header + 50 users x 2 physical lines (the address spans two lines). Filtering on a leading UUID gives 50 records.

- **Why does `multiLine=True` work but `textFile()` does not?** `textFile()` splits on every newline, without knowing it sits inside quotes. The CSV reader tracks the quoted state, so a newline inside quotes stays in the field. The cost: a multiLine CSV cannot be split freely across partitions.
- **What must a format satisfy for line-oriented reading to be safe?** One physical line must equal exactly one record: no newline inside a value (or escaped, as in JSON Lines), and each line must be parseable on its own.

## 4. Manual parsing of orders

`parse_order` works on the 3 first rows (quantity as int, product lowercased).

- **Extra comma in `product`?** `split(",")` returns 6 items, the unpacking raises `ValueError: too many values to unpack`, only at the action that first touches the row (a `take(3)` would not see it, a `count()` would), and the task fails.
- **Where does the DataFrame API check this?** In the CSV reader, at the action that triggers the read, row by row against the schema, with the `mode` option: `PERMISSIVE` (default, nulls), `DROPMALFORMED`, `FAILFAST`.

## 5. Key-value RDDs: total quantity per user

`reduceByKey(add)` and `groupByKey().mapValues(sum)` give the same result: 50 users (for example 12 for user `f143262f...`).

- **What does `groupByKey` shuffle?** Every `(key, value)` pair, without map-side combining, and all values of a key must fit in memory in one task. `reduceByKey` combines locally per partition first, so it shuffles at most one pair per key and per partition.
- **When is `groupByKey` unavoidable?** When all values of a key are needed together and the operation is not an associative and commutative merge (median, percentiles, sorting values per group). `aggregateByKey` and `combineByKey` cover the other cases, including results of a different type.

## 6. Manual deduplication

`orders_rdd.count()` and `orders_dedup.count()` are both 2829: this dataset has no duplicate `order_id`, so the deduplication removes nothing.

- **What plays the role of `partitionBy()`?** The shuffle done by `reduceByKey`, with `order_id` as key: all rows of an `order_id` land in the same partition where `most_recent` merges them.
- **Which version states the intent better?** The window function (`row_number() ... ORDER BY ordered_at DESC`, keep rank 1) is more explicit. `reduceByKey` is efficient (local combining) but hides the intent in the combiner, which must be associative and commutative. String comparison of dates works because the ISO format sorts chronologically.

## 7. Joining orders with users

`users_kv` has 50 pairs; `orders_kv.join(users_kv).count()` is 2829: every order has a known user.

- **Skipping the shuffle:** both RDDs must be co-partitioned (same partitioner and same number of partitions, for example `partitionBy(n)` on both, then persisted).
- **`leftOuterJoin`:** an order without a user is kept with `None` instead of the username, while `join` drops it.
- **Risk of `collect()`:** the whole result is brought to the driver memory, with an out-of-memory risk on large data. Filter or aggregate first, or use `take`, `count`, or write the result out.

## Exercises

1. **`aggregateByKey`:** zero value `(0, 0)`, `seq_op` adds 1 and the quantity, `comb_op` sums the pairs. For user `f143262f...`: `(4, 12)` and an average of 3.0.
2. **Unknown user:** after adding one order with `user_id` `unknown-user-id`, `leftOuterJoin` returns 2830 rows and exactly 1 order has `None` as username.
3. **DataFrame version:** `orders_df.groupBy("user_uuid").sum("quantity")` (the column is `user_uuid` in the file) gives 50 users in 2 lines of code. The `explain()` shows `FileScan` (only 2 columns read), partial `HashAggregate`, `Exchange hashpartitioning(user_uuid, 200)`, final `HashAggregate`: the same pattern as `reduceByKey`, built automatically, with 200 shuffle partitions planned versus 2 for the RDD.
4. **Cache timing (`time.perf_counter`):** with cache, `orders.count()` took 0.944 s then 0.216 s and 0.185 s; without cache 0.393 s then 0.217 s and 0.182 s. For `user_quantity`, around 0.2 to 0.4 s in both cases. No measurable gain at this data size: the fixed cost of launching a Spark job dominates, so caching only pays off on larger data or more expensive transformations. The Spark UI was not inspected; `getStorageLevel()` was used instead (memory level with cache, none without).

## Cleanup

`spark.stop()`, `exit()`, then `rm -f users.csv orders.csv` (both files are also in the local `.gitignore`).
