# Lab: Trino (deployment, silver to gold, star schema)

Module 10, three labs: deploy Trino with Helm, build the gold layer from the silver Iceberg tables with SQL, then model it as a star schema.

Environment: Onyxia `vscode-pyspark`, Trino 480 (chart 1.45.0, coordinator only), Iceberg connector on the Polaris REST catalog of `lab-polaris-deploy`. The Python cells of the statements are run as scripts with `uv run python`.

## Project layout

| File | Content |
|---|---|
| `trino/deployment/Chart.yaml`, `values.yaml` | Umbrella chart on the official Trino chart, catalog `lakehouse` = Iceberg REST on Polaris, principal `dev`, S3 keys from the `polaris-s3` Secret |
| `trino_client.py` | Connection (`catalog="lakehouse"`) and the `sql()` helper of the statement, plus `show()` and `value()` |
| `register_users.sh` | Prerequisite: registers `lab.silver.users` in Polaris through the REST API (the table was dropped by the Polaris lab cleanup) |
| `01_connect_explore.py` | Lab 2: catalogs, schemas, `DESCRIBE`, counts, address parsing, orphan orders |
| `02_metadata_plan.py` | Lab 2: `$snapshots`, `$files`, `$history`, `$manifests`, distributed plan, `EXPLAIN ANALYZE`, query history |
| `03_gold.py` | Lab 2: `gold` schema, CTAS `orders_by_state_month`, `top_products`, `user_activity` |
| `04_validate.py` | Lab 2: assertions gold versus silver |
| `05_refresh.py` | Lab 2: `DELETE` + `INSERT` refresh, snapshots, time travel |
| `06_star_schema.py` | Lab 3: `dim_user`, `dim_product`, `dim_date`, `fact_orders`, star queries |
| `07_star_validate.py` | Lab 3: assertions of the statement, plus key uniqueness and row-by-row consistency |
| `ex1_partitioning.py` to `ex4_optimize.py` | Lab 3 exercises 1 to 4 (Trino) |
| `ex5_spark_compare.py`, `run_ex5.sh` | Lab 3 exercise 5 (Spark through Polaris, PySpark 4.1.1) |
| `logs/` | Not versioned: outputs of the scripts |
| `warehouse/gold/` | Metadata (no data files) of the gold tables, synced from the bucket |

## How to run

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE" POLARIS_URI="http://polaris:8181" POLARIS_CATALOG="$LAB_BUCKET_NAME"
export TRINO_HOST=trino TRINO_PORT=8080 TRINO_USER="$KUBERNETES_NAMESPACE"   # TRINO_PORT is otherwise tcp://... (Kubernetes service variable)
helm dependency update trino/deployment
helm upgrade --install trino trino/deployment --wait
./register_users.sh
uv run python 01_connect_explore.py
```

The Polaris credentials and the S3 keys are only stored in Kubernetes Secrets, never in the repository.

## Differences with the lab statements

- Trino was already deployed by the lab 1 chart. The temporary S3 keys of the `polaris-s3` Secret had expired: the Secret was refreshed and Polaris and Trino restarted by scaling to 0 then 1 (a rolling restart stays blocked, the namespace quota has no room for a second pod).
- `silver.users` was no longer registered in Polaris (dropped by `08_cleanup.py` of the Polaris lab): `register_users.sh` registers the Hadoop table `lab.silver.users` (`v1.metadata.json`) again through the Polaris REST API, without copying data. The `dev` credentials file had been lost with the previous service, so the `root` principal of the catalog was used.
- `silver.orders` is the version evolved in the Iceberg lab: 2,833 rows (not 5,000), an extra `source` column, and 3 snapshots written on 2026-11-20.
- `DROP TABLE` fails in Trino: Trino always requests a purge and Polaris refuses it (`Unable to purge entity ... set the Polaris configuration DROP_WITH_PURGE`). The scripts use `CREATE OR REPLACE TABLE`, which also keeps the snapshot history.
- The statements run the cells in a Python shell; here each step is a script, so that the code is versioned.
- Exercise 3 inserts one order in `silver.orders` (2,834 rows afterwards); the star schema was rebuilt after it, so the lab 3 results below are on 2,834 orders.
- Spark exercise: the Polaris `dev` credential was lost, the `root` principal is used.
- The gold metadata is committed in `warehouse/gold/` (statement: `lab_trino/gold/`), like `lab-polaris/warehouse/`.

## Results

| Step | Observed result |
|---|---|
| `SELECT version()` | Trino 480; catalogs `lakehouse`, `system`, `tpcds`, `tpch` |
| `SHOW SCHEMAS FROM lakehouse` | `information_schema`, `silver`, `system` (then `gold`) |
| `DESCRIBE silver.orders` | `order_id varchar`, `user_id varchar`, `ordered_at timestamp(6) with time zone`, `quantity integer`, `product varchar`, `source varchar` |
| Counts silver | 2,833 orders, 50 users |
| Address parsing | `AE` 3 users / 3 without city, `AP` 2 / 2, `FM` 3 / 0; other states 2 users, no missing city |
| Orphan orders | 0 |
| `orders$snapshots` | 3: `overwrite`, `append`, `append` (2026-11-20, Spark); `$files`: 3 Parquet files |
| Distributed plan | 4 fragments; `orders` and `users` scanned in their own `SOURCE` fragments; join `distribution = REPLICATED` (broadcast of `users`) |
| `EXPLAIN ANALYZE` | scan of `orders`: 2,833 rows (124 kB); scan of `users`: 50 rows; 41 groups after the join |
| Gold CTAS rows written | `orders_by_state_month`: 50, `top_products`: 6, `user_activity`: 50 |
| `top_products` | croissant 1,523 units (rank 1), drink 1,493, bread 1,425, brioche 1,424, donut 1,390, cookie 1,373 |
| `04_validate.py` | OK: 8,628 units, 50 users, 2,833 orders |
| Gold files | `s3://<bucket>/warehouse/gold/top_products-<uuid>/data/...parquet` |
| Refresh of `top_products` | 3 snapshots: `append` (CTAS), `delete`, `append`; 6 products as of the first snapshot and now |
| Star row counts | `dim_user` 50, `dim_product` 6, `dim_date` 118 (2020-01-01 to 2020-04-27), `fact_orders` = `silver.orders` |
| Star query | first rows of `orders_by_state_month` rebuilt identically (`AL` 65 orders / 199 units, `AP` 17 / 49, `AS` 12 / 40) |
| `07_star_validate.py` | statement checks OK (2,834 orders, 8,632 units); extra check: **1 duplicated `order_id`** |

The duplicated `order_id` (`1b0ac4ef-...`) exists twice in `silver.orders`: the original row and a copy appended by the Iceberg lab (`source = spark`). The checks of the statement do not see it, since the fact table copies the duplicate faithfully.

## Questions, lab 2: silver to gold

### Connection

- **What does `catalog=` change?** It sets the default catalog of the session: an unqualified `silver.orders` resolves to `lakehouse.silver.orders`. The statements of the lab use fully qualified names, so they work with or without it.

### Catalogs and types

- **Type of `ordered_at`?** `timestamp(6) with time zone`: microsecond precision, and the time zone is kept (UTC for this table). Iceberg `timestamptz` maps to it.
- **Type of `order_id`?** `varchar`. Spark wrote a string column, Iceberg stored `string`, Trino reads `varchar`. Trino has a `uuid` type, but it is only used when the Iceberg column is of type `uuid`.

### Query the silver layer

- **Output of the queries.** The counts match the Spark lab. The address query groups the users by state: the military addresses (`AE`, `AP`, `AA`, of the form `Unit ... Box ...`) have no city, which is why `missing_city` equals `users` for them. The orphan query returns 0: every order has a known user, so an inner join loses nothing.
- **dbt test replaced by the orphan query?** The `relationships` test (orders.user_id references users.user_id), written as `assert_orders_have_users.sql` in `lab-dbt`. To run it at every refresh: keep it as a dbt test on the gold models (dbt can target Trino with `dbt-trino`), or as an assertion run by the orchestrator after each load.
- **No job, no session: when to choose Trino?** Trino is a long-running SQL service: a query is sent over HTTP and answered in seconds, without starting a driver or downloading packages. It fits interactive and BI queries; Spark fits long, programmatic or very large transformations.

### Iceberg metadata

- **Snapshots of `silver.orders`, and which engine?** 3 snapshots, all written by Spark (the initial `overwrite`, then two `append` of the Iceberg lab). Trino only reads the metadata files written by Spark, located through Polaris.
- **`aws s3 ls` versus `$files`?** `aws s3 ls` lists every object of the folder, including files of older snapshots or failed writes, and needs the S3 keys. `$files` lists only the data files of the current snapshot, read from the manifests: Trino gets them from the metadata, the client of the query needs no access to the bucket.

### Gold layer

- **LEFT JOIN versus inner join?** `user_activity` must keep every user, including those without orders (0 orders). `orders_by_state_month` counts orders: an inner join is enough as long as every order has a user. The inner join depends on the orphan query: if it returned orphans, those orders would silently disappear from `orders_by_state_month`.
- **`current_date` in `age`?** The table is a snapshot of the day of its creation: `age` is wrong as soon as a birthday passes. The table must be rebuilt regularly (daily), or `age` computed at query time from `birthdate`.
- **Where do the gold files live?** Under `s3://<bucket>/warehouse/gold/<table>-<id>/`. Polaris decided it: the location is derived from the default base location of the catalog and the namespace, not chosen by Trino.

### Refresh

- **Snapshots after the refresh?** 3: `append` (CTAS), `delete` (the `DELETE` removes every row, Trino commits it as a delete operation), `append` (the `INSERT`).
- **A dashboard between `DELETE` and `INSERT`?** It sees an empty table: each statement is its own commit. A `MERGE` applies the updates and inserts in one commit, so readers see either the old or the new content, never an empty table.

### Compare with Spark

- **What each engine needs.** Trino: a running cluster with the catalog configured once by the administrator; the client only needs a host, a port and a user. Spark: a session per job, the Iceberg runtime package, the S3 and Polaris credentials in the session, and a job submission.
- **Choices.** Dashboard every minute on gold: Trino (low latency, many concurrent queries). Nightly 10 TB transformation of bronze: Spark (fault tolerance, retries per task, large shuffles to disk). Data scientist exploring silver with a query: Trino (immediate answer, plain SQL); Spark if the exploration continues in Python.
- **No fault tolerance.** The queries of the lab last seconds: re-running a failed one costs nothing. A 10 TB transformation lasts hours: losing a worker would restart everything, so it needs fault-tolerant execution or Spark, which retries only the failed tasks.

## Questions, lab 3: star schema

- **Are the checks sufficient?** No. They check counts, sums and that each key exists, not that the key is the right one. Undetected defects: an order linked to another user with the same quantity, two orders swapped between users, a duplicated `order_id` compensated by a missing one. Extra checks (in `07_star_validate.py`): uniqueness of `order_id` in the fact and of `user_id` in `dim_user`, and a row-by-row comparison of user, product and quantity against `silver.orders`.
- **Grain?** `fact_orders`: one row per order. `dim_date`: one row per calendar day.
- **Duplicated `user_id` in `silver.users`?** `dim_user` gets two keys for the user, the lookup of its orders matches twice: `silver_orders == fact_orders` fails (and the units sum). **Order without product (`NULL`)?** The lookup finds no `product_key`, the row is kept with a `NULL` key: the `unmatched == 0` assertion fails.
- **Rebuilding `dim_user` after a new user?** `row_number() OVER (ORDER BY user_id)` renumbers: every user whose `user_id` sorts after the new one gets a new `user_key`. `fact_orders` then points to wrong users unless it is rebuilt at the same time. Stable keys need to be kept from one build to the next (lookup of existing keys, or a hash of `user_id`).
- **User who moves to another state?** `dim_user` is overwritten (type 1): past orders show the new state. Keeping the history needs a slowly changing dimension type 2: one row per version of the user, with validity dates (`valid_from`, `valid_to`, `is_current`) and a surrogate key per version, the fact pointing to the version valid at the order date.
- **`orders_by_state_month` versus the star?** The aggregate answers its one question, fast, with no join. The star answers any combination of its dimensions (state, product, day of the week, age, ...) without a new table, at the cost of joins and of scanning the fact table at each query.

## Exercises

1. **Partitioning** (`ex1_partitioning.py`): the unpartitioned table has 2 data files (the CTAS and the row written by the `MERGE` of exercise 3), the partitioned one 4, one per month (`month_month` 600 to 603 = January to April 2020, 17, 13, 11 and 10 rows). With a filter on February, `EXPLAIN ANALYZE` reads 51 input rows from the unpartitioned table and 11 from the partitioned one: partition pruning skips the other months' files.
2. **Cohorts** (`ex2_cohorts.py`): 11 decades of birth, from 1910 (1 user, 47 orders) to 2020 (5 users, 288 orders); between 2.79 (1980s) and 3.19 (1910s) units per order.
3. **MERGE** (`ex3_merge.py`): one order of 4 croissants inserted in `silver.orders` (2,834 rows), the `MERGE` updates only the changed `(state, month)` row. Units: 8,632 now, 8,628 `FOR VERSION AS OF` the snapshot before the merge, 8,632 in silver.
4. **Optimize** (`ex4_optimize.py`): no change, the same 3 snapshots and 1 data file before and after. The full `DELETE` was a metadata-only delete (no delete file), and the `INSERT` wrote a single file: there was nothing to compact. On a table with many small files or delete files, optimize rewrites them into larger files and commits a `replace` snapshot without changing the rows.
5. **Spark** (`ex5_spark_compare.py`, run with `run_ex5.sh`): the same aggregation in Spark, through the same Polaris catalog, with `spark.sql.session.timeZone=UTC` to cut the months like Trino. Spark: 51 rows, 8,632 units; the gold table written by Trino, read back by Spark: 51 rows, 8,632 units. The service image now ships Spark 4.3.0, for which no Iceberg runtime exists (`IncompatibleClassChangeError` on `SparkView`), so the script runs PySpark 4.1.1 with the S3A jars of the image. With `--driver-memory 1g` the service was killed (`OOMKilled`, limit 2000Mi); 512m is enough.

## Teardown

```bash
helm uninstall trino
helm repo remove trino
kubectl delete secret trino-polaris-oauth
kubectl delete configmap trino-polaris-config
```

## Utilisation de l'IA

J'ai utilisé l'IA (Claude) pour m'aider lors des difficultés rencontrées pendant ce lab : erreurs de déploiement et de configuration (Polaris, Trino, Spark), écriture des scripts et rédaction de ce README.
