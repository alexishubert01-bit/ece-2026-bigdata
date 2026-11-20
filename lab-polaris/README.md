# Lab: Govern Iceberg tables with Polaris

Environment: Onyxia `vscode-pyspark`, PySpark 4.1.1, Iceberg runtime 1.11.0, Polaris 1.8.0 deployed by `lab-polaris-deploy`. The notebook cells of the statement are run as scripts with `spark-submit`.

## Project layout

| File | Content |
|---|---|
| `spark_session.py` | Session with S3A, the Hadoop catalog `lab` and the REST catalog `polaris`; the principal comes from `POLARIS_PRINCIPAL` |
| `run.sh` | Runs a script in the background with `spark-submit`, waits, prints the output |
| `silver.py` | Silver transformations of the Spark lab (used to build `lab.silver.users`) |
| `00_prereq_users.py` | Creates `lab.silver.users`, expected by the lab |
| `01_register.py` | Compares the two catalogs, registers `lab.silver.orders` in Polaris |
| `02_dev_tables.py` | As `dev`: creates `polaris.silver.users`, adds a column |
| `03_analyst.py` | As `analyst`: reads, then tries an INSERT and an ALTER |
| `04_check_snapshots.py` | As `dev`: checks the denied INSERT left no snapshot |
| `05_exercise1_drop_column.py` | Exercise 1: drops the column, compares snapshots and files |
| `exercise2_setup.sh` | Exercise 2: catalog role `silver_reader`, principal role `silver_analyst`, principal `reader` (Management API) |
| `06_exercise2_gold.py`, `07_exercise2_reader.py` | Exercise 2: `gold` namespace as `dev`, then the checks as `reader` |
| `08_cleanup.py` | Drops the tables created in Polaris, keeps `silver.orders` |
| `warehouse/` | Metadata (no data files) of the Iceberg tables, synced from the bucket |

## How to run

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE" POLARIS_URI="http://polaris:8181" POLARIS_CATALOG="$LAB_BUCKET_NAME"
set -a; . /home/onyxia/.polaris-credentials.env; set +a   # credentials of dev, analyst and reader, outside the repository
POLARIS_PRINCIPAL=dev ./run.sh 01_register.py
```

The principal credentials are never committed.

## Differences with the lab statement

- `lab.silver.users` did not exist (the users part of the Spark lab was not done), so `00_prereq_users.py` creates it from the bronze layer.
- `lab.silver.orders` had evolved in the Iceberg lab: it is at `v4` (not `v1`), has a `source` column and 2,833 rows (2,829 orders, 3 appended, 1 inserted), not 5,000. `v4` is the version registered.
- The shell has no `~/.aws` profile: the `AWS_*` variables are exported directly.

## Results

| Step | Observed result |
|---|---|
| `SHOW NAMESPACES` | `lab` lists `silver`; `polaris` is empty before the registration |
| Registration | `silver.orders` visible in Polaris: 2,833 rows, 3 data files; 14 objects under `warehouse/silver/orders/` before and after |
| `polaris.silver.users` | 50 rows, created through Polaris under `warehouse/silver/users/` |
| Schema change as `dev` | `country` added; recorded in `00001-...metadata.json` of `users`, `latest_schema_id` still 0 |
| `analyst` read | `silver.orders` readable (group by product works) |
| `analyst` INSERT | `Forbidden`, operation `ADD_TABLE_SNAPSHOT`; no snapshot created (3 snapshots, 2,833 rows), no orphan data file (3 files) |
| `analyst` ALTER | `Forbidden`, operation `ADD_TABLE_SCHEMA` |
| Exercise 1 | 1 snapshot and 1 data file before and after; new metadata file `00002-...` |
| Exercise 2, `reader` | `silver` listed and readable; `gold` listing and reading `Forbidden`; catalog-level `SHOW NAMESPACES` `Forbidden` |

## Questions

### Connection

- **Which component answered each `SHOW NAMESPACES`?** `lab` is the Iceberg Hadoop catalog running in the Spark session: it lists the directories of the bucket. `polaris` is the Polaris service, called through REST. The results differ (`silver` versus nothing at first), which shows two independent sources.
- **Where is the credential used?** In the session configuration, but it is only sent to Polaris, in the OAuth client credentials request that returns a bearer token. It is not a bucket credential: S3 is accessed with the `AWS_*` keys.

### Registration

- **What did `register_table` write?** An entry in Polaris (a table pointing to `v4.metadata.json`). Nothing in the bucket: 14 objects before and after.
- **Why is it dangerous to keep writing through `lab`?** Each catalog keeps its own pointer to the current metadata. A write through `lab` moves the Hadoop pointer and leaves Polaris on the old version: the two histories diverge.

### Tables created through Polaris

- **Where did Polaris store `silver.users`, and why?** Under the base location of the catalog, `warehouse/` + namespace + table, so `warehouse/silver/users/`. This is the same folder as the Hadoop table `lab.silver.users`: both tables share the directory (different file names, nothing overwritten), and nothing in the storage prevents two catalogs from pointing to the same location.
- **Registered versus created?** A registered table keeps its files where they are and Polaris only references them. For a created table, Polaris chooses the location, Spark writes the data and Polaris writes the metadata. Both are objects of the same bucket; the difference is who owns the pointer.

### Schema change

- **Which metadata file records it?** `00001-...metadata.json` of the `users` table, a file written at the commit. It is not the file `register_table` pointed to, which belongs to `orders` (`v4`).
- **Which privilege does Polaris check?** The denial of the analyst is reported for the operation `ADD_TABLE_SCHEMA` (and `ADD_TABLE_SNAPSHOT` for the INSERT). The exact privilege names are listed in the Polaris access control documentation and were not checked here.

### Analyst

- `catalog_reader` provides `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`: reading, no write.
- The denied INSERT created no snapshot: Spark aborts the write, and Polaris refuses the commit.

## Exercises

1. **Dropping `country`** is a metadata operation: Iceberg identifies columns by id, so the column is removed from the current schema in a new metadata file. No data file is read or rewritten (1 file before and after) and no snapshot is created, since the rows do not change.
2. **`silver_reader`**: a catalog role with `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES` and `TABLE_READ_DATA` granted on the namespace `silver` only, assigned to the principal role `silver_analyst`, itself assigned to the principal `reader`. `reader` reads the tables of `silver` without any table-level grant (the namespace grants apply to its tables), cannot list or read `gold`, and cannot list the namespaces at the root of the catalog.

## What Polaris enforces, and what it does not

Polaris checks permissions when a client lists, reads or commits through the catalog. On this storage there is no credential vending: every session uses the same S3 keys, which give full access to the bucket. Anyone holding those keys can read or write the files directly, bypassing Polaris. The role model governs the catalog, not the objects.

## Cleanup

`08_cleanup.py` drops `gold` and `silver.users` in Polaris (the data and metadata files stay in the bucket, no purge). `silver.orders` is kept for the next module. The `reader` principal, `silver_analyst` and `silver_reader` remain in the catalog.
