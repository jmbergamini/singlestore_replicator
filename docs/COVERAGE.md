# Coverage grid: what the kit migrates

Author: John Bergamini

Status per item: Yes (migrated since the first release), Built (new in the release 1.0 pass; proven on the test fake, with the live iteration of `docs/RELEASE-1.0.md` section 6 that proves it on Helios named in the How column), No (an alternative is given), Not needed (unaffected by the switch). "Carried by LIKE" means `CREATE TABLE ... LIKE` reproduces it in the replacement. The grid is final for release 1.0: every Built item is on branch `kit-1.0.0`.

## Tables and storage

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| Columnstore sharded table | Yes | Hash-bucket chunks, `INSERT ... SELECT`, count proof | |
| Rowstore sharded table | Yes | Same path; format test from a READ ONLY source | |
| Reference table, columnstore | Built | Counted with `COUNT(*)` at P3 and O4 (TABLE_STATISTICS reports no rows for it); one bucket on the staging path; staging table on the replacement, then insert into the reference table; the estimate charges its rows at the insert rate twice; marked on the plan sheet, `tables.csv` and the table tab (I1) | |
| Reference table, rowstore | Built | Same count and staging path; `storage_type` reads the ROWSTORE keyword (I3, set B) | |
| Keyless table | Yes | Row-hash chunks over all columns; duplicates preserved | |
| Empty table | Yes | Created, verified at count 0 | |
| Temporary and global temporary tables | No | Session scoped; nothing persists | The application recreates them |
| Partition and sub-partition layout | Changed | The point of the kit; `databases[].layout` | |
| Durability and sync flags | Yes | Must equal the source; O5 checks | |
| Table options (compression, autostats, comments) | Yes | Carried by LIKE | |
| Table and column names over 64 characters | Yes | Validated at P3; the fresh-table name is capped | |

## Columns and types

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| All scalar types, DECIMAL, DATETIME(6), ENUM, SET | Yes | `INSERT ... SELECT` | |
| JSON, BSON, GEOGRAPHY, GEOGRAPHYPOINT, VECTOR, BLOB | Yes | `INSERT ... SELECT`; the fingerprint's value hash takes the time column and up to `verify.value_columns` supported columns only, so these columns are count and newest (I3, set B) | |
| Persisted computed columns | Yes | Carried by LIKE; excluded from the insert column list; the engine recomputes | |
| Column defaults and NOT NULL | Yes | Carried by LIKE | |
| AUTO_INCREMENT column | Yes | Carried by LIKE | |

## Keys, indexes, counters

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| PRIMARY, UNIQUE, SHARD KEY, SORT KEY | Yes | Carried by LIKE; keys drive the chunk predicate; the first sort key column drives the sampled fingerprint range | |
| Hash indexes, skiplist indexes | Yes | Carried by LIKE | |
| FULLTEXT index | Yes | Carried by LIKE; rebuilt by the engine on insert; set B proves the query path (I3) | |
| Vector index | Yes | Carried by LIKE; set B proves it (I3) | |
| AUTO_INCREMENT counter, sharded table | Yes | `AGGREGATOR SYNC AUTO_INCREMENT ... ALL`; ALTER is refused (1749) and skipped | |
| AUTO_INCREMENT counter, reference or rowstore | Yes | ALTER to the larger of source counter and MAX+1, then sync | |
| Projections | No | Not captured | Recreate after O10 on the live name; planned as roadmap item R6 |

## Views

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| Views, including views on views | Yes | Created in rounds until no progress | |
| View DEFINER | Yes | Kept when the account exists, else the owner; 1227 fallback recorded per view; the Views tab shows the definer each view got | |
| View naming the source database | Refused | P3 refuses with the view name | Edit the view to use unqualified names before the window |

## Routines

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| Functions (UDF) and table-valued functions | Built | `information_schema.ROUTINES` and `SHOW CREATE FUNCTION` with the owner session (the runner sees none); created at O5 after the tables; gate: every routine present with its normalized definition equal to the capture (I2, set A) | `objects.routines.ignore` per name |
| Aggregates (UDAF) | Built | `SHOW AGGREGATES` and `SHOW CREATE AGGREGATE`; created at O5 after the functions they name (I2, set A) | same |
| Stored procedures | Built | `SHOW CREATE PROCEDURE`; created at O7 after the views; O10 S3 expects the ROUTINES count the O7 gate recorded on the live name (I2, set A) | same |
| Routine DEFINER | Built | Kept while the account exists and is not the runner, else the owner; 1227 fallback recorded per routine; the Routines tab shows the definer each routine got; C4 refuses a routine with the runner as definer (I2) | |
| Routine naming the source database | Refused | O5 or O7 refuses with the routine name before any statement | Edit the definition under `definitions/routines/` to use unqualified names |
| External functions | No | Not captured | Recreate by hand on the live name after O10 |

## Pipelines

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| Kafka, S3, GCS, Azure, HDFS, filesystem pipelines | Yes | Captured with offsets at O2; created stopped at the recorded offsets at O9; started at O11 (`all` or `none`) | |
| Pipeline CREDENTIALS | Built | Never readable from the server; the kit asks the customer by field at P3 (or O9) and stores the CREDENTIALS JSON in the Keychain item of `pipelines.credentials_keychain`; the environment variables stay the fallback (I3) | Route `customer`: the customer runs the placeholder file. A source type skipped at the prompt: its pipelines are left out of the gates and listed in the final report with the placeholder file |
| Secrets inside CONFIG | Yes | Redacted at capture, filled from the same credential at O9 | |
| Pipeline INTO PROCEDURE | Built | O9 runs after O7 created the procedures; P3 and O4 refuse a load target whose procedure is ignored or not captured (I2, set A) | |
| Pipelines with WITH TRANSFORM | Yes | Part of the captured CREATE statement | The transform binary must be reachable from the workspace |
| Pipelines reading a LINK | No | LINK objects hold credentials and are not captured | Create the LINK on the replacement before O9; roadmap R7 |
| Application-managed pipelines | Yes | Stopped at O2; listed in `app_managed_pipelines`; the application recreates them | |
| Pipeline offsets past broker retention | Detected | O11 reports the gap per partition | Edit the ALTER statement to EARLIEST or LATEST and record the decision |

## Security and workspace objects

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| Users and roles | Not needed | Workspace level; unaffected by the switch | |
| Grants on the database name | Not needed | The live name is unchanged, so grants by name still apply; Path B snapshots and restores revoked write privileges | |
| Grants on the replacement name during the window | Yes | Runner only; dropped at C4 | |
| Resource pools, workload management | Not needed | Workspace level | |
| `query_parallelism_per_leaf_core` | Yes | O11 sets it when `closeout.flexible_parallelism` is set | |
| Workspace state and settings | Built | P2 reads the state (ACTIVE required), autoscale and auto-suspend through the Management API when a key is configured; O1 refuses while autoscale is on; the keep-alive thread covers every step (I4, set C) | Without a key: the three questions of P2, answered at O1 with `--workspace NOTE` |

## History and recovery

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| The original database | Kept | Attached read only under the retained name on its original storage (a `resume` record of the original's timeline); deleted only by C5 (`cleanup --delete`) after `retain_days` with two confirmations (I1) | |
| The original's own detached record | Removed at O10 S2b | Attached by its own syntax while the live name is free, proved unchanged, dropped; the data stays in the retained copy (proven October 27; I1) | A run from an older kit: the C3 repair block, about 30 seconds with the live name detached |
| The replacement name | Removed at O10 S4 | Dropped by its own record after S3's checks; the live branch is independent of it; recoverable with `ATTACH DROPPED DATABASE` for Helios's retention period (I1) | |
| Point-in-time history of the original | Kept on the original storage | The live database starts a new history from the switch | Keep the retained copy for the retention period |
| Backups of the original | Kept | Unaffected | |
| Rollback before O11 | Built | By branch record; before S2b the original record re-attaches by its syntax (R2a), after S2b a branch of the retained copy takes the live name (R2b, or R2c when the server refuses a branch of a READ ONLY attachment); refused after application writes (I1 rollback leg, I4 set C) | After writes: a new freeze and copy from the retained copy |
| Verification of the copy | Built | Counts and newest values on every table; fingerprints in parallel, a table over `verify.full_fingerprint_max_rows` sampled as a contiguous range on its sort key or time column; one table rerun with `step O8 --full TABLE` (I3, I5) | A table past the budget is recorded not fingerprinted and accepted with `--yes` |
| Rehearsal | Built | P5 runs the real steps on a throwaway copy, synthetic or a sample of the real database, through the switch, a rollback leg and the end state (I1, I5) | |

## Not inspected

Any object kind not in this grid is not migrated. The P3 inventory lists what it sees under "not inspected" so the gap is visible before the window.
