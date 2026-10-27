# Coverage grid: what the kit migrates

Author: John Bergamini

Status per item: Yes (release 1.0), New (built in the release 1.0 pass), No (an alternative is given). "Carried by LIKE" means `CREATE TABLE ... LIKE` reproduces it in the replacement. The grid is the state of the work order of October 27, 2026; the documentation pass of release 1.0 completes it item by item as the live ladder proves each one.

## Tables and storage

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| Columnstore sharded table | Yes | Hash-bucket chunks, `INSERT ... SELECT`, count proof | |
| Rowstore sharded table | Yes | Same path; format test from a READ ONLY source | |
| Reference table, columnstore | Yes | Counted with `COUNT(*)` at P3 and O4 (TABLE_STATISTICS reports no rows for it); one bucket; staging table on the replacement, then insert into the reference table | |
| Reference table, rowstore | Yes | Same count and staging path | |
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
| JSON, BSON, GEOGRAPHY, GEOGRAPHYPOINT, VECTOR, BLOB | Yes | `INSERT ... SELECT`; fingerprint hashes supported types only, others are count and newest | |
| Persisted computed columns | Yes | Carried by LIKE; excluded from the insert column list; the engine recomputes | |
| Column defaults and NOT NULL | Yes | Carried by LIKE | |
| AUTO_INCREMENT column | Yes | Carried by LIKE | |

## Keys, indexes, counters

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| PRIMARY, UNIQUE, SHARD KEY, SORT KEY | Yes | Carried by LIKE; keys drive the chunk predicate | |
| Hash indexes, skiplist indexes | Yes | Carried by LIKE | |
| FULLTEXT index | Yes | Carried by LIKE; rebuilt by the engine on insert; set B proves the query path | |
| Vector index | Yes | Carried by LIKE; set B proves it | |
| AUTO_INCREMENT counter, sharded table | Yes | `AGGREGATOR SYNC AUTO_INCREMENT ... ALL`; ALTER is refused (1749) and skipped | |
| AUTO_INCREMENT counter, reference or rowstore | Yes | ALTER to the larger of source counter and MAX+1, then sync | |
| Projections | No | Not captured | Recreate after O10 on the live name; planned as roadmap item R6 |

## Views

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| Views, including views on views | Yes | Created in rounds until no progress | |
| View DEFINER | Yes | Kept when the account exists, else the owner; 1227 fallback recorded per view | |
| View naming the source database | Refused | P3 refuses with the view name | Edit the view to use unqualified names before the window |

## Routines

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| Functions (UDF) and table-valued functions | New | Captured with the owner session; created at O5 after the tables | `objects.routines.ignore` per name |
| Aggregates (UDAF) | New | `SHOW AGGREGATES`; created after their functions | same |
| Stored procedures | New | Created at O7 after the views | same |
| External functions | No | Not captured | Recreate by hand on the live name after O10 |

## Pipelines

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| Kafka, S3, GCS, Azure, HDFS, filesystem pipelines | Yes | Captured with offsets at O2; created stopped at the recorded offsets at O9; started at O11 (`all` or `none`) | |
| Pipeline CREDENTIALS | Yes | Never readable from the server; the customer supplies them, the kit asks by field and stores them in the Keychain | Route `customer`: the customer runs the placeholder file |
| Secrets inside CONFIG | Yes | Redacted at capture, filled from the same credential at O9 | |
| Pipeline INTO PROCEDURE | New | O9 runs after the procedures exist | |
| Pipelines with WITH TRANSFORM | Yes | Part of the captured CREATE statement | The transform binary must be reachable from the workspace |
| Pipelines reading a LINK | No | LINK objects hold credentials and are not captured | Create the LINK on the replacement before O9 |
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

## History and recovery

| Item | 1.0 | How | If not migrated |
|---|---|---|---|
| The original database | Kept | Attached read only under the retained name on its original storage (a `resume` record of the original's timeline); deleted only by C5 after `retain_days` with two confirmations | |
| The original's own detached record | Removed at O10 S2b | Attached by its own syntax while the live name is free, checked, dropped; the data stays in the retained copy (proven October 27) | A run from an older kit: the C3 repair block, about 30 seconds with the live name detached |
| The replacement name | Removed at O10 S4 | Dropped by its own record after S3's checks; the live branch is independent of it; recoverable with `ATTACH DROPPED DATABASE` for Helios's retention period | |
| Point-in-time history of the original | Kept on the original storage | The live database starts a new history from the switch | Keep the retained copy for the retention period |
| Backups of the original | Kept | Unaffected | |
| Rollback before O11 | Yes | By branch record; before S2b the original record re-attaches by its syntax, after S2b a branch of the retained copy takes the live name; refused after application writes | After writes: a new freeze and copy from the retained copy |

## Not inspected

Any object kind not in this grid is not migrated. The P3 inventory lists what it sees under "not inspected" so the gap is visible before the window.
