# Lessons

Author: John Bergamini

The facts the kit relies on, in plain words, and what a real rebuild night taught. Each lesson names the design rule it produced.

## Helios facts

1. A database's partition layout is fixed at CREATE DATABASE. Sub-partitions cannot be added later, so a new layout means a new database and a copy.
2. ATTACH DATABASE takes a name, an optional new name (AS), an optional STORAGEID and COMPUTEID, and READ ONLY. AS creates a branch with its own STORAGE_ID. When two databases could share a name, only the STORAGE_ID tells them apart.
3. DETACH DATABASE waits while writers are active. DETACH ... KILL ends the blocking work and detaches after the upload. DETACH ... FORCE can lose unsynced data; the kit never uses it.
4. A database has one read-write attachment and may have several read-only ones. DETACH followed by ATTACH ... READ ONLY stops every writer, for every account, within minutes. The Portal does the same.
5. MV_CLOUD_BRANCHES lists every storage with its STORAGE_ID and whether it is detached. Its database names are not unique.
6. A branch and its parent are independent: dropping one leaves the other intact.
7. The Management API has no attach, detach, branch or read-only operation. Admin SQL or the Portal is the path.
8. CREATE DATABASE and DROP DATABASE are privileges of their own; CREATE and DROP do not cover them.
9. SUPER cannot be granted. SHOW CREATE PIPELINE ... EXTENDED returns the definition, the CONFIG and the ALTER PIPELINE ... SET OFFSETS and SET OFFSET CURSOR statements as extra rows, but hides CREDENTIALS from every identity, the workspace owner included.
10. An account without pipeline privileges sees no pipelines at all. Without PROCESS an account sees only its own sessions.
11. A global REVOKE applies to new connections only. Existing sessions keep their rights until they are killed. KILL CONNECTION reaches only the aggregator the killer is connected to.
12. A client killed in the middle of an INSERT leaves a prepared transaction holding a leaf table lock for 15 to 20 minutes. No process list shows it and no SQL statement clears it.
13. Inserting millions of rows into a columnstore table with a large varchar unique key takes a table-level lock on the leaf. Parallel chunks into such a table collide with error 1205.
14. CREATE TABLE ... LIKE copies the AUTO_INCREMENT attribute but not the counter. AGGREGATOR SYNC AUTO_INCREMENT syncs the counter after it is set.
15. TABLES.DATA_LENGTH is zero on Helios. Table size comes from MV_COLUMNSTORE_FILES joined to TABLE_STATISTICS on the leaf master partitions.
16. Attaching needs compute cores times 8 at or above the partition count.
17. query_parallelism_per_leaf_core is global, applies only to databases with sub-partitions, and is picked up by new connections.
18. Reference throughput with 4 workers on an S-16 at 2x: 13.1 million rows scanned and 339 thousand rows inserted per second per worker; 1.43 billion rows and about 490 GiB per hour in aggregate.

## Run lessons

1. The freeze is a database state, not a grant state. On the reference night the application kept writing for three and a half hours through revokes and kill loops: grants were global, sessions opened before a revoke kept their rights, something restored grants twice, and a second application account was never on any list. The read-only attachment stopped every writer in minutes once it was used. Rule: Path A is the default freeze, proved by a write probe; the lockout is the fallback.
2. Enumerate every writer before the night. Accounts with write rights come from three privilege views and can be global, database or table grants; connected users come from sampling the process list. Rule: P3 captures accounts and sessions, the lockout snapshots every scope, and the writer watch runs with PROCESS from the start.
3. A watcher that samples once a minute misses fast single-row writes. Rule: the watch samples every 5 seconds and also compares row counts.
4. The switch ran out of order. Four statements were pasted from separate blocks under time pressure, ATTACH ... AS succeeded while the source was still attached (it made a branch instead of failing), and the original became reachable only by its storage id. Rule: the switch is a state machine whose every statement sits between checks, refuses anything out of order, addresses storage by STORAGE_ID, and attaches the retained copy before the live name.
5. Pipelines were not carried over. The inventory had redacted every literal, the copy account could not see pipelines, and the plan to read them live was dropped without a replacement. Rule: pipelines are part of the database; definitions are captured with the owner identity and a credentials placeholder, offsets are captured after the stop, and the switch refuses until the replay is verified.
6. Never hard-stop the copy. A killed runner held a leaf lock for 17 minutes. Rule: stop with the stop file or the deadline only; the rehearsal includes a kill test so the wait is known; a locked table has a fresh-table fallback.
7. Large varchar unique keys serialize inserts. Rule: a table that hits a lock wait is serialized and probed with a DELETE that matches no row and a short lock_wait_timeout.
8. Schema drifts during the day. Columns appeared between the morning capture and the window. Rule: the copy uses the inventory captured after the freeze and refuses definitions that differ.
9. Privileges were discovered one refusal at a time. Rule: P2 checks every privilege the runner needs, by name, before the night; the owner identity does grants, attachments and pipelines.
10. Verification has to fit the window. Per-bucket counts were too slow for billion-row tables and a count cannot see an in-place change. Rule: counts for all tables, newest values where a time column exists, grouped fingerprints within a time budget, and a verified-at time per table that the switch compares with the freeze time.
11. Commands must be unambiguous at 4 a.m. Rule: one ordered block per admin step, numbered steps with a pass or fail gate, warnings next to the step they belong to, and stop at the first failure with the observed state.
12. Keep the critical path on the kit's own connections. Tools that time out, lose their sign-in or reach only one aggregator cost hours. Rule: the kit drives the database through its own long-lived connections, or the owner notebook it generates, and journals every statement and gate.
