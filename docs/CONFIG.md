# Configuration

Author: John Bergamini

One YAML file, default `customer/config.yaml`, selected with `--config` or `S2REBUILD_CONFIG`. Unknown keys are refused with the closest valid name. Identifiers match `^[A-Za-z0-9_]+$` and are at most 64 characters; source, replacement and retained must differ; every run name must be unique across the configured databases; the window end must follow its start. The `window` section is optional: without it there is no abort deadline, O1 records none, and the copy, the switch and the lockout run without a time limit. `examples/config.example.yaml` holds every key with a placeholder value.

Each table below lists a key with its type, default, range, the steps that read it and what it does. "Required" means no default.

## Top level

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `kit` | string | required | `s2rebuild` | every command | must equal `s2rebuild` |
| `version` | integer | required | `1` | every command | schema version of the file |
| `customer` | string | `Customer` | any | P3, C2 | report label only; never in the static tree |
| `run.mode` | enum | `production` | `production`, `test` | O1 | production asks for a `databases[].replacement` or `retained` name the configuration leaves to the kit, with the date default as the suggestion, and records the answers in the journal; test applies the defaults silently; `run --mode` and `step --mode` override it |

## workspace

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `workspace.host` | string | required | a host name | every connection | the DML endpoint |
| `workspace.port` | integer | `3306` | 1 to 65535 | every connection | |
| `workspace.ca_file` | path | `certs/singlestore_bundle.pem` | a readable file | every connection | TLS with CA verification |
| `workspace.compute_size` | string or null | null | a label | C2, reports | stored with the baseline |
| `workspace.id` | string or null | null | the workspace id | owner-notebook, P2 | the notebook checks it against the attached workspace; the Management API read needs it |
| `workspace.management_api_key_keychain` | string or null | null | a Keychain item | P2 (`check`) | when set, P2 reads the workspace state (refused unless ACTIVE), the auto-suspend setting and the autoscale setting through `GET /v1/workspaces/<workspace.id>` and prints them on the plan sheet; O1 refuses while autoscale is on unless `--accept-autoscale NOTE`; without a key P2 prints the three workspace questions and O1 records the answer from `--workspace NOTE`; the key never prints |
| `workspace.keepalive_minutes` | integer | `15` | 0 to 60 | every step the kit runs | a thread sends `SELECT 1` on its own runner connection every this many minutes while the step runs, so auto-suspend sees activity; 0 disables it; nothing runs between commands (STEPS P4) |

## connections

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `connections.runner.user` | string | required | identifier | every runner step | created at P1, dropped at C4 |
| `connections.runner.password_keychain` | string | one of the two | a Keychain item | every runner step | P1 (`account-sql --execute`) generates the password to the workspace policy and stores it there |
| `connections.runner.password_env` | string | one of the two | a variable name | every runner step | the variable holds the password; nothing is generated |
| `connections.owner` | mapping or absent | absent | `user` plus `password_keychain` or `password_env` | P2, P3 pipelines and routines, O2, O3, O5 routines, O7 views and procedures, O9, O10, O11, C3, C4, lockout, cleanup, engine-capture, sample-database | when absent, admin steps print blocks and wait for recorded gates, and routines, pipelines and other accounts' grants are not visible; with `password_keychain`, P1 asks for the password (hidden) when the item is missing and stores it |

## databases (list, at least one)

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `databases[].source` | identifier | required | the live name | every step | |
| `databases[].replacement` | identifier | `<source>_rebuild_<YYYYMMDD>` | differs from the others | P2, O5 to O10, C3 | when left out, O1 asks for it in `run.mode` production |
| `databases[].retained` | identifier | `<source>_retained_<YYYYMMDD>` | differs from the others | P2, O10, C3, C5 | when left out, O1 asks for it in `run.mode` production |
| `databases[].layout.partitions` | integer | required | 1 to leaf cores times 8 (P2 checks) | P2, O5, O10 | the target layout |
| `databases[].layout.sub_partitions` | integer | required | 0 and up | O5, O10 | the target layout |
| `databases[].time_column_candidates` | list | `[_modified_time, updated_at, modified_at, _created_time, created_at, event_date]` | column names | P3, O4, O7, O8 | the first match per table is the time column (tier 2, the value hash, the sample range when no sort key) |
| `databases[].app_managed_pipelines` | list | `[]` | pipeline names | O2, O9, C1 | stopped at O2, never replayed, excluded from the O9 count gate; C1 reports whether they came back |
| `databases[].tables.<table>.concurrency` | integer | none | 1 and up | O6 | chunks of the table in flight at once; overrides `copy.table_concurrency` |
| `databases[].tables.<table>.chunk_rows` | integer | none | 1 and up | P3, O4, O6 | bucket size for the table; overrides `copy.chunk_rows` |
| `databases[].target.transforms` | list | `[]` | must stay empty | O5 | any entry is refused in 1.0 (the transform hook, roadmap R4) |

## objects

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `objects.routines.ignore` | list | `[]` | routine names: `name` for every database, `database.name` for one | P2, P3, O4, P4 plan sheet, O5, O7, O10 | named routines are left behind on purpose, never recreated; listed on the plan sheet; the default migrates every captured routine; a pipeline that loads into an ignored procedure refuses P3 and O4, since O9 would find no target |

## window (optional)

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `window.start` | UTC time | absent | ISO 8601 | P3, P4, O1 | the window; without `start` and `end` nothing has a time limit |
| `window.end` | UTC time | absent | after `start` | P3, P4, O1, O6, O10, lockout | |
| `window.restart_allowance_minutes` | integer | `60` | 0 and up | O1 abort deadline | time the customer needs to restart on the original |
| `window.buffer_minutes` | integer | `30` | 0 and up | O1 abort deadline | abort deadline = end minus allowance minus buffer minus the longest modeled chunk |

## freeze

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `freeze.mode` | enum | `readonly` | `readonly`, `lockout` | O3, O10, owner-sql O3 | `lockout` with more than one configured database is refused at P2 (check), run and step O3, O10 and O11; the rehearsal always freezes with `readonly` |
| `freeze.detach_wait_seconds` | integer | `120` | 0 and up | O3, O10 S1 | after this, the waiting DETACH is cancelled and issued with KILL when `detach_kill` is true |
| `freeze.detach_kill` | boolean | `true` | | O3, O10 S1 | KILL ends the blocking workload and detaches after the upload |
| `freeze.write_probe` | boolean | `true` | | O3 | a DELETE of a value that cannot exist must be refused |
| `freeze.quiet_max_age_seconds` | integer | `60` | 1 and up | O10 S0 and S1 on Path B | the last lockout pass must be quiet and no older |
| `freeze.lockout.connections` | integer | `6` | 1 and up | lockout | held connections, to reach several aggregators |
| `freeze.lockout.interval_seconds` | integer | `5` | 0 and up | lockout | seconds between passes; 0 is a tight loop |
| `freeze.lockout.protected_accounts` | list | `[]` | grantees | P3, lockout, quiet-proof | never revoked; runner and owner are always protected |
| `freeze.lockout.quiet_passes_required` | integer | `3` | 1 and up | lockout | passes with zero threads before the quiet proof |

## copy

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `copy.workers` | integer | `4` | 1 to 32 | P2 (printed beside the leaf cores and the baseline's worker count), P3 estimate, O6, O8 (the default of `verify.sessions`) | copy connections; from 4 to 12 workers the reference workspace stayed near 3.5 B rows per hour, so a performance test decides (roadmap R3) |
| `copy.chunk_rows` | integer | `10000000` | 1 and up | P3, O4, O6 | target rows per bucket |
| `copy.min_rows_for_chunking` | integer | `5000000` | 0 and up | P3, O4, O6, sample-database | smaller tables are one bucket; the sample copies them whole |
| `copy.max_buckets_per_table` | integer | `32` | 1 and up | P3, O4, O6 | every bucket scans the whole table; the cap bounds the scan cost |
| `copy.table_concurrency` | `auto` or integer | `auto` | `auto`, or 1 to `copy.workers` | P3 estimate, O6 | auto: one table takes at most half the workers above 8, and 1 after a lock wait; an integer caps every table at that many chunks in flight, with no serialization after a lock wait |
| `copy.lock_probe_timeout_seconds` | integer | `5` | 1 and up | O3 write probe, O6 lock probe | the probe before a chunk on a table that had a lock wait |
| `copy.lock_retry_seconds` | integer | `120` | 1 and up | O6 | park time after a lock wait |
| `copy.max_attempts` | integer | `3` | 1 and up | O6 | per chunk; a lock wait and error 1997 consume none |
| `copy.max_lock_waits` | integer | `10` | 1 and up | O6 | lock waits of one chunk in a run; then it fails with the fresh-table command |
| `copy.max_consecutive_failures` | integer | `6` | 1 to 100 | O6 | the circuit breaker: this many chunk failures with no success between them (error 1997 counted, lock waits not) stop new chunks; the running ones finish and O6 ends INCOMPLETE (exit 2) with "chunks are failing in a row; nodes may be restarting, see MV_EVENTS; rerun with --retry-failed when the workspace is steady" |
| `copy.progress_seconds` | integer | `60` | 1 and up | O6 | progress line interval |

## verify

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `verify.tiers` | list | `[count, newest, fingerprint]` | a subset, in that order | O7, O8 | the O7 stale check uses count and newest |
| `verify.sessions` | integer or null | `null` (`copy.workers`) | 1 to 32 | O8 | tier 3 connections: the two sides of a table at the same time, several tables at once, largest first |
| `verify.full_fingerprint_max_rows` | integer | `1000000000` | 0 and up | O8 | a table with more rows (the journal's tier 1 count, else the estimate) is sampled: a contiguous range on its first sort key column or its time column, the same predicate on both sides; over the threshold with neither column it gets a full fingerprint with a note |
| `verify.sample_percent` | integer | `5` | 1 to 100 | O8 | the percent of the value span the sample covers from its minimum; 100 is a full fingerprint; `step O8 --fingerprint TABLE --percent N` overrides it for one table |
| `verify.fingerprint_budget_minutes` | integer or null | `null` (no deadline) | 1 and up | O8; P3 and P4 estimate | an elapsed deadline from the start of tier 3; a table whose turn comes after it is recorded not fingerprinted and O8 passes only with `--yes` |
| `verify.fingerprint_groups` | integer | `64` | 1 to 4096 | O8 | groups for single-bucket tables |
| `verify.value_columns` | integer | `4` | 0 to 16 | O8 | the value hash takes the time column plus up to this many supported columns, always; JSON, BSON, GEOGRAPHY, VECTOR and binary types are left out |

## pipelines

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `pipelines.route` | enum | `kit` | `kit`, `customer` | P3, O9 | who replays the definitions: the kit from the capture, or the customer from the placeholder file |
| `pipelines.credentials_keychain` | mapping | `{}` | source type to Keychain item (`S3: s2rebuild-pipelines-s3`) | P3, O9 | the item the credential prompt fills (by field per source type) and O9 reads before the variables; a type without an entry is not asked and keeps its variable |
| `pipelines.per_pipeline_keychain` | mapping | `{}` | pipeline to Keychain item | O9 | read before the source-type item |
| `pipelines.credentials_env` | mapping | `{KAFKA: S2REBUILD_KAFKA_CREDENTIALS, S3: S2REBUILD_S3_CREDENTIALS}` | source type to variable name | O9, owner-notebook | the fallback when no Keychain item holds the value; exported for O9 only |
| `pipelines.per_pipeline_env` | mapping | `{}` | pipeline to variable name | O9, owner-notebook | overrides the source-type entry |
| `pipelines.retention_hours` | mapping | `{}` | source type to hours | P4 plan sheet, O11 | the customer's stated retention, for the gap check |
| `pipelines.start_at_release` | enum | `all` | `all`, `none` | O11, C1 | none leaves the recreated pipelines stopped; C1 reports them and skips the offset sample |

## estimate

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `estimate.baseline` | path or null | `customer/baseline.json` | a file | P3, P4, C2 | refit by C2; carries the rates, workers, compute label and leaf cores |
| `estimate.band_percent` | integer | `15` | 0 to 100 | P3, P4 | the planning band, not a confidence interval |
| `estimate.scale_up_cap` | number | `1.0` | 1.0 to 4.0 | P3, P4, O4 | the most the scan and insert rates are scaled up for a workspace with more leaf cores than the baseline's (1.0 is no credit); fewer leaf cores always scale them down in proportion; no scaling while the baseline records no leaf cores |

## closeout

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `closeout.retain_days` | integer | `90` | 1 and up | C2, C5 | C2 records `retain_until_utc`, the switch time plus this; `s2rebuild cleanup --delete` refuses before it; a value under 90 prints a line on the plan sheet; no key makes a run delete the retained copy |
| `closeout.flexible_parallelism` | number or null | `0.5` | a positive number, or null | O11 | `SET GLOBAL query_parallelism_per_leaf_core`; null skips it |

## paths

| Key | Type | Default | Range | Read by | Logic |
|---|---|---|---|---|---|
| `paths.runs_dir` | path | `customer/runs` | a writable folder | every command | one folder per run; everything a run writes stays under its parent folder; the rehearsal writes under `<run>/rehearsal/` |

## Command line flags

Generated by `s2rebuild docs-flags`; a test keeps this table equal to its output.

<!-- docs-flags:start -->
| Command | Flag | Default | Description |
|---|---|---|---|
| (global) | `--config PATH` |  | configuration file (default: $S2REBUILD_CONFIG or customer/config.yaml) |
| (global) | `--run ID` |  | run id (default: $S2REBUILD_RUN, else the most recent run; a new run starts only with run --new, or when no run exists yet) |
| (global) | `--db NAME` |  | restrict to one configured source database; repeatable |
| (global) | `--dry-run` |  | print every statement, open no connection where the step allows it, write nothing |
| (global) | `--yes` |  | no interactive confirmation |
| (global) | `--verbose` |  | more detail in the log |
| (global) | `--accept-config-change NOTE` |  | go on although the configuration changed after O1: the journal records both hashes and the note, then keeps the new hash |
| check |  |  | (global flags only) |
| account-sql | `--execute` |  | run the block with the owner profile |
| account-sql | `--drop` |  | the DROP USER block and the zero-privilege check (C4) |
| profile | `--exact-counts` |  | run COUNT(*) per table instead of estimates |
| profile | `--baseline FILE` |  | throughput baseline file to use instead of the configured one |
| profile | `--all-databases` |  | add a system sheet with every database in the workspace for ranking and sequencing |
| profile | `--inventory DIR` |  | read a saved capture instead of connecting (required for --dry-run without a prior capture) |
| profile | `--out FILE` |  | workbook path inside the customer folder (default: the run reports folder) |
| profile | `--step` | P3 | P3 profile before the window (default) or O4 freeze capture with drift |
| profile | `--redo` |  | capture again although the step passed; logs what it rewrites (O4: the copy plan) |
| plan | `--inventory DIR` |  | read a saved capture instead of the run inventory |
| plan | `--baseline FILE` |  | throughput baseline file to use instead of the configured one |
| rehearse | `--keep` |  | leave the throwaway databases for inspection |
| rehearse | `--prefix NAME` |  | rehearsal database prefix, starting with s2rebuild_rehearsal_ (default s2rebuild_rehearsal_<run id>) |
| rehearse | `--rows N` | 1000000 | rows in the keyed and keyless tables (default 1000000) |
| rehearse | `--cleanup` |  | drop the rehearsal databases of the prefix and exit (asks first unless --yes) |
| rehearse | `--from-database NAME` |  | build the rehearsal source as a sample of this database (sample-database) instead of the synthetic tables |
| rehearse | `--percent N` | 1 | with --from-database: the sample percent (default 1) |
| rehearse | `--start-pipelines` |  | start the recreated pipelines at the rehearsed O11 (default: created stopped, never started) |
| sample-database | `SOURCE` |  | the database to sample |
| sample-database | `NAME` |  | the sample to create; must not be a database of the configured run |
| sample-database | `--percent N` |  | the rows to take: CRC32 of the key MOD 100 under N (1 to 100; 100 copies every table whole) |
| sample-database | `--tables LIST` |  | comma-separated tables that get rows (every table is still created) |
| run | `--from ID` | O1 | first step to run (default O1) |
| run | `--to ID` | O11 | last step to run (default O11) |
| run | `--redo` |  | run steps that already passed |
| run | `--retry-failed` |  | O6: also rerun chunks parked after copy.max_attempts failures |
| run | `--no-rehearsal NOTE` |  | O1 only: start without rehearsal evidence; the note is recorded as an event and in the O1 note |
| run | `--workspace NOTE` |  | O1 only: the answers to the three workspace questions of P2 (ACTIVE, autoscale off, auto-suspend off or a keep-alive job); recorded as evidence of the go |
| run | `--accept-autoscale NOTE` |  | O1 only: go on although P2 read autoscale on; the note is recorded |
| run | `--mode` |  | O1 only: override run.mode; production asks for a replacement or retained name the configuration leaves to the kit, test applies the defaults |
| run | `--new` |  | start a new run at O1 instead of resuming the most recent one; with --run ID the new run takes that id, which must not exist yet |
| step | `ID` |  | step id such as O6 |
| step | `--redo` |  | run the step even when it already passed |
| step | `--fresh-table TABLE` |  | O6 only: load one table with the fresh-table fallback |
| step | `--retry-failed` |  | O6: also rerun chunks parked after copy.max_attempts failures |
| step | `--no-rehearsal NOTE` |  | O1 only: start without rehearsal evidence; the note is recorded as an event and in the O1 note |
| step | `--workspace NOTE` |  | O1 only: the answers to the three workspace questions of P2 (ACTIVE, autoscale off, auto-suspend off or a keep-alive job); recorded as evidence of the go |
| step | `--accept-autoscale NOTE` |  | O1 only: go on although P2 read autoscale on; the note is recorded |
| step | `--mode` |  | O1 only: override run.mode; production asks for a replacement or retained name the configuration leaves to the kit, test applies the defaults |
| step | `--idle-workload NOTE` |  | C1 only: pass without application writes seen; why the live database is idle |
| step | `--fingerprint TABLE` |  | O8 only: run tier 3 for this table only, sampled (repeatable; the other tables keep their recorded result) |
| step | `--percent N` |  | O8 with --fingerprint: the sample percent (default verify.sample_percent) |
| step | `--full TABLE` |  | O8 only: run a full tier 3 fingerprint for this table only (repeatable) |
| gate | `ID` |  | step id such as O3 |
| gate | `--pass` |  | the gate passed |
| gate | `--fail` |  | the gate failed |
| gate | `--note TEXT` |  | what was observed |
| gate | `--evidence FILE` |  | file with the evidence (notebook output, query result) |
| gate | `--override NOTE` |  | C1 and C2 only: record the gate of a close-out step the kit could not pass, and why |
| status |  |  | (global flags only) |
| watch | `--interval N` | 5 | seconds between process list samples (default 5) |
| watch | `--tables N` | 10 | largest tables to count every 60 seconds (default 10) |
| watch | `--until TIME` |  | stop time, ISO 8601 UTC (default: until Ctrl-C) |
| quiet-proof |  |  | (global flags only) |
| lockout | `--until TIME` |  | stop time, ISO 8601 UTC (default: the abort deadline) |
| lockout | `--kill-only` |  | keep killing sessions without revoking or snapshotting grants |
| lockout | `--hold` |  | Path B: keep the loop running after the quiet proof until S1 of O10 has run (the source left its STORAGE_ID) |
| owner-sql | `--rollback` |  | O10 only: print the rollback block R1 to R3 instead of the forward block |
| owner-sql | `ID` |  | admin step id: O2, O3, O9, O10, O11, C3, C4 |
| owner-notebook |  |  | (global flags only) |
| mark-recopy | `TABLE` |  | table name as recorded in the journal |
| mark-recopy | `--bucket N` |  | one bucket instead of every bucket of the table |
| pause |  |  | (global flags only) |
| resume |  |  | (global flags only) |
| abandon | `--execute` |  | run the block with the owner profile (needs --note) |
| abandon | `--note TEXT` |  | why the run is abandoned; recorded in the journal |
| report |  |  | (global flags only) |
| calibrate | `--journal FILE` |  | journal to fit (default: the run journal) |
| calibrate | `--until TIME` |  | use chunks that ended before this ISO 8601 UTC time |
| calibrate | `--size-gib N` |  | total primary files GiB copied, for the GiB per hour figure |
| calibrate | `--all-attempts` |  | include re-copied chunks (default: first-pass chunks only) |
| cleanup | `--delete NAME` |  | the retained name to delete for good: refused before the retain-until date, then the name is typed again at the prompt (owner profile) |
| bundle | `--verify ZIP` |  | unzip to a temporary folder and compare hashes with the manifest |
| bundle | `--include-customer` |  | add customer/ to the zip |
| engine-capture | `--fixture-database NAME` |  | a database to read SHOW AGGREGATES in and to count ROUTINES and PIPELINES rows as the owner and as the runner |
| engine-capture | `--out FILE` |  | write the capture here instead of tests/engine/helios-<version>.json |
| docs-flags |  |  | (global flags only) |
<!-- docs-flags:end -->

## Environment variables

| Variable | Meaning |
|---|---|
| `S2REBUILD_CONFIG` | default configuration path |
| `S2REBUILD_RUN` | default run id for every command; export it after the P1 gate starts the run |
| the `password_env` names | runner and owner passwords when the Keychain is not used |
| the `pipelines.credentials_env` and `per_pipeline_env` names | pipeline CREDENTIALS JSON, exported for O9 only, when no Keychain item holds it |

Nothing else is read from the environment. The Keychain items (`password_keychain`, `pipelines.credentials_keychain`, `pipelines.per_pipeline_keychain`, `workspace.management_api_key_keychain`) are read with `security find-generic-password -a "$USER" -s ITEM -w` and written by the kit's prompts with `security add-generic-password -U`; no value is ever printed or logged.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | done |
| 1 | refused, or a configuration error |
| 2 | incomplete: rerun to resume (a connection reset by a node, the circuit breaker, a stop file) |
| 3 | verification mismatch |
| 4 | gate pending or failed: a person acts next |
