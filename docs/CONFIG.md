# Configuration

Author: John Bergamini

One YAML file, default `customer/config.yaml`, selected with `--config` or `S2REBUILD_CONFIG`. Unknown keys are refused with the closest valid name. Identifiers match `^[A-Za-z0-9_]+$`; source, replacement and retained must differ; the window end must follow its start. The `window` section is optional: without it there is no abort deadline, O1 records none, and the copy, the switch and the lockout run without a time limit. `examples/config.example.yaml` holds every key with a placeholder value.

## Keys

| Key | Type | Default | Read by |
|---|---|---|---|
| `kit` | string | required, `s2rebuild` | every command |
| `version` | integer | required, `1` | every command |
| `customer` | string | `Customer` | P3, C2 (report label only) |
| `run.mode` | `production` or `test` | `production` | O1: production asks for a `databases[].replacement` or `retained` name the configuration leaves to the kit, with the date default as the suggestion, and records the answers in the journal; test applies the defaults silently; `run --mode` and `step --mode` override it |
| `workspace.host` | string | required | every connection |
| `workspace.port` | integer | `3306` | every connection |
| `workspace.ca_file` | path | `certs/singlestore_bundle.pem` | every connection (TLS with CA verification) |
| `workspace.compute_size` | string or null | null | C2 (label stored with the baseline) |
| `workspace.id` | string or null | null | owner-notebook (checked against the attached workspace); P2 (the Management API read needs it) |
| `workspace.management_api_key_keychain` | string or null | null | P2 (`check`): when set, the Keychain item holding a Management API key; P2 reads the workspace state (refused unless ACTIVE), the auto-suspend setting and the autoscale setting through `GET /v1/workspaces/<workspace.id>` and prints them on the plan sheet; O1 refuses while autoscale is on unless `--accept-autoscale NOTE`; without a key P2 prints the three workspace questions and O1 records the answer from `--workspace NOTE` |
| `workspace.keepalive_minutes` | integer 0 to 60 | `15` | every step the kit runs: a thread sends `SELECT 1` on its own runner connection every this many minutes while the step runs, so auto-suspend sees activity; 0 disables it; nothing runs between commands (STEPS P4) |
| `connections.runner.user` | string | required | every runner step |
| `connections.runner.password_keychain` | string | one of the two | every runner step (Keychain service name); P1 (`account-sql --execute`) generates the password to the workspace policy and stores it there |
| `connections.runner.password_env` | string | one of the two | every runner step (environment variable name) |
| `connections.owner` | mapping or absent | absent | P2, P3 pipelines, O2, O3, O9, O10, O11, C1, C3, C4, lockout; same keys as runner; with `password_keychain`, P1 (`account-sql --execute`) asks for the password (hidden) when the item is missing and stores it |
| `databases[].source` | identifier | required | every step |
| `databases[].replacement` | identifier | `<source>_rebuild_<YYYYMMDD>` | P2, O5 to O10, C3; when left out, O1 asks for it in `run.mode` production |
| `databases[].retained` | identifier | `<source>_retained_<YYYYMMDD>` | P2, O10, C3; when left out, O1 asks for it in `run.mode` production |
| `databases[].layout.partitions` | integer | required | P2, O5, O10 |
| `databases[].layout.sub_partitions` | integer | required | O5, O10 |
| `databases[].time_column_candidates` | list | `[_modified_time, updated_at, modified_at, _created_time, created_at, event_date]` | P3, O4, O7, O8 |
| `databases[].app_managed_pipelines` | list | `[]` | O2, O9, C1 |
| `databases[].tables.<table>.concurrency` | integer | none | O6 (chunks of the table in flight at once) |
| `databases[].tables.<table>.chunk_rows` | integer | none | P3, O4, O6 (bucket size for the table) |
| `databases[].target.transforms` | list | `[]` | O5; must stay empty in this pass (layout change only) |
| `window.start` | UTC time | absent | P3, P4, O1 |
| `window.end` | UTC time | absent | P3, P4, O1, O6, O10, lockout |
| `window.restart_allowance_minutes` | integer | `60` | O1 abort deadline |
| `window.buffer_minutes` | integer | `30` | O1 abort deadline |
| `freeze.mode` | `readonly` or `lockout` | `readonly` | O3, O10, owner-sql O3; `lockout` with more than one configured database is refused at P2 (check), run and step O3, O10 and O11 |
| `freeze.detach_wait_seconds` | integer | `120` | O3, O10 S1 |
| `freeze.detach_kill` | boolean | `true` | O3, O10 S1 |
| `freeze.write_probe` | boolean | `true` | O3 |
| `freeze.quiet_max_age_seconds` | integer | `60` | O10 S1 on Path B (the last lockout pass must be quiet and no older) |
| `freeze.lockout.connections` | integer | `6` | lockout |
| `freeze.lockout.interval_seconds` | integer | `5` | lockout (seconds between passes; 0 = tight loop) |
| `freeze.lockout.protected_accounts` | list | `[]` | P3, lockout, quiet-proof (runner and owner are always protected) |
| `freeze.lockout.quiet_passes_required` | integer | `3` | lockout |
| `copy.workers` | integer 1 to 16 | `4` | P3 estimate, O6 |
| `copy.chunk_rows` | integer | `10000000` | P3, O4, O6 |
| `copy.min_rows_for_chunking` | integer | `5000000` | P3, O4, O6 |
| `copy.max_buckets_per_table` | integer | `32` | P3, O4, O6 |
| `copy.table_concurrency` | `auto` or `1` (auto: at most half the workers on one table above 8 workers) | `auto` | O6 |
| `copy.lock_probe_timeout_seconds` | integer | `5` | O3 write probe, O6 lock probe |
| `copy.lock_retry_seconds` | integer | `120` | O6 |
| `copy.max_attempts` | integer | `3` | O6 (per chunk; a lock wait and error 1997 consume none) |
| `copy.max_lock_waits` | integer | `10` | O6 (lock waits of one chunk in a run before it fails with the fresh-table command) |
| `copy.max_consecutive_failures` | integer 1 to 100 | `6` | O6 circuit breaker: this many chunk failures with no success between them (error 1997 counted, lock waits not) stop new chunks; the running ones finish and O6 ends INCOMPLETE (exit 2) with "chunks are failing in a row; nodes may be restarting, see MV_EVENTS; rerun with --retry-failed when the workspace is steady" |
| `copy.progress_seconds` | integer | `60` | O6 |
| `verify.tiers` | list | `[count, newest, fingerprint]` | O8 |
| `verify.sessions` | integer 1 to 32 or null | `null` (`copy.workers`) | O8 (tier 3 connections: the two sides of a table at the same time, several tables at once, largest first) |
| `verify.full_fingerprint_max_rows` | integer | `1000000000` | O8 (a table with more rows is sampled: a contiguous range on its first sort key column or its time column, the same predicate on both sides; over the threshold with neither column it gets a full fingerprint with a note) |
| `verify.sample_percent` | integer 1 to 100 | `5` | O8 (the percent of the value span the sample covers from its minimum; 100 is a full fingerprint; `step O8 --fingerprint TABLE --percent N` overrides it for one table) |
| `verify.fingerprint_budget_minutes` | integer or null | `null` (no deadline) | O8 (an elapsed deadline from the start of tier 3; a table whose turn comes after it is recorded not fingerprinted and O8 passes only with `--yes`); P3 and P4 estimate |
| `verify.fingerprint_groups` | integer 1 to 4096 | `64` | O8 |
| `verify.value_columns` | integer 0 to 16 | `4` | O8 (the value hash takes the time column plus up to this many supported columns, always; JSON, BSON, GEOGRAPHY, VECTOR and binary types are left out) |
| `pipelines.route` | `kit` or `customer` | `kit` | O9 |
| `pipelines.start_at_release` | `all` or `none` | `all` | O11, C1 (none: the recreated pipelines stay stopped; C1 reports them instead of requiring them Running) |
| `pipelines.credentials_keychain` | mapping | `{}` | P3, O9: the Keychain item per source type (`S3: s2rebuild-pipelines-s3`) that the credential prompt fills and O9 reads before the variables; a type without an entry is not asked |
| `pipelines.per_pipeline_keychain` | mapping | `{}` | O9: the Keychain item per pipeline, read before the source-type item |
| `pipelines.credentials_env` | mapping | `{KAFKA: S2REBUILD_KAFKA_CREDENTIALS, S3: S2REBUILD_S3_CREDENTIALS}` | O9, owner-notebook (the fallback when no Keychain item holds the value) |
| `pipelines.per_pipeline_env` | mapping | `{}` | O9, owner-notebook (overrides the source-type entry) |
| `pipelines.retention_hours` | mapping | `{}` | P4 plan sheet |
| `estimate.baseline` | path or null | `customer/baseline.json` | P3, P4, C2 |
| `estimate.band_percent` | integer | `15` | P3, P4 |
| `estimate.scale_up_cap` | number 1.0 to 4.0 | `1.0` | P3, P4, O4: the most the scan and insert rates are scaled up for a workspace with more leaf cores than the baseline's (1.0 is no credit); fewer leaf cores always scale them down in proportion; no scaling while the baseline records no leaf cores |
| `closeout.retain_days` | integer | `90` (minimum 1) | C2 records `retain_until_utc`, the switch time plus this; C5 (`s2rebuild cleanup --delete`) refuses before it; a value under 90 prints a line on the plan sheet; no key makes a run delete the retained copy |
| `closeout.flexible_parallelism` | number or null | `0.5` | O11 |
| `paths.runs_dir` | path | `customer/runs` | every command (everything a run writes stays under its parent folder) |
| `objects.routines.ignore` | list | `[]` | P2, P3, O4, P4 plan sheet, O5, O7, O10 (routines left behind on purpose, never recreated: `name` for every database, `database.name` for one; the default migrates every captured routine; a pipeline that loads into an ignored procedure refuses P3 and O4, since O9 would find no target) |

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

Nothing else is read from the environment.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | done |
| 1 | refused, or a configuration error |
| 2 | incomplete: rerun to resume |
| 3 | verification mismatch |
| 4 | gate pending or failed: a person acts next |
