# SingleStore Database Rebuild Kit

Author: John Bergamini. Version 1.0.0. Proprietary; see [LICENSE](LICENSE).

The kit rebuilds a SingleStore Helios database into a new partition layout and cuts it over under its original name inside one outage window. The first outage step makes the source read only. Everything after that is a copy, a verification and a switch, each behind a gate, each restartable from a terminal.

![Flow of the P, O and C steps](docs/flow.svg)

## Who runs what

| Identity | Who | Steps |
|---|---|---|
| runner | A SQL account the kit creates at P1 and drops at C4 | P2, P3 tables, O4 to O8, C1, C2, watch |
| owner | The workspace admin SQL user, or the Portal identity in the owner notebook | P1 account, P3 pipelines, O2, O3, O7 views, O9, O10, O11, C3, C4, lockout |
| operator | The person running the kit | every command; records the gates a person decides |
| customer | The application owner | stops the application (if asked), hands over pipeline credentials, restarts the application at O11 |

The runner copies. The owner freezes, switches and releases. With an owner profile in the configuration the kit runs the admin steps itself; without one it prints each admin block and waits for `s2rebuild gate`.

## Install

Python 3.10 or later on macOS or Linux. The Keychain password source is macOS only; elsewhere use `password_env`.

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/s2rebuild --help
```

Run the offline tests with `.venv/bin/pip install pytest` and `.venv/bin/python -m pytest`. No test opens a network connection.

## Configure

1. Copy the example: `cp examples/config.example.yaml customer/config.yaml`.
2. Set the workspace host, the database names and the target layout. Every key is in [docs/CONFIG.md](docs/CONFIG.md).
3. Store each password in the macOS Keychain (`security add-generic-password -a "$USER" -s s2rebuild-runner -w`) or name an environment variable with `password_env`. The kit never prints or writes a password.
4. Everything a run writes stays under `customer/`. The rest of the folder is the static tree.

## Steps

Each step prints what it did and the gate it needs. Exit codes: 0 done, 1 refused, 2 incomplete (rerun to resume), 3 verification mismatch, 4 gate pending or failed. The full catalog with inputs, outputs, statements and recovery is [docs/STEPS.md](docs/STEPS.md).

Every command works on one run under `customer/runs/`. The P1 gate starts the run and prints its id. Export that id as `S2REBUILD_RUN`, so every later command names the same run. Without `S2REBUILD_RUN` or `--run ID`, a command uses the most recent run. `s2rebuild run` resumes that run and skips the steps that passed. It starts a run only when none exists, or with `--new` (`--new --run ID` names the new run). Once a run exists, `s2rebuild run --run ID` refuses an id that names no run, so a mistyped id never starts another run. Every gate hint and every log header prints the run id with its `--run` form.

The sequence from P1 to the go and on to O2, where RUN_ID is the id the P1 gate printed:

```bash
s2rebuild account-sql
s2rebuild gate P1 --pass --note TEXT
export S2REBUILD_RUN=RUN_ID
s2rebuild check
s2rebuild profile
s2rebuild plan
s2rebuild gate P4 --pass --note TEXT
s2rebuild rehearse
s2rebuild run --from O1
s2rebuild gate O1 --pass --note TEXT
s2rebuild run
```

### Prepare (before the window)

1. P1 Prerequisites. Print the runner account block with `s2rebuild account-sql` and have the admin run it, or run it with `s2rebuild account-sql --execute`. Store the passwords and write the configuration. Gate: the account, the Keychain items and the configuration are in place; record it with `s2rebuild gate P1 --pass --note TEXT`. When no run exists yet, this gate starts the run and prints its id; export it as `S2REBUILD_RUN`.
2. P2 Preflight. `s2rebuild check` connects as the runner (and the owner), checks every runner privilege by name, records the source's branch record (name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID), confirms the replacement and retained names are free and the compute size fits the partition count. It refuses until P1 is recorded. Run it again any time: O1 needs P2 passed in the same run. Gate: every line ok.
3. P3 Profile. `s2rebuild profile` captures the inventory (tables, keys, sizes, accounts, sessions, views; with the owner profile the pipelines, the routines and the write-capable accounts, which the runner cannot see) and writes `profile.xlsx` with the estimate and, when a window is configured, the window fit. It refuses until P2 passed. Once P3 passed, a new capture needs `--redo`, which logs what it rewrites. `--dry-run` writes only `profile-dry-run.xlsx`, never the capture. Gate: the estimate fits the window, when one is configured. Routines (functions, table-valued functions, aggregates, procedures) are captured with their definitions and recreated at O5 and O7; `objects.routines.ignore` leaves one behind on purpose. A pipeline that loads into an ignored procedure refuses the capture, since O9 would find no target.
4. P4 Plan. `s2rebuild plan` prints the plan sheet: window and abort deadline (when configured), freeze path, step owners, pipeline credential route. Gate: the decisions are recorded with `s2rebuild gate P4 --pass --note TEXT`.
5. P5 Rehearsal. `s2rebuild rehearse` builds throwaway `s2rebuild_rehearsal_` databases, copies them with a SIGKILL test and a resume, verifies, freezes, switches and drops what it created. The killed chunk leaves a leaf lock for 15 to 20 minutes, so plan 30 minutes. Gate: P5 passed (the view at O7 and the switch need the owner profile).

### Outage window

6. O1 Start. `s2rebuild run --from O1` checks that P2 passed in this run and that rehearsal evidence exists (P5 passed, or a passed rehearsal of any run), records the window and the abort deadline when a window is configured, records the configuration hash, and waits for the go decision: `s2rebuild gate O1 --pass --note TEXT`. Without a rehearsal, `--no-rehearsal NOTE` starts anyway and records the note. The window is optional: without `window.start` and `window.end` there is no abort deadline, and the copy and the switch run without a time limit. Then `s2rebuild run` resumes the same run: it skips O1 and continues at O2. `s2rebuild run --new` starts another run instead, which needs its own P1 and P2.
7. O2 Stop pipelines. The owner stops every pipeline and the kit captures every definition and offset again after the stop. These are the offsets O9 replays. Gate: no pipeline Running; loaded and successful offsets agree on every partition; capture written.
8. O3 Freeze. Path A (default): DETACH, ATTACH READ ONLY, a write probe that must be refused. A DETACH that waits longer than `freeze.detach_wait_seconds` is cancelled and issued again with KILL. Path B (single database only): `s2rebuild lockout --hold` revokes write privileges and kills sessions until the quiet proof passes, then keeps passing until S1 of O10 has run; leave it running in a second terminal through O10. After a notebook or hand run, `s2rebuild step O3 --redo` runs the probe and records freeze_time. A run that stopped between DETACH and ATTACH resumes at the ATTACH: `s2rebuild step O3 --redo` sees the detached storage and the free source name and never detaches again. For a hand run, `s2rebuild owner-sql O3` writes that block to `owner-sql/O3-reentry.sql` beside `O3.sql`. The Portal may show 0 rows for the frozen source; the kit's counts are the reference. Gate: probe refused and READONLY (A), or quiet proof passed (B).
9. O4 Freeze capture. `s2rebuild profile --step O4` captures the inventory after the freeze and reports drift against P3 (tables, columns, views, routines and pipelines; on Path A the pipelines are not listed on the READ ONLY source and the report says so). It refuses until O1 to O3 passed. Once O4 passed, a new capture needs `--redo`: it rewrites the copy plan. Gate: drift reviewed.
10. O5 Create replacement. `s2rebuild run --from O4` continues: the replacement is created with the target layout and every table definition is compared with the capture; its STORAGE_ID is recorded. Then the owner session creates the captured functions and table-valued functions, and the aggregates after the functions they name; the gate compares each definition with the capture. A routine body that names the source database refuses the step before any statement.
11. O6 Copy. Server-side INSERT ... SELECT in hash buckets, journaled, resumable. Stop it only with `s2rebuild pause` or the abort deadline: a killed client leaves a prepared transaction holding a leaf table lock for 15 to 20 minutes. Gate: every chunk done; per-table count proof all match.
12. O7 Post-copy. Views, then procedures, AUTO_INCREMENT counters and the stale check (counts and newest values) with re-copy of anything that moved. A completed re-copy marks O6 passed again, and the counters are set after the last re-copy. A view runs under its definer and C4 removes the runner, so the runner never creates one. With the owner profile the kit creates the views, keeping each original definer whose account still exists, then the procedures the same way (a procedure may read a view, and a pipeline INTO PROCEDURE at O9 needs its procedure). Without it O7 writes `owner-sql/O7.sql` and ends pending: run that block as the admin, then `s2rebuild step O7`. A view or procedure whose definition names the source database refuses the step. Gate: views and procedures created, zero mismatches.
13. O8 Verify. Counts, newest values and grouped fingerprints within the time budget. Gate: tiers 1 and 2 pass for every table; tier 3 within budget or accepted with `--yes`. A mismatch marks the buckets for re-copy; run `s2rebuild run --from O6` to copy them again and rerun O7 and O8.
14. O9 Pipelines. Export the credential variables for this step only, run it, then unset them. The kit recreates every pipeline in the replacement, stopped, at the O2 offsets. Route customer: the customer runs the placeholder file and the kit checks. Gate: count, none Running, offsets equal the O2 capture partition by partition.
15. O10 Switch. A state machine that ends with two entries: S1 detaches the source; S2 attaches the original storage read only under the retained name; S2b attaches the original record by its own syntax under the live name, proves it unchanged (the three smallest tables, no foreign session) and drops it; S3 attaches the replacement as a branch under the live name; S4 drops the replacement record; the gate lists both storages and expects exactly the live record and the retained record. A failed proof at S2b stops with the original attached read write under the live name, the rollback state. S0 refuses unless every planned table was verified after its last chunk was copied; without the owner profile O10 refuses on the same rule, and the printed block and the notebook cell start with a STOP while it holds. A re-copy rewrites `owner-sql/O10.sql` with a STOP; generate the blocks or the notebook again after O8 passes. On Path B, S0 and S1 refuse unless the last lockout pass was quiet within `freeze.quiet_max_age_seconds` (default 60); that refusal comes before the go, so a re-copy stays possible. The printed block and the notebook cell check instead, before the DETACH, that every privilege the lockout took is still revoked. Type the live name when asked (or pass `--yes`). Gate: the live name resolves to the new branch with the target layout and the capture counts. From S3 the live name accepts writes. A rollback (`s2rebuild owner-sql O10 --rollback` writes both legs) runs only while every table still holds the row count of the copy and O11 has not started; it detaches the live name only while it holds the recorded live record, then re-attaches the original record by its own ATTACH_SYNTAX before S2b ran (R2a), or after S2b attaches a read-write branch of the retained copy under the live name (R2b; R2c re-attaches the retained record itself when the server refuses a branch of a read-only attachment). No rollback step drops the retained copy. The count check cannot see an update that keeps the count, so a rollback after S3 is safe only when the application stayed stopped until O11, as the O1 note records.
16. O11 Release. Retention check per partition, grants restored, pipelines started; the customer restarts the application. Gate: application writing.

### Close-out

17. C1 Verify after release. `s2rebuild step C1`: writer watch, count changes, pipelines Running with offsets advancing, application-managed pipelines back, no errors, grants compared. Gate: all ok and application writes seen. For a live database idle on purpose, pass `s2rebuild step C1 --idle-workload NOTE`. Without the owner profile the pipeline and grants checks are recorded as not checked.
18. C2 Report and calibrate. `s2rebuild step C2` writes `final.xlsx` and `run-report.md` and refits the throughput model into `customer/baseline.json`. With too few chunks to fit, the baseline in use stays, the reports say so, and C2 passes. When the kit cannot pass C1 or C2, record that step by hand with `s2rebuild gate ID --pass --note TEXT --override NOTE`.
19. C3 End state. `s2rebuild step C3` checks that the workspace holds exactly two entries for the run, the live database and the retained copy (see below), that the live name resolves to the recorded live record, and that a write on the retained name is refused; the listing goes into the journal and the final report. It issues no DETACH, ATTACH or DROP. When the gate finds a third entry and the journal shows no S2b or S4 (a switch made by an older kit), the step prints the repair block, says that the live name is detached for about 30 seconds, and runs it after you type proceed (or with `--yes`): detach the live name, attach the original record by its own syntax, check it, drop it, re-attach the live record by its own syntax, drop the replacement record, then the gate again. Without the owner profile the block is printed and the gate waits for a person.
20. C4 Remove the migration account. `s2rebuild step C4` drops the runner account; delete its Keychain item. C3 must be passed. The step stops before DROP USER while a view or a routine in the live database has the runner as its definer.
21. C5 Delete the retained original, later and by the customer. `s2rebuild cleanup` lists what the run left: the live record, the retained copy with its record, its size in GiB and the retain-until date. After that date `s2rebuild cleanup --delete <retained>` prints the record and asks you to type the name again; anything else keeps the copy and records "kept by customer". The delete is DETACH, re-attach by the record's own syntax read write, a record check, DROP, each behind its check, then a check that the live name still resolves to the live record. `--yes` does not apply, and no configuration key makes a run delete the retained copy.

After a run the workspace holds two databases: the live database under the live name, a branch of the replacement with the target layout, and the retained copy of the original under the retained name, attached read only on the original storage. The switch (O10) ends there on purpose: the original's own record and the replacement are dropped inside the switch (S2b and S4), after the proof that the copy equals the original and that the live branch is independent of the replacement, so no second outage is needed and nothing is left to delete by hand. The retained copy is the way back and the audit copy; the kit never deletes it. C2 records `retain_until_utc` (the switch time plus `closeout.retain_days`, 90 by default) in the journal, the final workbook and `run-report.md`, with the C5 command.

## Troubleshooting

- Re-copy after O8: `s2rebuild mark-recopy TABLE [--bucket N]` sets O6, O7 and O8 back to pending and clears the table's verification. Run `s2rebuild run --from O6`; O6 copies the buckets again, then O7 and O8 run again. The switch refuses until O8 passes again. A fresh-table load reopens O7 and O8 the same way. Both rewrite `owner-sql/O10.sql` and `O10-rollback.sql` with a STOP; after O8 passes, run `s2rebuild owner-sql O10` (and `--rollback`) or `s2rebuild owner-notebook` again, and never run the O10 cells of an older notebook. Once the switch may have begun (O10 passed, the go recorded, an O10 gate, or the retained or live attachment recorded), a re-copy is refused; a new copy is a new run.
- Lock waits (error 1205): the kit serializes the table, probes the lock and retries after `copy.lock_retry_seconds`. A lock held by a killed client clears in 15 to 20 minutes. A chunk fails after `copy.max_lock_waits` lock waits and O6 stops INCOMPLETE with the command to run: `s2rebuild step O6 --fresh-table TABLE` loads the table into a new table and renames it. It refuses past the abort deadline or while the stop file exists. After a failure, rerun the same command; it resumes and does not load a complete fresh table again.
- Detach waits: a DETACH blocked by writers is cancelled after `freeze.detach_wait_seconds` and issued with KILL when `freeze.detach_kill` is true. With `detach_kill: false` the step stops; run `s2rebuild lockout` as a bridge, then `s2rebuild step O3 --redo`.
- Grants restored by another session: the lockout logs `GRANT RESTORED BY ANOTHER SESSION` and revokes again on the next pass, with `--hold` also after the quiet proof. Find the job that restores them before the night.
- Name collisions: names are not unique on Helios, and a STORAGE_ID names a storage that several branch records share. Every ATTACH names a STORAGE_ID, and every check of the switch, the rollback, C3 and abandon compares the branch record (name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID) with the one the journal recorded. `s2rebuild check` refuses a replacement or retained name that already exists.
- Pipelines the application recreates: list them in `databases[].app_managed_pipelines`. They are stopped at O2, never replayed, and C1 reports whether they came back.
- Configuration changed after O1: O1 records the configuration hash. Every command that opens the run journal refuses a configuration that hashes differently, until `--accept-config-change NOTE` records both hashes and the note in the journal. A changed database name is refused with or without it: use the configuration of the run. `s2rebuild status` reports both.
- Abandon a run before the switch: run `s2rebuild pause` and wait until the copy step ends, so no chunk still reads the source. `s2rebuild abandon` prints the block; `s2rebuild abandon --execute --note TEXT` runs it with the owner profile. It attaches the source READWRITE again as its recorded record, drops the replacement only while its name holds the record O5 recorded and no branch was made from it, and closes the run: `run`, `step` and `gate` refuse it afterwards. Then remove the runner with `s2rebuild account-sql --drop --execute`. A new run starts at P1. Once the switch may have begun, abandon refuses; the way back is `s2rebuild owner-sql O10 --rollback`.
- An admin step stopped at a check: the error names the check, the expected value and the observed value. Fix the cause and rerun the step; the freeze and the switch re-enter from the live attachment state.

## Documents

- [docs/STEPS.md](docs/STEPS.md): every step in full.
- [docs/CONFIG.md](docs/CONFIG.md): every key, flag, variable and exit code.
- [docs/COVERAGE.md](docs/COVERAGE.md): the coverage grid, what the kit migrates and what it does not.
- [docs/CODE-GUIDE.md](docs/CODE-GUIDE.md): modules, data flow, decisions you can change.
- [docs/LESSONS.md](docs/LESSONS.md): Helios facts and run lessons.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): the design.
- `docs/s2rebuild-guide.pdf`: this README and STEPS with the flow diagram, built by `docs/build_pdf.py`.
- `tests/engine/helios-<version>.json`: the record of the engine the test fake is built from, written by `s2rebuild engine-capture --fixture-database NAME` with the owner profile (reads only); refresh it per engine version as docs/CODE-GUIDE.md says.
