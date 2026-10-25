# SingleStore Database Rebuild Kit

Author: John Bergamini. Version 1.0.0. Proprietary; see [LICENSE](LICENSE).

The kit rebuilds a SingleStore Helios database into a new partition layout and cuts it over under its original name inside one outage window. The first outage step makes the source read only. Everything after that is a copy, a verification and a switch, each behind a gate, each restartable from a terminal.

![Flow of the P, O and C steps](docs/flow.svg)

## Who runs what

| Identity | Who | Steps |
|---|---|---|
| runner | A SQL account the kit creates at P1 and drops at C4 | P2, P3 tables, O4 to O8, C1, C2, watch |
| owner | The workspace admin SQL user, or the Portal identity in the owner notebook | P1 account, P3 pipelines, O2, O3, O9, O10, O11, C3, C4, lockout |
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

### Prepare (before the window)

1. P1 Prerequisites. Print the runner account block with `s2rebuild account-sql` and have the admin run it, or run it with `s2rebuild account-sql --execute`. Then run `s2rebuild check`. Gate: `check` passes; record it with `s2rebuild gate P1 --pass --note TEXT`.
2. P2 Preflight. `s2rebuild check` connects as the runner (and the owner), checks every runner privilege by name, records the source STORAGE_ID, confirms the replacement and retained names are free and the compute size fits the partition count. Gate: every line ok.
3. P3 Profile. `s2rebuild profile` captures the inventory (tables, keys, sizes, accounts, sessions, views, pipelines with the owner profile) and writes `profile.xlsx` with the estimate and the window fit. Gate: the estimate fits the window.
4. P4 Plan. `s2rebuild plan` prints the plan sheet: window, abort deadline, freeze path, step owners, pipeline credential route. Gate: the decisions are recorded with `s2rebuild gate P4 --pass --note TEXT`.
5. P5 Rehearsal. `s2rebuild rehearse` builds throwaway `s2rebuild_rehearsal_` databases, copies them with a SIGKILL test and a resume, verifies, freezes, switches and drops what it created. The killed chunk leaves a leaf lock for 15 to 20 minutes, so plan 30 minutes. Gate: P5 passed (the switch needs the owner profile).

### Outage window

6. O1 Start. `s2rebuild run --from O1` records the window and the abort deadline and waits for the go decision: `s2rebuild gate O1 --pass --note TEXT`. Then `s2rebuild run` continues.
7. O2 Stop pipelines. The owner stops every pipeline and the kit captures every definition and offset again after the stop. These are the offsets O9 replays. Gate: no pipeline Running; capture written.
8. O3 Freeze. Path A (default): DETACH, ATTACH READ ONLY, a write probe that must be refused. A DETACH that waits longer than `freeze.detach_wait_seconds` is cancelled and issued again with KILL. Path B: `s2rebuild lockout` revokes write privileges and kills sessions until the quiet proof passes. After a notebook or hand run, `s2rebuild step O3 --redo` runs the probe and records freeze_time. Gate: probe refused and READONLY (A), or quiet proof passed (B).
9. O4 Freeze capture. `s2rebuild profile --step O4` captures the inventory after the freeze and reports drift against P3. Gate: drift reviewed.
10. O5 Create replacement. `s2rebuild run --from O4` continues: the replacement is created with the target layout and every table definition is compared with the capture; its STORAGE_ID is recorded.
11. O6 Copy. Server-side INSERT ... SELECT in hash buckets, journaled, resumable. Stop it only with `s2rebuild pause` or the abort deadline: a killed client leaves a prepared transaction holding a leaf table lock for 15 to 20 minutes. Gate: every chunk done; per-table count proof all match.
12. O7 Post-copy. Views, AUTO_INCREMENT counters and the stale check (counts and newest values) with re-copy of anything that moved. Gate: zero mismatches.
13. O8 Verify. Counts, newest values and grouped fingerprints within the time budget. Gate: tiers 1 and 2 pass for every table; tier 3 within budget or accepted with `--yes`.
14. O9 Pipelines. Export the credential variables for this step only, run it, then unset them. The kit recreates every pipeline in the replacement, stopped, at the O2 offsets. Route customer: the customer runs the placeholder file and the kit checks. Gate: count, none Running, offsets equal the O2 capture.
15. O10 Switch. A state machine: retained copy attached read only first, then the replacement as a branch under the live name. Type the live name when asked (or pass `--yes`). Gate: the live name resolves to the new branch with the target layout and the capture counts.
16. O11 Release. Retention check per partition, grants restored, pipelines started; the customer restarts the application. Gate: application writing.

### Close-out

17. C1 Verify after release. `s2rebuild step C1`: writer watch, count changes, pipelines Running with offsets advancing, no errors, grants compared. Gate: all ok.
18. C2 Report and calibrate. `s2rebuild step C2` writes `final.xlsx` and `run-report.md` and refits the throughput model into `customer/baseline.json`.
19. C3 Retention cleanup. After `closeout.retain_days`: `s2rebuild step C3` drops the retained branch and the replacement. Delete the detached original in the Portal by STORAGE_ID.
20. C4 Remove the migration account. `s2rebuild step C4` drops the runner account; delete its Keychain item.

## Troubleshooting

- Lock waits (error 1205): the kit serializes the table, probes the lock and retries after `copy.lock_retry_seconds`. A lock held by a killed client clears in 15 to 20 minutes. If it does not clear, `s2rebuild step O6 --fresh-table TABLE` loads the table into a new table and renames it.
- Detach waits: a DETACH blocked by writers is cancelled after `freeze.detach_wait_seconds` and issued with KILL when `freeze.detach_kill` is true. With `detach_kill: false` the step stops; run `s2rebuild lockout` as a bridge, then `s2rebuild step O3 --redo`.
- Grants restored by another session: the lockout logs `GRANT RESTORED BY ANOTHER SESSION` and revokes again on the next pass. Find the job that restores them before the night.
- Name collisions: names are not unique on Helios. Every ATTACH names a STORAGE_ID. `s2rebuild check` refuses a replacement or retained name that already exists.
- Pipelines the application recreates: list them in `databases[].app_managed_pipelines`. They are stopped at O2, never replayed, and C1 reports whether they came back.
- An admin step stopped at a check: the error names the check, the expected value and the observed value. Fix the cause and rerun the step; the switch re-enters from the live attachment state.

## Documents

- [docs/STEPS.md](docs/STEPS.md): every step in full.
- [docs/CONFIG.md](docs/CONFIG.md): every key, flag, variable and exit code.
- [docs/CODE-GUIDE.md](docs/CODE-GUIDE.md): modules, data flow, decisions you can change.
- [docs/LESSONS.md](docs/LESSONS.md): Helios facts and run lessons.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): the design.
- `docs/s2rebuild-guide.pdf`: this README and STEPS with the flow diagram, built by `docs/build_pdf.py`.
