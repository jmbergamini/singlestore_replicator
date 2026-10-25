# Code guide

Author: John Bergamini

The package is `s2rebuild/`. Every module starts with `s2rebuild <module>: <purpose>.` and the author line, and every public function says what it issues. Every statement the kit sends passes a Guard first (`sqlutil.py`); `tests/test_guard.py` scans each module for statement words outside its reviewed allowance.

## Module map

- `cli.py`: the command line. Parses global and command flags, picks the run (`Invocation`), opens journals, logs and sessions, and dispatches to the step handlers or command functions. Every journal it opens is checked in one place (`Invocation.check_journals`: the recorded names, and the configuration hash after O1). `check` and `profile` apply the precondition and `--redo` rules of `steps.run_step` before they connect or write. `docs-flags` prints the flag table in `docs/CONFIG.md`.
- `common.py`: KitError and its exit codes, UTC time helpers, run ids.
- `config.py`: loads and validates the YAML against a declarative schema, applies defaults and the run-date names, refuses unknown keys, and exposes `Config`, `DatabaseSpec` and `Window` (with the abort deadline rule).
- `connect.py`: Secrets (Keychain or environment), the Redactor that masks secrets in every line (SQL user names are masked in logs but kept in the admin blocks an operator copies), guarded `Session`, `DryRunSession` and `SessionFactory`, and the 0600 `Log`.
- `journal.py`: the SQLite journal per run and database (WAL, synchronous FULL, one lock): meta, steps, tables, chunks, verification, attachments, grants snapshot, pipelines, offsets, events; schema migrations; read-only and in-memory copies.
- `sqlutil.py`: identifier quoting, the statement classifier, the Guard (statement kinds and the databases they may name), SQL literals for injected secrets and the forms a secret can take in an error message.
- `steps.py`: the registry of P1 to C4 with kinds, identities, gates and preconditions; the owner allowances per step; `run_step`, `run_steps` and gate recording. Modules register their handlers here.
- `inventory.py`: P2 preflight and the P3 and O4 capture (tables, keys, sizes, accounts, sessions, views, routines, pipelines with the credentials placeholder), the copy plan (`TablePlan`, hash buckets), drift.
- `estimate.py`: the throughput model, the list-scheduling simulation, allowances, bands, window fit, and `calibrate`.
- `report_xlsx.py` and `report_md.py`: the profile and final workbooks (Summary first, one tab per table, pipelines and offsets tabs) and the Markdown run report; the C2 handler.
- `transform.py`: the replacement definition hook (a pass-through in this pass; see below).
- `copy.py`: O5 and O6: the replacement, the chunk scheduler, reconciliation, lock probes, the fresh-table fallback, the abort deadline and stop file, the count proof, and the copy lock that O6, an O7 re-copy round, `mark-recopy` and O10 take.
- `verify.py` and `postcopy.py`: the three verification tiers and the O7 stale check, views and counters, the O8 handler.
- `watch.py`: the writer watch and the quiet proof.
- `ownersql.py`: admin blocks (checks, statements, stops, skip conditions, captures) and their companion blocks (the O3 re-entry block), their execution with gates, the DETACH escalation, the P1, C3 and C4 blocks and handlers, and `ownerlib`, the dependency-free copy of the gate logic for the notebook.
- `freeze.py`: O3 Path A with the write probe and the resume paths (after the READONLY attachment, and at the ATTACH after a stop between DETACH and ATTACH).
- `lockout.py` and `grants.py`: O3 Path B: per-scope snapshots, revoke, the kill loop, restore and compare.
- `pipelines.py`: O2 stop and capture, O9 replay or verification, O11 release, C1 checks, and the import of a notebook capture.
- `switch.py`: the O10 state machine and rollback.
- `notebook.py`: fills `notebooks/owner-steps.template.ipynb` with the run, the blocks and ownerlib.
- `rehearse.py`: P5 on throwaway databases with the SIGKILL test.
- `bundle.py`: MANIFEST.json, the static-tree scan, the zip and `--verify`.

`tests/fakedb.py` is an in-memory stand-in for the workspace (attachments and branches, pipelines that move with their storage, grants by scope, identity rules, read-only refusals, lock waits, blocking DETACH, the capture reads of P3 and O4). `tests.helpers.fake_driver` puts it behind the driver, so a test can run a live command on the command line. Tests never open a network connection.

## Data flow

1. `config.yaml` becomes a `Config`; each configured database a `DatabaseSpec` with its replacement and retained names.
2. P3 writes the inventory under `customer/runs/<run>/<db>/inventory/P3/`; O4 writes it again after the freeze. The copy plan comes from the newest capture.
3. Every step writes its start, end, outcome and evidence to `journal.sqlite`; the copy writes each chunk transition before the statement it guards; admin blocks write each check and statement as events; the switch records storage ids as attachments.
4. Reports read the capture and the journal: the profile workbook at P3, the final workbook and run report at O8 and C2. `calibrate` reads the chunk timings back into `customer/baseline.json`.

## Adding a step or a statement

1. Add the step to `REGISTRY` in `steps.py` with its gate and preconditions.
2. Write the handler in the module that owns the work and call `steps.register(ID, handler)` at the bottom of that module.
3. An owner statement goes into a `Block` with a check before and after it; add its kind to the step's entry in `OWNER_ALLOWANCES` and teach `sqlutil.classify` the statement shape if it is new.
4. Add the statement words to the module's entry in `tests/test_guard.py` with a reason, and a test on the fake.

## Decisions you can change

| Decision | Current choice | Alternative and where |
|---|---|---|
| Replacement definition | Partition layout only; `transform.apply` is a pass-through | Fill in `transform.py` for keys, indexes, storage, renames; the hook, config keys and report block exist |
| Chunking strategy | Hash buckets, `CRC32(key) MOD B`, complete coverage by construction | Range chunking on a sort-key prefix for multi-billion-row tables: a `RangePlan` in `copy.py` with NULL and open-ended buckets and a coverage test |
| Bucket cap | 32 per table | Raise for restart granularity, lower for scan cost: `copy.max_buckets_per_table` |
| Freeze default | Read-only attachment | `freeze.mode: lockout`; the lockout loop also serves as a bridge during a slow detach |
| Switch order | Retained branch first, then live | Live first saves one step; the current order keeps the live name unambiguous |
| Replacement after switch | Left attached until C3 | Detach it at S3 to free compute; cleanup then needs the Portal |
| Verification depth | Count, newest, grouped fingerprint within a budget | Full fingerprint everywhere (`verify.fingerprint_budget_minutes` very large) or counts only |
| View definers | Created by the owner session; the original DEFINER kept while its account exists and is not the runner, else the owner account; without the owner profile the O7 block for the admin | Always the owner account, or a named service account: `postcopy.definer_choice` |
| Counters | The larger of the source counter and MAX + 1, then sync on every aggregator (ALL) | MAX + 1 only, or the source counter only: `postcopy.counter_value` |
| Admin steps | Execute with gates when the owner profile exists | Print only (no owner profile) or the generated notebook |
| Workers | 4 | Up to 16; watch leaf CPU: `copy.workers` |
| Driver | PyMySQL in the terminal, singlestoredb in the notebook | Either everywhere: `connect.py` and the notebook template |
| Secrets | Keychain or environment | A secrets manager: `connect.py` |
| Pipelines | Route kit: credentials from the environment at O9 | Route customer: the customer runs the placeholder file, the kit verifies: `pipelines.route` |
| Pipeline start position | Recorded offsets from O2 | EARLIEST or LATEST for a pipeline past retention: edit its ALTER statement in the capture and record the decision in the gate note |

## The transform hook

`transform.py` is where a later pass changes the replacement beyond its partition layout. Today `databases[].target.transforms` must be empty and `transform.apply` returns each table definition unchanged; the final report already carries an "original versus target definition" block. The ordered list of later-pass items is in the module docstring. A transform must keep the copy's column list valid and its definition diff explainable, and it must come with tests on the fake.
