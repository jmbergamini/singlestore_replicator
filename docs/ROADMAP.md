# s2rebuild: Future Enhancements

Author: John Bergamini
Purpose: enhancements to pick up after release 1.0. Not in scope for the release 1.0 pass; the work order (`docs/RELEASE-1.0.md`, section 10) is where this table comes from. Effort: S under half a day, M one to two days, L three to five days, XL more than a week. Items stay out of 1.0 unless John moves one. The detail behind R1 and R2 follows the table.

## Items

| ID | Item | Why later | Effort |
|---|---|---|---|
| R1 | Copy progress bar: terminal bar, rows in flight, the same numbers from a second terminal (`watch` or `status`) | Cosmetic for the operator; the journal already holds the numbers | S |
| R2 | Streamlit dashboard: step board, per-table progress, verify results, pause and resume buttons; `s2rebuild ui` starts it against the journal | Share-out candidate after 1.0; Streamlit is the local view, Dash for a page hosted in Helios | M |
| R3 | Performance test and a workers study: whether 16 to 32 workers raise throughput or the target partition count and storage writes set the limit | Needs the 1.0 baseline from the full run of the live ladder (I5); `copy.workers` accepts up to 32 and `estimate.scale_up_cap` gives no credit above the baseline until the test says more | M (test), S (code) |
| R4 | Transform hook build-out: shard key, sort key, index add and drop, storage type, renames and exclusions | Decided per customer; the hook and config keys exist (`transform.py`, `databases[].target.transforms`) | L |
| R5 | Range chunking on a sort-key prefix with segment elimination for multi-billion-row tables | Hash buckets scan the whole table per chunk; the sampled fingerprint of O8 already uses a sort-key range | L |
| R6 | Projections: capture and recreate after the table is full | Not present in the test databases yet | M |
| R7 | LINK objects: capture the definition with a credentials placeholder, recreate before O9 | Same handover model as pipelines | M |
| R8 | Path B rework: run-level grant snapshot, global restores at the last O11, MV_PROCESSLIST sampling, hold loop inside `run`, rehearsal of Path B | Path A is the default; Path B stays single database; the rehearsal freezes with Path A | L |
| R9 | Evidence validity as one design: every row records run id, storage ids, config hash, freeze time, epoch; one function invalidates dependents | The re-copy epoch, the configuration hash and the record checks cover the known cases | L |
| R10 | Runner privileges narrowed to the replacement and rehearsal names where Helios allows | `*.*` grants are dropped at C4 | S |
| R11 | Owner notebook refresh: regenerate after O3 and O5, exclude CURRENT_USER and CONNECTION_ID from the lockout cell | The notebook is the secondary route | S |
| R12 | Incremental catch-up and pre-staging of very large tables outside the window | Changes the copy model | XL |
| R13 | Keep-alive as a scheduled notebook job created and removed by the kit | The kit's own keep-alive thread covers the steps (`workspace.keepalive_minutes`); the job would cover the waits between commands, which P4 records as auto-suspend off or a job of the customer | S |
| R14 | Secrets manager and `singlestoredb` driver as alternatives in `connect.py` | Keychain and env are enough for one operator | S |
| R15 | Terminal dashboard (`watch --tui`) with Textual | R2 covers the need if the Streamlit view is built | S |
| R16 | Multi-workspace runs and a system profile across workspaces | One workspace per config today | M |
| R17 | `sample-database` as a standalone tool for customers to build test copies | Built for P5 in 1.0 (`s2rebuild sample-database SOURCE NAME --percent P`, with `--tables`); the remainder is the customer-facing options: pipelines, a target workspace, a size cap | S |

## E1 Copy progress bar (O6), roadmap R1

- Added: 2026-10-26. Local note: `customer/tpcds/kit-notes-for-later.md` item 1c.
- Problem: the kit counts rows only when a whole chunk commits. On a 3 TB database each chunk of the largest table is about 270 million rows, so the progress line reads 0 for roughly the first 30 minutes.
- Scope:
  - A terminal progress bar in the copy's progress line.
  - The same numbers readable from a second terminal (for example `s2rebuild watch` or `status`).
  - A count of rows still in flight, so the bar moves before the first chunk commits.
- Open question: uncommitted rows from an `INSERT ... SELECT` are not visible to `COUNT(*)` from another session. Candidate sources for the in-flight figure: elapsed time times the measured rows per second of earlier chunks, or the engine's activity views (`MV_ACTIVITIES`, `PROCESSLIST`). Decide when the item is picked up.

## E2 Streamlit dashboard, roadmap R2

- Added: 2026-10-26.
- Rule: the dashboard reads the journal and starts CLI commands. It is never a second way of running steps, so every gate, write guard and confirmation stays in one place.
- Scope: `s2rebuild ui` starts `streamlit run` against the run folder; a step board P1 to C4 with status and times; per-table copy progress (with E1's in-flight figure); verify results per tier; pipelines and their state; the two-entry listing after the switch; links to the Excel and Markdown reports; buttons for `pause`, `resume` and `report` only. A timed rerun every 5 to 10 seconds.
- Not in scope: switch, rollback, cleanup or any delete from the dashboard; those stay in the terminal.
- Hosting: Streamlit for the operator's machine. A page hosted inside Helios would be a Dash app and would need the kit to copy journal rows into a small status table on the workspace; decide after 1.0.
- Lighter alternative: a terminal dashboard (`watch --tui`) with Textual, roadmap R15.
