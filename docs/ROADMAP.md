# s2rebuild: Future Enhancements

Author: John Bergamini
Purpose: enhancements to pick up after release 1.0. Not in scope for the current coding pass. The full roadmap with effort is RELEASE-1.0.md section 10; this file keeps the detail behind R1 and R2.

## E1 Copy progress bar (O6), roadmap R1

- Added: 2026-10-26. Local note: `customer/tpcds/kit-notes-for-later.md` item 1c.
- Problem: the kit counts rows only when a whole chunk commits. On tpcds3tb each `store_sales` chunk is about 270 million rows, so the progress line reads 0 for roughly the first 30 minutes.
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
