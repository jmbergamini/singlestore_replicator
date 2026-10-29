# Instructions for a coding agent that runs the kit

Author: John Bergamini

You help an operator rebuild a SingleStore Helios database with this kit. The kit does the work: it connects, runs every statement behind its checks, journals every step and stops at the first failure. Your job is to run its commands in order, read what they print, and report to the operator. You never replace the kit with your own SQL.

Read these first, in this order: `README.md`, `docs/STEPS.md` (every step: inputs, gate, re-entry, what to do when it fails), `docs/CONFIG.md` (every key and flag). `docs/ARCHITECTURE.md` explains the design.

## Rules

1. Never print, log, store or type a password, an API key or a pipeline credential. The kit asks the operator for them at its own hidden prompts and keeps them in the macOS Keychain. When a prompt appears, hand the terminal to the operator.
2. Never run SQL against the workspace yourself, not even a read. Every statement goes through the kit, which checks it against the step's guard.
3. Run nothing against the workspace until the operator asks for it in the session.
4. Stop at the first failure. Report the step, the check, the expected value and the observed value, exactly as the kit printed them, plus the exit code. Do not retry, work around or skip a failed step; follow the "When it fails" line of that step in `docs/STEPS.md` only when the operator agrees.
5. Never stop the copy with kill or Ctrl-C twice. Use `s2rebuild pause`: a killed client leaves a leaf table lock for 15 to 20 minutes.
6. Never delete the retained copy. Only the customer does that, after its retain-until date, with `s2rebuild cleanup --delete NAME`, typing the name at the prompt themselves.
7. A gate that records a person's decision (P1, P4, the O1 go, an admin step run by hand) is recorded only with the operator's own words in `--note`. Never pass `--yes` to O10 or answer the typed-name prompt for the operator.
8. Never edit the journal, the files under `customer/runs/`, or the captures. Edit `customer/config.yaml` only as the operator asks.
9. Keep the Mac awake and on power. The kit starts `caffeinate -i` for every `s2rebuild run` and `s2rebuild step`; for the waits between commands, ask the operator to run `caffeinate -i` in a second terminal for the whole night.

## Setup

1. Install: `python3 -m venv .venv`, then `.venv/bin/pip install -e .`. Gate: `.venv/bin/s2rebuild --help` prints the commands.
2. Configure: copy `examples/config.example.yaml` to `customer/config.yaml` and fill in the host, the database, the target layout and the two SQL identities with the operator. Gate: `s2rebuild check --dry-run` exits 0.
3. Offline tests (optional): `.venv/bin/pip install pytest`, then `.venv/bin/python -m pytest`. Gate: every test passes; no test opens a network connection.

## A run, step by step

Each command prints its gate. Exit codes: 0 done, 1 refused, 2 incomplete (rerun the same command), 3 verification mismatch, 4 a person must record a gate.

1. P1: `s2rebuild account-sql --execute` creates the runner account (the operator types the owner password if the kit asks). Gate: "P1 account creation executed". Then `s2rebuild gate P1 --pass --note TEXT` starts the run and prints its id; export it with `export S2REBUILD_RUN=RUN_ID`.
2. P2: `s2rebuild check`. Gate: every line ok. Read the three workspace questions to the operator.
3. P3: `s2rebuild profile`. Gate: exit 0 and `profile.xlsx` written. The operator answers the pipeline credential prompts.
4. P4: `s2rebuild plan`, read the plan sheet with the operator, then `s2rebuild gate P4 --pass --note TEXT` with their decisions.
5. O1: `s2rebuild run --from O1 --workspace NOTE`, where NOTE is the operator's answers to the three workspace questions. Gate: it stops for the go. The operator types the names if asked; record the go with `s2rebuild gate O1 --pass --note TEXT` in their words.
6. O2 to O11: `s2rebuild run`. It stops at every gate a person must record and at the first failure. Gate: "Steps O1 to O11 passed". The operator types the live name at O10.
7. Close-out: `s2rebuild step C1`, `s2rebuild step C2`, `s2rebuild step C3`, then `s2rebuild account-sql --drop --execute` for C4. Gate: each step passes; C3 shows exactly two entries, the live database and the retained copy.

At any point `s2rebuild status` prints the board of steps. Every command takes `--dry-run`, which prints the statements and writes nothing.

## Reporting

After each command, report in a few lines: the step, the result line the kit printed, the exit code and the next command. After a failure, report only that failure and stop.

## Changing the kit's code

Read `docs/CODE-GUIDE.md` first. Run the tests of the modules you touch, and the whole suite once at the end. Style: no em dashes or en dashes in any file, the author line in every module, every admin statement in a block between a check and its expected result.
