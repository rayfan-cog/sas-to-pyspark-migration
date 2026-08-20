---
name: sas-migration-task
description: Execute one numbered task of the SAS-to-PySpark migration plan in this repo - branch/stack conventions, the fixed verification commands and their known-good numbers, and the PR body format. Use whenever a session is asked to implement "Task N" of docs/superpowers/plans/*-full-codebase-migration.md.
---

# Executing one migration plan task

The plan (`docs/superpowers/plans/2026-08-19-full-codebase-migration.md`, ~1600 lines) is the
source of truth for *what* each task does. This skill is the source of truth for *how* every task
is executed, so a task prompt only needs to say which task to run and which branch to stack on.

## 1. Read only your slice of the plan

Do not read the plan end to end. It is sectioned `### Task 1` .. `### Task 8`; read `## Global
Constraints` (~40 lines) plus your own task section, and grep for cross-references instead of
opening neighbouring tasks.

## 2. Global constraints (do not re-derive from the plan)

- Python 3.10, PySpark pinned `3.5.1`, Java 17. Deps are pre-installed by the environment
  blueprint - only run `pip install -r requirements.txt` if an import actually fails.
- Package `homeequity` under `src/`; modules snake_case, local variables camelCase.
- Column names UPPERCASE, matching SAS variable names exactly.
- Deterministic seed `42` wherever sampling occurs.
- No transformation logic in `jobs/` or `tests/` - both import from `src/homeequity/`.
- Never edit `data/home_equity.csv` or anything under `sas/`.
- Every behavioral difference from SAS gets a `D-0NN` row appended to `docs/deviations.md` in the
  same commit that introduces it.

## 3. Branching and stacking

Tasks land as a stack, never onto `main` directly:

```bash
git fetch origin <integration-branch>
git checkout -b devin/<short-task-name> origin/<integration-branch>
```

Open exactly one PR whose base is the integration branch given in the prompt. Never merge, never
rebase a sibling task's branch. Concurrent sibling sessions own their own paths - touch only the
files your task section lists.

## 4. Verification: run this once, before opening the PR

```bash
ruff check src tests jobs
python -m pytest tests -q
for j in jobs/*.py; do PYTHONPATH=src python "$j" > "/tmp/$(basename "$j").out" 2>&1 || echo "FAIL $j"; done
```

Jobs are cwd-independent (`DATA_PATH` is anchored via `__file__`).

### Known-good invariants

Compare against these instead of recomputing them; only investigate a number that disagrees.

| Quantity | Value |
|---|---|
| Raw rows / columns | 5960 / 18 |
| Raw `JOB` non-null / null | 5681 / 279 |
| After critical-null filter | 5848 |
| Final analysis rows | 5337 (Default 997 / Paid 4340) |
| Final `JOB` non-null | 5161 |
| `RISK_SEGMENT` | Low 3118 / Medium 1776 / High 427 / Very High 16 |
| Model complete cases | 3532 (train 2528 / valid 1004) |
| Model AUC / accuracy | 0.8028 / 0.9263 (TP 25 / TN 905 / FP 4 / FN 70) |

If a task legitimately changes one of these, update this table in the same PR.

## 5. One verification pass, not two

This is a shell-only CLI repo: there is no UI, so there is nothing a browser or recording pass can
add. Run the commands above once, paste the real numbers into the PR body, and stop. Do not hand
the same commands to a second verification pass afterwards - a re-run that reproduces numbers
already in the PR body buys no new information and roughly doubles the cost of the task.

## 6. PR body

Follow the repo's PR template if one exists, and include, in this order:

1. What changed, per file.
2. The verification block: the exact commands from §4 and their real output numbers.
3. Any new `D-0NN` deviation rows, quoted.
4. For a task that deletes or replaces tests: the assertion-by-assertion mapping from each removed
   assertion to its replacement.

Report to the user only the verdict plus the PR link - the PR body is the evidence.
