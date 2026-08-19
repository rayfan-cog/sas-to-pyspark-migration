# Golden SAS outputs

The PySpark migration is only "correct" against numbers SAS actually produced. These
files are that reference set — never hand-write or estimate the values. If a file is
missing, `tests/parity/test_golden_parity.py` skips itself rather than asserting
against invented numbers.

The committed set was produced by a real SAS 9.4 run; see [`PROVENANCE.md`](PROVENANCE.md)
for engine, site, date, source commit, path adaptations and a review of the run log.

## Files

| File | Contents | Used by parity tests |
|---|---|---|
| `row_counts.csv` | `STAGE`, `N` for `home_equity`, `home_equity_final`, `home_equity_risk` | yes — exact integer equality |
| `freq_loan_outcome.csv` | `LOAN_OUTCOME`, `FREQUENCY`, `PERCENT` | yes — exact counts, percent to 1e-9 |
| `risk_segment_freq.csv` | `RISK_SEGMENT`, `FREQUENCY`, `PERCENT` | yes — exact counts, percent to 1e-9 |
| `means_by_outcome.csv` | `LOAN_OUTCOME`, `_TYPE_`, `_FREQ_`, per-variable `COUNT`/`MEAN`/`STDDEV` | yes — counts exact, statistics to 1e-9 relative |
| `means_by_outcome_formatted.csv` | the same dataset as first written by the generating run, before formats were stripped | no — evidence only |
| `sas_run.log` | full SAS log of the generating run | no — evidence |

### Why there are two means files

At the time of the generating run, `99_export_golden.sas` exported means with the SAS
variable formats inherited from the source columns (DOLLAR / F8.1), rounding values to
`"$16,622"` and `40.5`. That output is kept verbatim as `means_by_outcome_formatted.csv`.
`means_by_outcome.csv` is the same `WORK.MEANS_BY_OUTCOME` dataset re-exported in the
same session after `format _numeric_;`, and is the one the tests compare against.

The exporter now strips formats itself, so a regeneration writes the comparison-grade
`means_by_outcome.csv` directly and produces no formatted variant.

### Log contains errors

The committed log has 5 ERRORs, all in `sas/05_logistic_regression.sas` and all
downstream of the four exports (which come from datasets built in 01–04). The cause is
a defect in the checked-in SAS code — `PROC SURVEYSELECT` without `OUTALL` leaves
`work.train` empty. This does not affect the four reference outputs; see `PROVENANCE.md`.

## How to regenerate

1. On a machine with SAS 9.4 (or SAS Viya) and this repo's `data/home_equity.csv`
   available at the path used by `sas/01_data_loading.sas`, run the programs in order
   in a single SAS session so the `work` library carries between them:

   ```sas
   %include "sas/01_data_loading.sas";
   %include "sas/02_data_cleaning.sas";
   %include "sas/03_aggregation_reporting.sas";
   %include "sas/04_risk_segmentation.sas";
   %include "sas/05_logistic_regression.sas";
   ```

2. Set `%let outdir = ...;` at the top of `sas/99_export_golden.sas` to a writable
   directory, then run it:

   ```sas
   %include "sas/99_export_golden.sas";
   ```

3. Copy the four CSVs it writes into this directory, refresh `PROVENANCE.md`, and commit
   them together. `means_by_outcome_formatted.csv` is an artifact of the original run
   only — a regeneration does not reproduce it, and it can be dropped once the run it
   documents is no longer the committed set.

Regenerate whenever `data/home_equity.csv` changes or any program under `sas/` changes.
Stale golden files are worse than missing ones: the parity tests will pass against
numbers that no longer describe the SAS pipeline.

## Security guardrails when regenerating

Golden artifacts come off someone's SAS environment, so treat everything copied here as
publishable:

- Never commit credentials, session tickets, activation URLs, cookies or `.env` files.
  Keep SAS passwords in a secret store, never in a program, log or provenance file.
- Sanitize logs before committing: strip account IDs, home directory paths, host names
  and email addresses. The committed `sas_run.log` has its ODA user ID replaced with
  `ODAUSER` and its server name generalized.
- Record provenance by identity of the *run* (engine, version, site, date, commit),
  not by identity of the *person*.
- Grep the staged files before committing, e.g.
  `git grep -n -iE 'password|secret|token|api[_-]?key|activation|ticket' -- tests/parity/golden`,
  and review each hit — SAS logs legitimately contain words like `ERROR`.
- Only the aggregate outputs above belong here. Do not commit row-level extracts or any
  dataset that is not already in this repo.

## Licensing of the committed set

These files were produced on SAS OnDemand for Academics, whose terms permit learning and
non-commercial use only; this migration was confirmed non-commercial. If the project ever
becomes commercial work, regenerate on a SAS Viya trial or a licensed SAS installation
before relying on them.
