# Golden SAS outputs

The PySpark migration is only "correct" against numbers SAS actually produced. These
files are that reference set — never hand-write or estimate the values. If a file is
missing, `tests/parity/test_golden_parity.py` skips itself rather than asserting
against invented numbers.

The committed set was produced by a real SAS 9.4 run; see [`PROVENANCE.md`](PROVENANCE.md)
for engine, site, date, source commit, path adaptations and a review of the run log.

## Files

Production outputs, from `data/home_equity.csv` (5,960 rows) via `sas/99_export_golden.sas`:

| File | Contents | Used by parity tests |
|---|---|---|
| `row_counts.csv` | `STAGE`, `N` for `home_equity`, `home_equity_final`, `home_equity_risk` | yes — exact integer equality |
| `freq_loan_outcome.csv` | `LOAN_OUTCOME`, `FREQUENCY`, `PERCENT` | yes — exact counts, percent to 1e-9 |
| `freq_job.csv` | `JOB` frequencies, including the missing level | yes |
| `freq_reason.csv` | `REASON` frequencies, including the missing level | yes |
| `risk_segment_freq.csv` | `RISK_SEGMENT`, `FREQUENCY`, `PERCENT` | yes |
| `freq_ltv_risk_cat.csv`, `freq_dti_risk_cat.csv`, `freq_delinq_risk_cat.csv` | each risk driver's category frequencies | yes |
| `freq_risk_score.csv` | the composite `RISK_SCORE` distribution | yes |
| `crosstab_risk_outcome.csv` | `RISK_SEGMENT` × `LOAN_OUTCOME` cell counts and percents | yes |
| `means_by_outcome.csv` | per-`LOAN_OUTCOME` `COUNT`/`MEAN`/`STDDEV` | yes — counts exact, statistics to 1e-9 relative |
| `means_by_risk_segment.csv` | the same statistics classed by `RISK_SEGMENT` | yes |
| `quantiles_numeric.csv` | `N`/`NMISS`/`MIN`/`P25`/`MEDIAN`/`P75`/`MAX` per numeric column | yes — SAS `PCTLDEF=5` quantiles |
| `missing_flag_totals.csv` | sums of the `*_MISS` flags set by `02_data_cleaning.sas` | yes |
| `means_by_outcome_formatted.csv`, `means_by_risk_segment_formatted.csv` | the same datasets with SAS display formats applied | no — reference only |
| `sas_run.log` | full SAS log of the generating run | no — evidence |

Synthetic boundary outputs in `edge/`, from `data/home_equity_edge.csv` (38 rows) via
`sas/97_load_edge_cases.sas` + `sas/98_export_golden_edge.sas`:

| File | Contents | Used by parity tests |
|---|---|---|
| `edge/row_counts.csv` | rows surviving each stage of the fixture | yes |
| `edge/clean_rows.csv` | per-row `LTV` and `LOAN_OUTCOME` *before* the WHERE filters | yes — row-level |
| `edge/final_rows.csv` | per-row `LTV` and every `*_MISS` flag after filtering | yes — row-level |
| `edge/risk_rows.csv` | per-row risk categories, `RISK_SCORE` and `RISK_SEGMENT` | yes — row-level |
| `edge/risk_segment_freq.csv` | segment frequencies over the fixture | yes |

The fixture is keyed by `CITY`: each row's `CITY` names the condition it pins down ("ltv
exactly 0.80", "dti exactly 40", "value zero"), so a row-level mismatch names the rule
that diverged instead of an aggregate where two errors can cancel out. Aggregates alone
cannot distinguish a boundary handled as `>=` from one handled as `>`; the fixture puts
rows exactly on every threshold in `02_data_cleaning.sas` and `04_risk_segmentation.sas`.

### Why there are two means files

`99_export_golden.sas` exports means with the SAS variable formats inherited from the
source columns (DOLLAR / F8.1), so the exporter's own output rounds to `"$16,622"` and
`40.5`. That file is kept verbatim as `means_by_outcome_formatted.csv`.
`means_by_outcome.csv` is the same `WORK.MEANS_BY_OUTCOME` dataset re-exported in the
same session after `format _numeric_;`, and is the one the tests compare against.
Adding `format _numeric_;` to the exporter would collapse the two.

### Log contains errors

The committed log has 5 ERRORs, all in `sas/05_logistic_regression.sas` and all
downstream of the exports (which come from datasets built in 01–04). The cause is
a defect in the checked-in SAS code — `PROC SURVEYSELECT` without `OUTALL` leaves
`work.train` empty. This does not affect the reference outputs; see `PROVENANCE.md`.
Nothing in `05` is covered by a golden file, so the model stage is unvalidated.

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

3. For the boundary fixture, reload and re-run the derivation programs in the same
   session, then run the edge exporter (it writes into `&outdir./edge`):

   ```sas
   %include "sas/97_load_edge_cases.sas";
   %include "sas/02_data_cleaning.sas";
   %include "sas/04_risk_segmentation.sas";
   %include "sas/98_export_golden_edge.sas";
   ```

4. Copy the CSVs it writes into this directory, refresh `PROVENANCE.md`, and commit
   them together.

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
