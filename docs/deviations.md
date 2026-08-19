# SAS -> PySpark Deviations Register

Every intentional behavioral difference between the SAS programs in `sas/` and the
PySpark package in `src/homeequity/`. Add a row in the same commit that introduces
the difference.

| ID | SAS construct | SAS program | PySpark behavior | Why it differs | Impact |
|---|---|---|---|---|---|
| D-001 | `.master("local[*]")` hardcoded | n/a (demo scripts) | Master only forced when `SPARK_MASTER`/`DATABRICKS_RUNTIME_VERSION` are unset | Managed clusters set the master themselves | None; local runs unchanged |
| D-002 | `proc datasets ... label` / `format` | sas/01 | Labels stored as Spark column `comment` metadata; formats kept as a `COLUMN_FORMATS` dict, applied only at display time | Spark has no variable labels or display formats | Printed output is unformatted unless a formatter is applied |
| D-003 | `guessingrows=5960` type inference | sas/01 | Explicit `HOME_EQUITY_SCHEMA`, all numerics `DoubleType` | Reproducibility: inference varies with data | Types are now stable across data refreshes |
| D-013 | Char comparison pads/ignores trailing blanks | sas/01-05 (all char handling) | `loadHomeEquity` applies `trim()` to every StringType column at ingest, as a blanket policy | SAS char equality ignores trailing blanks; Spark string equality is exact, so untrimmed values silently split `groupBy` groups and miss join keys | None on values without trailing blanks; guarantees group/join keys compare like SAS. Leading blanks are also removed (SAS keeps them) — acceptable for this extract, which has none |
| D-014 | `APPDATE` stored as SAS date numeric (days since 1960-01-01) | sas/01 | Schema reads `APPDATE` as `DoubleType`; `loadHomeEquity` converts via `date_add(to_date('1960-01-01'), APPDATE)` | SAS epoch is 1960-01-01, 3,653 days before the Unix/Spark epoch; Spark has no SAS-date reader | Dates are correct calendar dates (2017-2021 for this extract); any Unix-epoch parse would shift them ~10 years |
| D-015 | Special missing values `.A`-`.Z`, `._` (`X ne .` is true for them) | sas/02, sas/05 null filters | None required: verified 2026-08-19 that the extract contains no special missing encodings, so `.` -> `null` and `X ne .` -> `isNotNull()` are exact | Spark has a single `null`; the extract only uses the single SAS missing `.` | Safe for this extract; re-run the Task 2 Step 5b scan on any data refresh and add handling if special missings appear |
| D-004 | `propcase(CITY)` | sas/02 | `initcap(CITY)` | Closest Spark builtin | Identical for space-delimited names; differs for hyphenated/apostrophe names (for example `O'Fallon`). Observed once in this extract: `winston-salem` -> `Winston-salem` (SAS: `Winston-Salem`) |
