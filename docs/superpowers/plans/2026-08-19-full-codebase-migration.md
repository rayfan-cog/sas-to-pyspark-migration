# Full SAS-to-PySpark Codebase Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the five stand-alone demo scripts into a tested, importable PySpark package whose outputs are verified against the SAS programs they replace, so the whole `sas/` tree can be retired with evidence.

**Architecture:** A `src/` layout package (`homeequity`) owns the schema, the SAS metadata (labels/formats), and one module per SAS program's logic; the executable scripts in `jobs/` become thin CLI wrappers that call those modules. Tests import the same modules the jobs run — no logic is re-implemented in the test file — and a parity harness compares module output against golden CSV extracts of the SAS ODS output, with every intentional difference recorded in a deviations register.

**Tech Stack:** Python 3.10, PySpark 3.5.1 (DataFrame API + `pyspark.ml`), pytest 8, ruff, GitHub Actions, Java 17 (Temurin).

## Repository Assessment

Answers the four questions this plan was commissioned to settle. All counts measured at commit `32aca55` (the repo's only commit).

### 1. Scale

| Metric | Value |
|---|---|
| Tracked files (excl. `.git`) | 15 |
| SAS source | 5 programs, **417 lines** (`01`: 57, `02`: 84, `03`: 57, `04`: 131, `05`: 88) |
| Existing PySpark | 5 scripts, **891 lines** |
| Tests | 1 file, 394 lines, 14 test methods |
| Docs | `migration_guide/` 550 lines |
| Data | `data/home_equity.csv`, 5,960 rows x 18 columns (~1 MB) |
| Distinct SAS steps to cover | 32 `DATA`/`PROC`/`%let`/`libname` statements |
| Packaging / CI / lint config | **none** |
| Current test suite state | `pytest tests -q` -> **1 failed, 13 passed** (verified 2026-08-19 with pyspark 3.5.1): `test_frequency_counts` asserts `5960 == 5681`, i.e. it counts all rows but compares against non-null `JOB` rows |

This is a **small, single-wave migration**: ~400 lines of SAS with no macro library, no `%INCLUDE` graph, no multi-library ETL, one input file. The work is not volume — it is *fidelity*. Every SAS program already has a PySpark counterpart, but the counterparts are demo-grade: each script re-reads the raw CSV and re-derives upstream logic (`jobs/05` re-implements the LTV and filter logic from `02`), so cleaning rules exist in up to three places and will drift. The test file re-implements the transformations inline rather than importing the scripts, so a bug in `pyspark/02_data_cleaning.py` cannot fail a test today.

**Sizing:** 8 tasks, one session of focused work; no external waits except SAS golden-output extraction (Task 7), which is the one item that can block on someone else's SAS environment.

### 2. Modules to prioritize

Priority is set by *how many downstream modules depend on the module* multiplied by *how far the current PySpark drifts from SAS semantics*.

| Rank | Module | Why first | Fidelity risk today |
|---|---|---|---|
| **P0** | Loading / schema (`sas/01`) | Every other module consumes it; `inferSchema=True` re-derives types per run, whereas `PROC IMPORT ... guessingrows=5960` pinned them once | High — `APPDATE` infers as string, `DEROG`/`DELINQ` may infer as int vs double, changing downstream null/round behavior |
| **P0** | Cleaning (`sas/02`) | Produces `home_equity_final`, the input of `03`, `04`, `05` | High — logic duplicated in three scripts |
| **P1** | Risk segmentation (`sas/04`) | Input to `05`; pure deterministic arithmetic, cheapest to prove exactly equal | Medium — `PROC FORMAT` ranges are hand-inlined; no test pins the score arithmetic against SAS |
| **P2** | Modeling (`sas/05`) | Leaf module, but the only one that cannot be made bit-exact | **Highest** — `PROC SURVEYSELECT method=srs seed=42` != `randomSplit(seed=42)`; `selection=stepwise slentry=0.05 slstay=0.05` != `elasticNetParam=0.8`; no equivalent exists for `lackfit` (Hosmer-Lemeshow) or the `Association` concordance table |
| **P3** | Reporting (`sas/03`) | Leaf; output is human-read, tolerance is loosest | Medium — `PROC TABULATE` has no PySpark equivalent and is currently approximated; `PROC MEANS` percentiles use `approxQuantile` (approximate by construction) |

Do **not** start with reporting even though it looks easiest: it consumes `home_equity_final` and would need reworking after P0.

### 3. External dependencies

| Dependency | Impact | Handling |
|---|---|---|
| **SAS runtime (SAS 9.4 / Viya)** | Needed to produce golden outputs; nobody can prove parity without it | Task 7. If no SAS access, downgrade parity assertions to the self-consistency checks in Tasks 3-6 and record the gap |
| **Upstream dataset** `COG-GTM/sas-viya-quick-start` | `data/home_equity.csv` is a copy; upstream changes silently invalidate golden files | Pin a row-count + column-set + checksum assertion (Task 2) |
| **Absolute SAS path** `/data/home_equity.csv` in `sas/01` | Not a relative path; implies a SAS server mount, not this repo layout | Loader takes a path argument; default resolved from the package root (Task 2) |
| **Java 8/11/17 + PySpark 3.5.1** | Unpinned today; PySpark version changes `approxQuantile`, ML defaults, CSV parsing | Pin in `requirements.txt` and CI (Task 1) |
| **Target platform** (Databricks / EMR / Synapse) | Scripts hardcode `.master("local[*]")`, which fails on a managed cluster | Session builder that only sets master when `SPARK_MASTER` is unset (Task 1) |
| **Directory named `pyspark/` at repo root** | Verified **not** a shadowing bug: with `pyspark==3.5.1` installed, `sys.path.insert(0, '.'); import pyspark` still resolves to site-packages, because a regular package beats a namespace-package candidate. It remains a readability trap (`import pyspark` next to `pyspark/02_data_cleaning.py`) | Rename to `jobs/` (Task 1) for clarity, not correctness |
| **No CI, no lint, no dependency manifest** | Nothing enforces any of the above | Task 1 |

There is no SAS macro library, no `libname` to an RDBMS, no SAS/ACCESS engine, and no ODS destination beyond the default listing — so none of the usual heavyweight migration blockers (stored processes, SAS/CONNECT, EG projects) apply here.

### 4. Parallel work paths

```
Wave 0 (serial, blocks everything)
  Task 1  Packaging, jobs/ rename, session builder, CI     -----+
  Task 2  Explicit schema + metadata (labels/formats)      -----+
                                                                |
Wave 1 (four independent paths, run concurrently)               v
  Path A  Task 3  cleaning.py        (sas/02)   <- P0
  Path B  Task 4  risk.py            (sas/04)   <- P1   (needs Task 3 interface only)
  Path C  Task 5  reporting.py       (sas/03)   <- P3   (needs Task 3 interface only)
  Path D  Task 6  logistic.py        (sas/05)   <- P2   (needs Task 3+4 interfaces only)

Wave 2 (serial)
  Task 7  Parity harness vs SAS golden outputs
  Task 8  jobs/ wrappers, README, deviations register
```

Wave 1 parallelizes because Tasks 4-6 consume Task 3 **through its published signature**, which Task 3's `Interfaces` block fixes before any of them start. Assign one worker (or one subagent) per path. Path D is the long pole — start it first inside the wave.

## Global Constraints

- Python 3.10; PySpark pinned to `3.5.1`; Java 17.
- Package name `homeequity`, `src/` layout, all modules snake_case; local variables stay camelCase to match the existing scripts.
- Column names are UPPERCASE everywhere, matching SAS variable names exactly (`LTV`, `LOAN_OUTCOME`, `RISK_SCORE`, `RISK_SEGMENT`, `LTV_RISK_CAT`, `DTI_RISK_CAT`, `DELINQ_RISK_CAT`, `<COL>_MISS`).
- Dataset invariants: 5,960 rows, 18 source columns, target column `BAD`.
- Deterministic seed: `42` wherever sampling occurs.
- No transformation logic may live in `jobs/` or in `tests/` — both import from `src/homeequity/`.
- Every behavioral difference from SAS is appended to `docs/deviations.md` in the same commit that introduces it.
- Never edit `data/home_equity.csv` or the files under `sas/` — they are the migration source of truth.

---

## File Structure

**Created:**

| Path | Responsibility |
|---|---|
| `pyproject.toml` | Package metadata, ruff + pytest config |
| `requirements.txt` | Pinned runtime/test deps |
| `.github/workflows/ci.yml` | Lint + test on push/PR |
| `src/homeequity/__init__.py` | Package marker, version |
| `src/homeequity/session.py` | `getSparkSession(appName)` — cluster-safe builder |
| `src/homeequity/schema.py` | `HOME_EQUITY_SCHEMA` (`StructType`), `DATA_PATH` |
| `src/homeequity/metadata.py` | `COLUMN_LABELS`, `COLUMN_FORMATS`, `applyLabels()` — the `PROC DATASETS`/`FORMAT` layer |
| `src/homeequity/io.py` | `loadHomeEquity(spark, path)` — the `PROC IMPORT` layer |
| `src/homeequity/transforms/cleaning.py` | `sas/02` logic |
| `src/homeequity/transforms/risk.py` | `sas/04` logic |
| `src/homeequity/reporting.py` | `sas/03` logic |
| `src/homeequity/model/logistic.py` | `sas/05` logic |
| `tests/conftest.py` | Session-scoped `spark` fixture, `rawDf` fixture |
| `tests/test_io.py`, `tests/test_cleaning.py`, `tests/test_risk.py`, `tests/test_reporting.py`, `tests/test_logistic.py` | Per-module unit tests |
| `tests/parity/golden/*.csv` | SAS ODS extracts (four committed via PR #2; `freq_job.csv` pending a SAS rerun) |
| `tests/parity/test_golden_parity.py` | Golden-file comparison (exists on `main`; Task 7 rewires it onto `src/homeequity` and adds JOB coverage) |
| `docs/deviations.md` | Register of every intentional SAS/PySpark difference |

**Renamed:** `pyspark/` -> `jobs/` (5 scripts; contents rewritten in Task 8).

**Deleted:** `tests/test_pyspark_outputs.py` (its 14 assertions are re-homed into the per-module test files across Tasks 2-6; delete only in Task 8, once every assertion has a new home).

---

### Task 1: Project skeleton, `jobs/` rename, session builder, CI

**Files:**
- Create: `pyproject.toml`, `requirements.txt`, `.github/workflows/ci.yml`, `src/homeequity/__init__.py`, `src/homeequity/session.py`, `tests/conftest.py`, `tests/test_session.py`, `docs/deviations.md`
- Modify: rename directory `pyspark/` -> `jobs/`; `README.md` (run commands)
- Test: `tests/test_session.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `homeequity.session.getSparkSession(appName: str) -> pyspark.sql.SparkSession`; pytest fixture `spark` (session-scoped `SparkSession`).

- [ ] **Step 1: Record the current baseline, then rename the directory**

```bash
cd "$(git rev-parse --show-toplevel)"
pip install "pyspark==3.5.1" "pytest==8.3.3" "ruff==0.6.9"
python -m pytest tests -q
```

Expected: `1 failed, 13 passed` — `test_frequency_counts` fails with `5960 != 5681`. The 279-row gap is exactly the null-`JOB` rows: SAS `PROC FREQ` drops missing class values by default, while the demo script's `groupBy` counts every row. That file is deleted in Task 8; its replacements in Task 5 (`test_frequency_excludes_missing_class_by_default`, `test_frequency_include_missing_matches_sas_missing_option`) pin the SAS-faithful semantics on both sides of the divergence. Do not fix the old file.

```bash
git mv pyspark jobs
```

This is a readability rename, not a bug fix: `import pyspark` already resolves to site-packages even with the repo root first on `sys.path` (a regular package outranks a namespace-package candidate).

- [ ] **Step 2: Add the dependency manifest**

`requirements.txt`:

```
pyspark==3.5.1
pytest==8.3.3
ruff==0.6.9
```

- [ ] **Step 3: Add `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "homeequity"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = ["pyspark==3.5.1"]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]

[tool.ruff]
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I"]
```

- [ ] **Step 4: Write the failing test**

`tests/test_session.py`:

```python
from homeequity.session import getSparkSession


def test_session_has_app_name(spark):
    assert spark.conf.get("spark.app.name") == "HomeEquityTests"


def test_session_is_reused():
    first = getSparkSession("HomeEquityTests")
    second = getSparkSession("HomeEquityTests")
    assert first is second
```

`tests/conftest.py`:

```python
import pytest

from homeequity.session import getSparkSession


@pytest.fixture(scope="session")
def spark():
    session = getSparkSession("HomeEquityTests")
    yield session
    session.stop()
```

- [ ] **Step 5: Run the tests to verify they fail**

Run: `pip install -r requirements.txt && python -m pytest tests/test_session.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'homeequity'`

- [ ] **Step 6: Write the minimal implementation**

`src/homeequity/__init__.py`:

```python
__version__ = "0.1.0"
```

`src/homeequity/session.py`:

```python
import os

from pyspark.sql import SparkSession


def getSparkSession(appName: str) -> SparkSession:
    """Build (or fetch) a SparkSession.

    Only forces local mode when no master is supplied by the environment, so the
    same code runs unchanged on Databricks, EMR, and Synapse.
    """
    builder = SparkSession.builder.appName(appName)
    if not os.environ.get("SPARK_MASTER") and not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        builder = builder.master("local[*]")
    builder = builder.config("spark.sql.shuffle.partitions", "4")
    return builder.getOrCreate()
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `python -m pytest tests/test_session.py -v`
Expected: 2 passed

- [ ] **Step 8: Add CI**

`.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: ["**"]
  pull_request:

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
          cache: pip
      - run: pip install -r requirements.txt
      - run: ruff check src tests jobs
      # tests/test_pyspark_outputs.py is the legacy demo suite with a known-red
      # baseline (see Task 1 Step 1); it is deleted in Task 8, which also removes
      # this --ignore so the full tree gates from then on.
      - run: python -m pytest tests -v --ignore=tests/test_pyspark_outputs.py
```

CI/CD requirements binding on every migration PR (including each stacked child-session PR):

- The pytest step runs everything except the legacy demo suite (ignored until Task 8
  deletes it), so the golden parity suite under `tests/parity/` executes on every
  push/PR against the committed SAS extracts — parity is a merge gate, not an optional
  local check. Task 8 must remove the `--ignore` flag in the same PR that deletes
  `tests/test_pyspark_outputs.py`.
- A PR may not merge with a red `test` job; stacked PRs merge bottom-up only after the
  PR below them is green and merged (configure branch protection on `main` to require
  the `test` check once this workflow lands).
- `ruff check src tests jobs` must pass with zero findings on every PR; do not add
  per-file ignores to silence findings introduced by the PR itself.
- Never edit files under `tests/parity/golden/` to make CI pass — golden values only
  change via a real SAS rerun recorded in `tests/parity/golden/PROVENANCE.md`.

- [ ] **Step 9: Seed the deviations register**

`docs/deviations.md`:

```markdown
# SAS -> PySpark Deviations Register

Every intentional behavioral difference between the SAS programs in `sas/` and the
PySpark package in `src/homeequity/`. Add a row in the same commit that introduces
the difference.

| ID | SAS construct | SAS program | PySpark behavior | Why it differs | Impact |
|---|---|---|---|---|---|
| D-001 | `.master("local[*]")` hardcoded | n/a (demo scripts) | Master only forced when `SPARK_MASTER`/`DATABRICKS_RUNTIME_VERSION` are unset | Managed clusters set the master themselves | None; local runs unchanged |
```

- [ ] **Step 10: Run lint**

Run: `ruff check src tests jobs`
Expected: `All checks passed!` (fix any finding before committing). The legacy scripts
carry ~20 pre-existing findings (import order, unused imports, an f-string without
placeholders, a repeated dict key, unnecessary generators). Fix the mechanical ones in
the renamed `jobs/` scripts as part of this task — they do not change behavior. For
`tests/test_pyspark_outputs.py` (deleted in Task 8), add a `[tool.ruff.lint.per-file-ignores]`
entry in `pyproject.toml` instead of editing the file, and remove that entry in Task 8.

- [ ] **Step 11: Commit**

```bash
git add pyproject.toml requirements.txt .github/workflows/ci.yml src tests docs/deviations.md jobs README.md
git commit -m "chore: add package skeleton, session builder, and CI; rename pyspark/ to jobs/"
```

---

### Task 2: Explicit schema, metadata layer, and loader (`sas/01`)

**Files:**
- Create: `src/homeequity/schema.py`, `src/homeequity/metadata.py`, `src/homeequity/io.py`, `tests/test_io.py`
- Test: `tests/test_io.py`

**Interfaces:**
- Consumes: `homeequity.session.getSparkSession` (Task 1); fixture `spark`.
- Produces:
  - `homeequity.schema.HOME_EQUITY_SCHEMA: pyspark.sql.types.StructType`
  - `homeequity.schema.DATA_PATH: str` (absolute path to `data/home_equity.csv`)
  - `homeequity.metadata.COLUMN_LABELS: dict[str, str]`
  - `homeequity.metadata.COLUMN_FORMATS: dict[str, str]`
  - `homeequity.metadata.applyLabels(df: DataFrame) -> DataFrame`
  - `homeequity.io.loadHomeEquity(spark: SparkSession, path: str | None = None) -> DataFrame`
  - Fixture `rawDf` in `tests/conftest.py`.

- [ ] **Step 1: Write the failing test**

`tests/test_io.py`:

```python
from pyspark.sql.types import DoubleType, StringType

from homeequity.metadata import COLUMN_LABELS
from homeequity.schema import HOME_EQUITY_SCHEMA

EXPECTED_COLUMNS = [
    "BAD", "LOAN", "MORTDUE", "VALUE", "REASON", "JOB", "YOJ", "DEROG",
    "DELINQ", "CLAGE", "NINQ", "CLNO", "DEBTINC", "APPDATE", "CITY",
    "STATE", "DIVISION", "REGION",
]


def test_schema_matches_sas_import(rawDf):
    assert rawDf.columns == EXPECTED_COLUMNS
    assert len(HOME_EQUITY_SCHEMA.fields) == 18


def test_row_count_is_pinned(rawDf):
    assert rawDf.count() == 5960


def test_numeric_columns_are_double_not_inferred(rawDf):
    dtypes = dict(rawDf.dtypes)
    for colName in ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ",
                    "CLAGE", "NINQ", "CLNO", "DEBTINC"]:
        assert dtypes[colName] == "double", f"{colName} is {dtypes[colName]}"
    assert isinstance(HOME_EQUITY_SCHEMA["REASON"].dataType, StringType)
    assert isinstance(HOME_EQUITY_SCHEMA["LOAN"].dataType, DoubleType)


def test_appdate_parsed_as_date(rawDf):
    assert dict(rawDf.dtypes)["APPDATE"] == "date"


def test_appdate_epoch_conversion_is_sas_not_unix(rawDf):
    # Raw values 20820-22645 are days since 1960-01-01 (SAS epoch), i.e.
    # 2017-01-01..2021-12-31. A Unix-epoch (1970) mistake shifts every date
    # +3,653 days into 2027-2032, so this range check catches it.
    outside = rawDf.filter("APPDATE < DATE'2017-01-01' OR APPDATE > DATE'2021-12-31'")
    assert outside.count() == 0


def test_string_columns_are_trimmed_at_ingest(rawDf):
    for colName in ["REASON", "JOB", "CITY", "STATE", "DIVISION", "REGION"]:
        assert rawDf.filter(f"{colName} <> trim({colName})").count() == 0


def test_every_column_has_a_label(rawDf):
    for colName in rawDf.columns:
        assert colName in COLUMN_LABELS


def test_labels_are_attached_as_comments(labelledDf):
    field = labelledDf.schema["BAD"]
    assert field.metadata["comment"] == "Loan Status (1=Default, 0=Paid)"
```

Append to `tests/conftest.py`:

```python
from homeequity.io import loadHomeEquity
from homeequity.metadata import applyLabels


@pytest.fixture(scope="session")
def rawDf(spark):
    return loadHomeEquity(spark).cache()


@pytest.fixture(scope="session")
def labelledDf(rawDf):
    return applyLabels(rawDf)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_io.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'homeequity.schema'`

- [ ] **Step 3: Write the schema module**

`src/homeequity/schema.py`:

```python
import os

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(PACKAGE_ROOT, "data", "home_equity.csv")

# Mirrors `proc import ... guessingrows=5960` in sas/01_data_loading.sas.
# SAS numerics are all 8-byte floats, so every numeric column is DoubleType.
HOME_EQUITY_SCHEMA = StructType([
    StructField("BAD", DoubleType(), True),
    StructField("LOAN", DoubleType(), True),
    StructField("MORTDUE", DoubleType(), True),
    StructField("VALUE", DoubleType(), True),
    StructField("REASON", StringType(), True),
    StructField("JOB", StringType(), True),
    StructField("YOJ", DoubleType(), True),
    StructField("DEROG", DoubleType(), True),
    StructField("DELINQ", DoubleType(), True),
    StructField("CLAGE", DoubleType(), True),
    StructField("NINQ", DoubleType(), True),
    StructField("CLNO", DoubleType(), True),
    StructField("DEBTINC", DoubleType(), True),
    # Raw SAS date value (days since 1960-01-01); loadHomeEquity converts it to
    # DateType. See D-014.
    StructField("APPDATE", DoubleType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("DIVISION", StringType(), True),
    StructField("REGION", StringType(), True),
])
```

- [ ] **Step 4: Write the metadata module**

`src/homeequity/metadata.py`:

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# sas/01_data_loading.sas, `proc datasets ... label`.
COLUMN_LABELS = {
    "BAD": "Loan Status (1=Default, 0=Paid)",
    "LOAN": "Amount of Loan Request",
    "MORTDUE": "Amount Due on Existing Mortgage",
    "VALUE": "Value of Current Property",
    "REASON": "Loan Purpose (HomeImp or DebtCon)",
    "JOB": "Job Category",
    "YOJ": "Years at Present Job",
    "DEROG": "Number of Derogatory Reports",
    "DELINQ": "Number of Delinquent Credit Lines",
    "CLAGE": "Age of Oldest Credit Line (months)",
    "NINQ": "Number of Recent Credit Inquiries",
    "CLNO": "Number of Credit Lines",
    "DEBTINC": "Debt to Income Ratio",
    "APPDATE": "Loan Application Date",
    "CITY": "City",
    "STATE": "State",
    "DIVISION": "Census Division",
    "REGION": "Census Region",
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

# sas/01_data_loading.sas, `proc datasets ... format`; sas/02 adds LTV percent8.2.
# Display-only in SAS: these drive formatting helpers, never storage types.
COLUMN_FORMATS = {
    "LOAN": "dollar12.",
    "MORTDUE": "dollar12.",
    "VALUE": "dollar12.",
    "APPDATE": "date9.",
    "DEBTINC": "8.1",
    "CLAGE": "comma8.1",
    "LTV": "percent8.2",
}


def applyLabels(df: DataFrame) -> DataFrame:
    """Attach SAS variable labels as Spark column comments.

    Spark has no label concept; column metadata `comment` is the closest analog and
    surfaces in `DESCRIBE TABLE` and in Databricks catalogs.
    """
    labelled = df
    for colName in df.columns:
        label = COLUMN_LABELS.get(colName)
        if label is not None:
            labelled = labelled.withColumn(colName, col(colName).alias(colName, metadata={"comment": label}))
    return labelled
```

- [ ] **Step 5: Verify the raw extract's encodings before writing the loader**

Two properties of `data/home_equity.csv` decide how the loader must behave; verify both and record the result (Step 8) so the assumptions are auditable.

**5a — `APPDATE` epoch (SAS dates are day counts from 1960-01-01, not 1970-01-01):**

```bash
head -3 data/home_equity.csv | cut -d, -f14
python3 - <<'EOF'
import csv, datetime
with open("data/home_equity.csv", newline="") as handle:
    values = [int(row["APPDATE"]) for row in csv.DictReader(handle) if row["APPDATE"].strip()]
sasEpoch = datetime.date(1960, 1, 1)
print(len(values), min(values), max(values))
print(sasEpoch + datetime.timedelta(days=min(values)),
      sasEpoch + datetime.timedelta(days=max(values)))
EOF
```

Verified 2026-08-19 against the current extract: `APPDATE` is **not** a formatted date string — it arrives as raw SAS date numerics (`22040`, `22229`, ...), range 20820-22645, which is 2017-01-01..2021-12-31 counted from the SAS epoch 1960-01-01. The SAS epoch sits **3,653 days before** the Unix/Spark epoch (1970-01-01), so parsing these values with any Unix-based routine (`from_unixtime`, `to_date` on day counts, etc.) silently shifts every date about ten years into the future. The loader therefore converts explicitly from 1960-01-01 (Step 6) and D-014 records the deviation. If a future extract ships `APPDATE` pre-formatted (e.g. `yyyy-MM-dd` strings), switch the schema field back to `DateType` with the matching `dateFormat`, drop the conversion, and update D-014 — do not keep both paths.

**5b — SAS special missing values (`.A`-`.Z`, `._`) in numeric columns:**

SAS numerics have 28 distinct missing values; `X ne .` is **true** for `.A`-`.Z`/`._`, while Spark has only `null`, so a naive `isNotNull()` translation silently changes filter semantics if special missings are present.

```bash
python3 - <<'EOF'
import csv, re
numericColumns = ["BAD", "LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG",
                  "DELINQ", "CLAGE", "NINQ", "CLNO", "DEBTINC", "APPDATE"]
pattern = re.compile(r"\.[A-Z_]")
hits = {}
with open("data/home_equity.csv", newline="") as handle:
    for row in csv.DictReader(handle):
        for name in numericColumns:
            if pattern.fullmatch(row[name].strip()):
                hits[name] = hits.get(name, 0) + 1
print(hits or "no special missing encodings found")
EOF
```

Verified 2026-08-19 against the current extract: **no special missing encodings are present** — every numeric missing is an empty field, so mapping SAS `.` to Spark `null` and `X ne .` to `isNotNull()` is safe *for this extract*, and D-015 records that assumption. If a future extract does contain them, do **not** let them coerce to `null` silently: parse the affected column as string, split it into a numeric column plus a `<COL>_MISSTYPE` string column preserving the letter, translate every `X ne .` on that column to `isNotNull() | <COL>_MISSTYPE.isNotNull()` (special missings pass SAS `ne .`), and replace D-015 with a row describing that handling.

- [ ] **Step 6: Write the loader**

`src/homeequity/io.py`:

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_add, lit, to_date, trim
from pyspark.sql.types import StringType

from homeequity.schema import DATA_PATH, HOME_EQUITY_SCHEMA

# SAS stores dates as day counts from 1960-01-01 — 3,653 days before the
# Unix/Spark epoch (1970-01-01). Convert from the SAS epoch, never via Unix.
SAS_EPOCH = "1960-01-01"


def loadHomeEquity(spark: SparkSession, path: str | None = None) -> DataFrame:
    """Load the home equity CSV with the pinned schema.

    SAS equivalent: `proc import datafile=... dbms=csv guessingrows=5960`.
    An explicit schema replaces `inferSchema=True` so column types cannot change
    with the data.

    Two mandatory ingest normalizations:
    - Every StringType column is trimmed (blanket policy, not per-column): SAS
      character comparison ignores trailing blanks, Spark string equality is
      exact, so untrimmed values silently split groups and miss join keys (D-013).
    - APPDATE arrives as a raw SAS date numeric and is converted to DateType
      from the 1960-01-01 SAS epoch (D-014).
    """
    df = spark.read.csv(path or DATA_PATH, header=True, schema=HOME_EQUITY_SCHEMA)
    for field in HOME_EQUITY_SCHEMA.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df.withColumn(
        "APPDATE", date_add(to_date(lit(SAS_EPOCH)), col("APPDATE").cast("int"))
    )
```

The trim loop is a **blanket ingest policy**: every `StringType` column in the schema is trimmed, so no downstream module may assume it must trim (and none may skip it by taking a different load path — all loading goes through `loadHomeEquity`).

- [ ] **Step 7: Run the tests**

Run: `python -m pytest tests/test_io.py -v`
Expected: 8 passed. If `test_appdate_epoch_conversion_is_sas_not_unix` fails with dates in 2027-2032, the conversion used the Unix epoch — re-read Step 5a; do not widen the date range.

- [ ] **Step 8: Record the deviations**

Append to `docs/deviations.md`:

```markdown
| D-002 | `proc datasets ... label` / `format` | sas/01 | Labels stored as Spark column `comment` metadata; formats kept as a `COLUMN_FORMATS` dict, applied only at display time | Spark has no variable labels or display formats | Printed output is unformatted unless a formatter is applied |
| D-003 | `guessingrows=5960` type inference | sas/01 | Explicit `HOME_EQUITY_SCHEMA`, all numerics `DoubleType` | Reproducibility: inference varies with data | Types are now stable across data refreshes |
| D-013 | Char comparison pads/ignores trailing blanks | sas/01-05 (all char handling) | `loadHomeEquity` applies `trim()` to every StringType column at ingest, as a blanket policy | SAS char equality ignores trailing blanks; Spark string equality is exact, so untrimmed values silently split `groupBy` groups and miss join keys | None on values without trailing blanks; guarantees group/join keys compare like SAS. Leading blanks are also removed (SAS keeps them) — acceptable for this extract, which has none |
| D-014 | `APPDATE` stored as SAS date numeric (days since 1960-01-01) | sas/01 | Schema reads `APPDATE` as `DoubleType`; `loadHomeEquity` converts via `date_add(to_date('1960-01-01'), APPDATE)` | SAS epoch is 1960-01-01, 3,653 days before the Unix/Spark epoch; Spark has no SAS-date reader | Dates are correct calendar dates (2017-2021 for this extract); any Unix-epoch parse would shift them ~10 years |
| D-015 | Special missing values `.A`-`.Z`, `._` (`X ne .` is true for them) | sas/02, sas/05 null filters | None required: verified 2026-08-19 that the extract contains no special missing encodings, so `.` -> `null` and `X ne .` -> `isNotNull()` are exact | Spark has a single `null`; the extract only uses the single SAS missing `.` | Safe for this extract; re-run the Task 2 Step 5b scan on any data refresh and add handling if special missings appear |
```

- [ ] **Step 9: Commit**

```bash
git add src/homeequity/schema.py src/homeequity/metadata.py src/homeequity/io.py tests/test_io.py tests/conftest.py docs/deviations.md
git commit -m "feat: pin home equity schema, SAS metadata layer, and loader"
```

---

### Task 3: Cleaning transforms (`sas/02`)

**Files:**
- Create: `src/homeequity/transforms/__init__.py`, `src/homeequity/transforms/cleaning.py`, `tests/test_cleaning.py`
- Test: `tests/test_cleaning.py`

**Interfaces:**
- Consumes: `homeequity.io.loadHomeEquity` (Task 2).
- Produces (Tasks 4, 5, 6 all call these):
  - `addDerivedColumns(df: DataFrame) -> DataFrame` — adds `LTV` (double), `LOAN_OUTCOME` (string), rewrites `CITY`
  - `addMissingFlags(df: DataFrame) -> DataFrame` — adds `LOAN_MISS`, `MORTDUE_MISS`, `VALUE_MISS`, `YOJ_MISS`, `DEROG_MISS`, `DELINQ_MISS`, `CLAGE_MISS`, `NINQ_MISS` (all int, 0/1)
  - `filterCriticalNulls(df: DataFrame) -> DataFrame`
  - `removeOutliers(df: DataFrame) -> DataFrame`
  - `buildFinalDataset(df: DataFrame) -> DataFrame` — the full `sas/02` chain, i.e. SAS `work.home_equity_final`
  - `MISSING_FLAG_COLUMNS: list[str]`

- [ ] **Step 1: Write the failing test**

`tests/test_cleaning.py`:

```python
import pytest

from homeequity.transforms.cleaning import (
    MISSING_FLAG_COLUMNS,
    addDerivedColumns,
    addMissingFlags,
    buildFinalDataset,
    filterCriticalNulls,
    removeOutliers,
)


@pytest.fixture(scope="module")
def derivedDf(rawDf):
    return addDerivedColumns(rawDf).cache()


def test_ltv_is_mortdue_over_value(derivedDf):
    row = derivedDf.filter("VALUE > 0 AND MORTDUE IS NOT NULL").first()
    assert row["LTV"] == pytest.approx(row["MORTDUE"] / row["VALUE"], rel=1e-9)


def test_ltv_is_null_when_value_missing_or_zero(derivedDf):
    bad = derivedDf.filter("VALUE IS NULL OR VALUE <= 0 OR MORTDUE IS NULL")
    assert bad.filter("LTV IS NOT NULL").count() == 0


def test_loan_outcome_maps_bad_flag(derivedDf):
    assert derivedDf.filter("BAD = 0 AND LOAN_OUTCOME <> 'Paid'").count() == 0
    assert derivedDf.filter("BAD = 1 AND LOAN_OUTCOME <> 'Default'").count() == 0


def test_city_is_proper_cased(derivedDf):
    cities = [r["CITY"] for r in derivedDf.select("CITY").distinct().collect() if r["CITY"]]
    assert all(city == city.title() for city in cities)


def test_missing_flags_are_zero_or_one_and_match_nulls(derivedDf):
    flagged = addMissingFlags(derivedDf)
    assert len(MISSING_FLAG_COLUMNS) == 8
    assert flagged.filter("LOAN IS NULL AND LOAN_MISS <> 1").count() == 0
    assert flagged.filter("LOAN IS NOT NULL AND LOAN_MISS <> 0").count() == 0


def test_filter_drops_rows_missing_critical_fields(derivedDf):
    filtered = filterCriticalNulls(derivedDf)
    assert filtered.filter("LOAN IS NULL OR VALUE IS NULL OR BAD IS NULL").count() == 0


def test_outlier_filter_bounds_ltv(derivedDf):
    trimmed = removeOutliers(filterCriticalNulls(derivedDf))
    assert trimmed.filter("LTV <= 0 OR LTV >= 5").count() == 0
    assert trimmed.filter("LOAN <= 0 OR VALUE <= 0").count() == 0


def test_outlier_filter_drops_null_ltv_like_sas(derivedDf):
    # SAS subsetting IF on a missing value is false, so `if LTV > 0 and LTV < 5`
    # drops missing-LTV rows; Spark null comparisons drop them too.
    trimmed = removeOutliers(filterCriticalNulls(derivedDf))
    assert trimmed.filter("LTV IS NULL").count() == 0


def test_final_dataset_is_stable(rawDf):
    finalDf = buildFinalDataset(rawDf)
    assert finalDf.count() > 0
    for colName in ["LTV", "LOAN_OUTCOME"] + MISSING_FLAG_COLUMNS:
        assert colName in finalDf.columns
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_cleaning.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'homeequity.transforms'`

- [ ] **Step 3: Write the implementation**

`src/homeequity/transforms/__init__.py`: empty file.

`src/homeequity/transforms/cleaning.py`:

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, initcap, lit, when

NUMERIC_FLAG_SOURCES = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
MISSING_FLAG_COLUMNS = [f"{name}_MISS" for name in NUMERIC_FLAG_SOURCES]


def addDerivedColumns(df: DataFrame) -> DataFrame:
    """SAS: data work.home_equity_clean (sas/02 step 1)."""
    return (
        df.withColumn(
            "LTV",
            when(
                col("VALUE").isNotNull() & col("MORTDUE").isNotNull() & (col("VALUE") > 0),
                col("MORTDUE") / col("VALUE"),
            ),
        )
        .withColumn(
            "LOAN_OUTCOME",
            when(col("BAD") == 0, lit("Paid")).when(col("BAD") == 1, lit("Default")),
        )
        .withColumn("CITY", initcap(col("CITY")))
    )


def addMissingFlags(df: DataFrame) -> DataFrame:
    """SAS: array num_vars / num_flags do-loop (sas/02 step 2)."""
    flagged = df
    for name in NUMERIC_FLAG_SOURCES:
        flagged = flagged.withColumn(
            f"{name}_MISS", when(col(name).isNull(), lit(1)).otherwise(lit(0))
        )
    return flagged


def filterCriticalNulls(df: DataFrame) -> DataFrame:
    """SAS: if LOAN ne . and VALUE ne . and BAD ne . (sas/02 step 3)."""
    return df.filter(col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull())


def removeOutliers(df: DataFrame) -> DataFrame:
    """SAS: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0 (sas/02 step 5)."""
    return df.filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )


def buildFinalDataset(df: DataFrame) -> DataFrame:
    """SAS: work.home_equity_final — the input of sas/03, sas/04, sas/05."""
    return removeOutliers(filterCriticalNulls(addMissingFlags(addDerivedColumns(df))))
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_cleaning.py -v`
Expected: 9 passed. If `test_city_is_proper_cased` fails on a value like `Mcallen`, that is `initcap` vs SAS `propcase` agreeing — both title-case each word; investigate the failing value before changing the assertion.

- [ ] **Step 5: Record the deviation and commit**

Append to `docs/deviations.md`:

```markdown
| D-004 | `propcase(CITY)` | sas/02 | `initcap(CITY)` | Closest Spark builtin | Identical for space-delimited names; differs for hyphenated/apostrophe names (for example `O'Fallon`) |
```

```bash
git add src/homeequity/transforms tests/test_cleaning.py docs/deviations.md
git commit -m "feat: migrate sas/02 cleaning logic into homeequity.transforms.cleaning"
```

---

### Task 4: Risk segmentation transforms (`sas/04`)

**Files:**
- Create: `src/homeequity/transforms/risk.py`, `tests/test_risk.py`
- Test: `tests/test_risk.py`

**Interfaces:**
- Consumes: `homeequity.transforms.cleaning.buildFinalDataset` (Task 3).
- Produces (Task 6 calls these):
  - `addRiskCategories(df: DataFrame) -> DataFrame` — adds `LTV_RISK_CAT`, `DTI_RISK_CAT`, `DELINQ_RISK_CAT`
  - `addRiskScore(df: DataFrame) -> DataFrame` — adds `RISK_SCORE` (double, 0-10)
  - `addRiskSegment(df: DataFrame) -> DataFrame` — adds `RISK_SEGMENT` (string)
  - `buildRiskDataset(df: DataFrame) -> DataFrame` — SAS `work.home_equity_risk`

- [ ] **Step 1: Write the failing test**

`tests/test_risk.py`:

```python
import pytest

from homeequity.transforms.cleaning import buildFinalDataset
from homeequity.transforms.risk import buildRiskDataset


@pytest.fixture(scope="module")
def riskDf(rawDf):
    return buildRiskDataset(buildFinalDataset(rawDf)).cache()


def test_ltv_risk_boundaries(riskDf):
    assert riskDf.filter("LTV < 0.60 AND LTV_RISK_CAT <> 'Low'").count() == 0
    assert riskDf.filter("LTV >= 0.60 AND LTV < 0.80 AND LTV_RISK_CAT <> 'Medium'").count() == 0
    assert riskDf.filter("LTV >= 0.80 AND LTV_RISK_CAT <> 'High'").count() == 0


def test_dti_risk_boundaries(riskDf):
    assert riskDf.filter("DEBTINC < 30 AND DTI_RISK_CAT <> 'Low'").count() == 0
    assert riskDf.filter("DEBTINC >= 30 AND DEBTINC < 40 AND DTI_RISK_CAT <> 'Medium'").count() == 0
    assert riskDf.filter("DEBTINC >= 40 AND DEBTINC < 50 AND DTI_RISK_CAT <> 'High'").count() == 0
    assert riskDf.filter("DEBTINC >= 50 AND DTI_RISK_CAT <> 'Very High'").count() == 0


def test_delinq_risk_boundaries(riskDf):
    assert riskDf.filter("DELINQ = 0 AND DELINQ_RISK_CAT <> 'None'").count() == 0
    assert riskDf.filter("DELINQ = 1 AND DELINQ_RISK_CAT <> 'Low'").count() == 0
    assert riskDf.filter("DELINQ IN (2, 3) AND DELINQ_RISK_CAT <> 'Medium'").count() == 0
    assert riskDf.filter("DELINQ >= 4 AND DELINQ_RISK_CAT <> 'High'").count() == 0


def test_risk_categories_are_null_when_input_missing(riskDf):
    assert riskDf.filter("DEBTINC IS NULL AND DTI_RISK_CAT IS NOT NULL").count() == 0
    assert riskDf.filter("DELINQ IS NULL AND DELINQ_RISK_CAT IS NOT NULL").count() == 0


def test_risk_score_is_never_null_and_in_range(riskDf):
    assert riskDf.filter("RISK_SCORE IS NULL").count() == 0
    assert riskDf.filter("RISK_SCORE < 0 OR RISK_SCORE > 10").count() == 0


def test_risk_score_components_add_up():
    # LTV 0.85 (+3), DEBTINC 55 (+3), DELINQ 5 (+2), DEROG 4 (+2) = 10.0
    from pyspark.sql import Row

    from homeequity.session import getSparkSession
    from homeequity.transforms.risk import addRiskScore

    spark = getSparkSession("HomeEquityTests")
    df = spark.createDataFrame([Row(LTV=0.85, DEBTINC=55.0, DELINQ=5.0, DEROG=4.0)])
    assert addRiskScore(df).first()["RISK_SCORE"] == pytest.approx(10.0)


def test_risk_segment_thresholds(riskDf):
    assert riskDf.filter("RISK_SCORE < 3 AND RISK_SEGMENT <> 'Low Risk'").count() == 0
    assert riskDf.filter("RISK_SCORE >= 3 AND RISK_SCORE < 5 AND RISK_SEGMENT <> 'Medium Risk'").count() == 0
    assert riskDf.filter("RISK_SCORE >= 5 AND RISK_SCORE < 7 AND RISK_SEGMENT <> 'High Risk'").count() == 0
    assert riskDf.filter("RISK_SCORE >= 7 AND RISK_SEGMENT <> 'Very High Risk'").count() == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_risk.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'homeequity.transforms.risk'`

- [ ] **Step 3: Write the implementation**

`src/homeequity/transforms/risk.py`:

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


def addRiskCategories(df: DataFrame) -> DataFrame:
    """SAS: proc format value ltv_risk / dti_risk / delinq_risk applied in sas/04 step 2."""
    return (
        df.withColumn(
            "LTV_RISK_CAT",
            when(col("LTV").isNull(), lit(None).cast("string"))
            .when(col("LTV") < 0.60, lit("Low"))
            .when(col("LTV") < 0.80, lit("Medium"))
            .otherwise(lit("High")),
        )
        .withColumn(
            "DTI_RISK_CAT",
            when(col("DEBTINC").isNull(), lit(None).cast("string"))
            .when(col("DEBTINC") < 30, lit("Low"))
            .when(col("DEBTINC") < 40, lit("Medium"))
            .when(col("DEBTINC") < 50, lit("High"))
            .otherwise(lit("Very High")),
        )
        .withColumn(
            "DELINQ_RISK_CAT",
            when(col("DELINQ").isNull(), lit(None).cast("string"))
            .when(col("DELINQ") == 0, lit("None"))
            .when(col("DELINQ") == 1, lit("Low"))
            .when(col("DELINQ") <= 3, lit("Medium"))
            .otherwise(lit("High")),
        )
    )


def addRiskScore(df: DataFrame) -> DataFrame:
    """SAS: composite RISK_SCORE accumulation in sas/04 step 2.

    A missing component contributes 0, matching the SAS `if X ne . then do;` guards.
    """
    ltvPoints = (
        when(col("LTV").isNull(), lit(0.0))
        .when(col("LTV") >= 0.80, lit(3.0))
        .when(col("LTV") >= 0.60, lit(1.5))
        .otherwise(lit(0.0))
    )
    dtiPoints = (
        when(col("DEBTINC").isNull(), lit(0.0))
        .when(col("DEBTINC") >= 50, lit(3.0))
        .when(col("DEBTINC") >= 40, lit(2.0))
        .when(col("DEBTINC") >= 30, lit(1.0))
        .otherwise(lit(0.0))
    )
    delinqPoints = (
        when(col("DELINQ").isNull(), lit(0.0))
        .when(col("DELINQ") >= 4, lit(2.0))
        .when(col("DELINQ") >= 2, lit(1.5))
        .when(col("DELINQ") == 1, lit(0.5))
        .otherwise(lit(0.0))
    )
    derogPoints = (
        when(col("DEROG").isNull(), lit(0.0))
        .when(col("DEROG") >= 3, lit(2.0))
        .when(col("DEROG") >= 1, lit(1.0))
        .otherwise(lit(0.0))
    )
    return df.withColumn("RISK_SCORE", ltvPoints + dtiPoints + delinqPoints + derogPoints)


def addRiskSegment(df: DataFrame) -> DataFrame:
    """SAS: length RISK_SEGMENT $14 assignment in sas/04 step 2."""
    return df.withColumn(
        "RISK_SEGMENT",
        when(col("RISK_SCORE") < 3, lit("Low Risk"))
        .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
        .when(col("RISK_SCORE") < 7, lit("High Risk"))
        .otherwise(lit("Very High Risk")),
    )


def buildRiskDataset(df: DataFrame) -> DataFrame:
    """SAS: work.home_equity_risk."""
    return addRiskSegment(addRiskScore(addRiskCategories(df)))
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_risk.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/homeequity/transforms/risk.py tests/test_risk.py
git commit -m "feat: migrate sas/04 risk segmentation into homeequity.transforms.risk"
```

---

### Task 5: Reporting and aggregation (`sas/03`)

**Files:**
- Create: `src/homeequity/reporting.py`, `tests/test_reporting.py`
- Test: `tests/test_reporting.py`

**Interfaces:**
- Consumes: `homeequity.transforms.cleaning.buildFinalDataset` (Task 3).
- Produces:
  - `frequencyTable(df: DataFrame, column: str, includeMissing: bool = False) -> DataFrame` — columns `VALUE`, `FREQUENCY`, `PERCENT`; drops null class values by default like `PROC FREQ` (the flag is the SAS `missing` option)
  - `summaryByGroup(df, groupCols: list[str], valueCols: list[str], includeMissing: bool = False) -> DataFrame` — drops rows with a null class value by default like `PROC MEANS` with `CLASS` (the flag is the SAS `missing` option)
  - `crossTabDefaultRate(df, rowCol: str, colCol: str, includeMissing: bool = False) -> DataFrame` — columns `rowCol`, `colCol`, `N`, `DEFAULT_RATE`; drops rows with a null class value by default like `PROC TABULATE`
  - `topStatesByAverageLoan(df, minLoans: int = 10, limit: int = 10) -> DataFrame` — columns `STATE`, `NUM_LOANS`, `AVG_LOAN`, `AVG_PROPERTY_VALUE`, `DEFAULT_RATE`; ordered by `AVG_LOAN` desc with `STATE` asc as a deterministic tie-breaker

- [ ] **Step 1: Write the failing test**

`tests/test_reporting.py`:

```python
import pytest

from homeequity.reporting import (
    crossTabDefaultRate,
    frequencyTable,
    summaryByGroup,
    topStatesByAverageLoan,
)
from homeequity.transforms.cleaning import buildFinalDataset


@pytest.fixture(scope="module")
def finalDf(rawDf):
    return buildFinalDataset(rawDf).cache()


def test_frequency_percentages_sum_to_100(finalDf):
    freq = frequencyTable(finalDf, "LOAN_OUTCOME")
    assert freq.columns == ["VALUE", "FREQUENCY", "PERCENT"]
    total = sum(row["PERCENT"] for row in freq.collect())
    assert total == pytest.approx(100.0, abs=1e-6)


def test_frequency_excludes_missing_class_by_default(finalDf):
    # SAS PROC FREQ drops missing class values unless `missing` is specified,
    # and its percent denominator is the non-missing total.
    rows = frequencyTable(finalDf, "JOB").collect()
    assert None not in [row["VALUE"] for row in rows]
    nonNull = finalDf.filter("JOB IS NOT NULL").count()
    assert sum(row["FREQUENCY"] for row in rows) == nonNull
    assert sum(row["PERCENT"] for row in rows) == pytest.approx(100.0, abs=1e-6)


def test_frequency_include_missing_matches_sas_missing_option(finalDf):
    rows = frequencyTable(finalDf, "JOB", includeMissing=True).collect()
    assert sum(row["FREQUENCY"] for row in rows) == finalDf.count()
    assert sum(row["PERCENT"] for row in rows) == pytest.approx(100.0, abs=1e-6)


def test_summary_by_group_returns_one_row_per_group(finalDf):
    summary = summaryByGroup(finalDf, ["LOAN_OUTCOME"], ["LOAN", "DEBTINC"])
    assert summary.count() == finalDf.select("LOAN_OUTCOME").distinct().count()
    assert "LOAN_MEAN" in summary.columns
    assert "DEBTINC_STDDEV" in summary.columns


def test_summary_by_group_excludes_missing_class_by_default(finalDf):
    groupCols = ["REASON", "LOAN_OUTCOME"]
    valueCols = ["LOAN", "LTV", "DEBTINC"]
    summary = summaryByGroup(finalDf, groupCols, valueCols)
    assert summary.filter("REASON IS NULL").count() == 0
    nonNullReason = finalDf.filter("REASON IS NOT NULL").count()
    assert sum(row["LOAN_COUNT"] for row in summary.collect()) == nonNullReason
    withMissing = summaryByGroup(finalDf, groupCols, valueCols, includeMissing=True)
    assert withMissing.count() > summary.count()
    assert withMissing.filter("REASON IS NULL").count() > 0


def test_crosstab_default_rate_between_zero_and_one(finalDf):
    tab = crossTabDefaultRate(finalDf, "JOB", "REGION")
    assert tab.filter("DEFAULT_RATE < 0 OR DEFAULT_RATE > 1").count() == 0
    assert tab.filter("N <= 0").count() == 0


def test_crosstab_excludes_missing_class_by_default(finalDf):
    tab = crossTabDefaultRate(finalDf, "JOB", "REGION")
    assert tab.filter("JOB IS NULL OR REGION IS NULL").count() == 0
    withMissing = crossTabDefaultRate(finalDf, "JOB", "REGION", includeMissing=True)
    assert withMissing.count() >= tab.count()


def test_top_states_respects_limit_and_having(finalDf):
    top = topStatesByAverageLoan(finalDf, minLoans=10, limit=10)
    rows = top.collect()
    assert len(rows) <= 10
    assert all(row["NUM_LOANS"] >= 10 for row in rows)
    avgLoans = [row["AVG_LOAN"] for row in rows]
    assert avgLoans == sorted(avgLoans, reverse=True)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_reporting.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'homeequity.reporting'`

- [ ] **Step 3: Write the implementation**

`src/homeequity/reporting.py`:

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, expr, lit, mean, stddev
from pyspark.sql.functions import max as sparkMax
from pyspark.sql.functions import min as sparkMin


def frequencyTable(df: DataFrame, column: str, includeMissing: bool = False) -> DataFrame:
    """SAS: proc freq tables <column> / nocum (sas/03 step 1).

    PROC FREQ drops observations with a missing class value and uses the
    non-missing total as the percent denominator unless `missing` is specified;
    `includeMissing=True` is that option. Spark `groupBy` would otherwise emit
    null as its own group and inflate the denominator (D-016).
    """
    counted = df if includeMissing else df.filter(col(column).isNotNull())
    total = counted.count()
    return (
        counted.groupBy(col(column).alias("VALUE"))
        .agg(count(lit(1)).alias("FREQUENCY"))
        .withColumn("PERCENT", col("FREQUENCY") / lit(total) * lit(100.0))
        .orderBy(col("VALUE").asc_nulls_last())
    )


def summaryByGroup(
    df: DataFrame, groupCols: list[str], valueCols: list[str], includeMissing: bool = False
) -> DataFrame:
    """SAS: proc means n mean median std min max; class ...; var ... (sas/03 steps 2, 5).

    PROC MEANS with CLASS drops observations with a missing class value unless
    `MISSING` is specified; `includeMissing=True` models that SAS option (D-016).
    REASON in sas/03 step 5 is the case that matters: 252 nulls occur in the
    raw extract and 159 survive cleaning into the final dataset.
    """
    counted = df
    if not includeMissing:
        for groupCol in groupCols:
            counted = counted.filter(col(groupCol).isNotNull())
    aggregations = []
    for valueCol in valueCols:
        aggregations.extend([
            count(col(valueCol)).alias(f"{valueCol}_COUNT"),
            mean(col(valueCol)).alias(f"{valueCol}_MEAN"),
            expr(f"percentile_approx({valueCol}, 0.5)").alias(f"{valueCol}_MEDIAN"),
            stddev(col(valueCol)).alias(f"{valueCol}_STDDEV"),
            sparkMin(col(valueCol)).alias(f"{valueCol}_MIN"),
            sparkMax(col(valueCol)).alias(f"{valueCol}_MAX"),
        ])
    return counted.groupBy(*groupCols).agg(*aggregations)


def crossTabDefaultRate(
    df: DataFrame, rowCol: str, colCol: str, includeMissing: bool = False
) -> DataFrame:
    """SAS: proc tabulate class JOB REGION; var BAD; (sas/03 step 3).

    PROC TABULATE has no PySpark equivalent; a long-format grouped frame carries the
    same numbers (N and mean of BAD per cell) without the printed layout.
    Like PROC TABULATE, rows with a missing class value are dropped unless
    `includeMissing=True` (the SAS `missing` option; see D-016).
    """
    counted = (
        df
        if includeMissing
        else df.filter(col(rowCol).isNotNull() & col(colCol).isNotNull())
    )
    return (
        counted.groupBy(rowCol, colCol)
        .agg(count(lit(1)).alias("N"), avg(col("BAD")).alias("DEFAULT_RATE"))
        .orderBy(rowCol, colCol)
    )


def topStatesByAverageLoan(df: DataFrame, minLoans: int = 10, limit: int = 10) -> DataFrame:
    """SAS: proc sql outobs=10 ... having count(*) >= 10 order by avg_loan desc (sas/03 step 4).

    STATE is a deterministic secondary sort key: Spark `orderBy` does not
    guarantee stable ordering of ties, so `.limit()` after a single-key sort
    could return different rows across runs (D-017).
    """
    return (
        df.groupBy("STATE")
        .agg(
            count(lit(1)).alias("NUM_LOANS"),
            avg(col("LOAN")).alias("AVG_LOAN"),
            avg(col("VALUE")).alias("AVG_PROPERTY_VALUE"),
            avg(col("BAD")).alias("DEFAULT_RATE"),
        )
        .filter(col("NUM_LOANS") >= lit(minLoans))
        .orderBy(col("AVG_LOAN").desc(), col("STATE").asc())
        .limit(limit)
    )
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_reporting.py -v`
Expected: 8 passed. If `test_frequency_percentages_sum_to_100` fails by a rounding hair, widen only the `abs` tolerance in the test — do not round inside `frequencyTable`, because Task 7 compares its counts to SAS.

- [ ] **Step 5: Record deviations and commit**

Append to `docs/deviations.md`:

```markdown
| D-005 | `proc tabulate` | sas/03 | `crossTabDefaultRate` returns a long-format grouped DataFrame | No PySpark equivalent for TABULATE's printed layout | Same numbers, different presentation |
| D-006 | `proc means` median / percentiles | sas/03, sas/02 | `percentile_approx` / `approxQuantile` | Spark percentiles are approximate on distributed data | Percentile comparisons need a tolerance (see Task 7) |
| D-007 | `proc means` class-level totals | sas/03 | Group rows only; no `all='Total'` row | Spark `groupBy` emits no grand-total row; add `rollup` if needed | Totals must be computed separately |
| D-016 | `PROC FREQ`/`PROC MEANS`/`PROC TABULATE` drop missing class values unless `MISSING` is specified | sas/03, sas/04 | `frequencyTable`, `summaryByGroup`, and `crossTabDefaultRate` filter null class values by default and use the non-null total as the percent denominator where applicable; `includeMissing=True` reproduces the SAS `missing` option | Spark `groupBy` emits null as its own group by default, so a naive translation adds a null row and inflates the percent denominator or emits a null class group | Frequencies and percentages match SAS exactly (e.g. `JOB` has 279 nulls in the raw extract); `summaryByGroup` excludes the 159 null-`REASON` rows surviving cleaning (252 `REASON` nulls in the raw extract); callers wanting null groups must opt in |
| D-017 | `proc sql outobs=10 ... order by avg_loan desc` returns one stable listing | sas/03 | `topStatesByAverageLoan` orders by `AVG_LOAN` desc, then `STATE` asc before `.limit(10)` | Spark `orderBy` does not guarantee stable tie ordering across runs/partitions, so `.limit()` on a single-key sort is nondeterministic under ties | Output is reproducible run-to-run. Related trap: `dropDuplicates()` keeps an arbitrary row per key and is **not** equivalent to `PROC SORT NODUPKEY` (which keeps the first row in sort order); do not introduce it as a NODUPKEY translation |
```

```bash
git add src/homeequity/reporting.py tests/test_reporting.py docs/deviations.md
git commit -m "feat: migrate sas/03 aggregation and reporting into homeequity.reporting"
```

---

### Task 6: Logistic regression (`sas/05`)

**Known SAS-side defect (evidence: `tests/parity/golden/sas_run.log`, PROVENANCE.md):**
the golden-generation run showed `sas/05_logistic_regression.sas` never trains —
`PROC SURVEYSELECT ... method=srs samprate=0.7` without `OUTALL` writes only selected
rows and no `Selected` variable, so `work.train` ends up with 0 observations and
`PROC LOGISTIC` errors out. There is consequently **no SAS model golden to match**;
the PySpark model is validated by its own tests below (determinism, split ratio,
AUC floor), and this defect must be recorded as a deviation row (use the next free
D-0xx ID) rather than reproduced.

**Files:**
- Create: `src/homeequity/model/__init__.py`, `src/homeequity/model/logistic.py`, `tests/test_logistic.py`
- Test: `tests/test_logistic.py`

**Interfaces:**
- Consumes: `homeequity.transforms.cleaning.buildFinalDataset` (Task 3), `homeequity.transforms.risk.buildRiskDataset` (Task 4).
- Produces:
  - `MODEL_PREDICTORS: list[str]`, `CATEGORICAL_PREDICTORS: list[str]`
  - `prepareModelData(df: DataFrame) -> DataFrame`
  - `splitTrainValid(df: DataFrame, trainFraction: float = 0.7, seed: int = 42) -> tuple[DataFrame, DataFrame]`
  - `buildPipeline() -> pyspark.ml.Pipeline`
  - `evaluateModel(scoredDf: DataFrame) -> dict[str, float]` — keys `auc`, `accuracy`, `truePositives`, `trueNegatives`, `falsePositives`, `falseNegatives`
  - `confusionMatrix(scoredDf: DataFrame, threshold: float = 0.5) -> DataFrame`

- [ ] **Step 1: Write the failing test**

`tests/test_logistic.py`:

```python
import pytest

from homeequity.model.logistic import (
    CATEGORICAL_PREDICTORS,
    MODEL_PREDICTORS,
    buildPipeline,
    confusionMatrix,
    evaluateModel,
    prepareModelData,
    splitTrainValid,
)
from homeequity.transforms.cleaning import buildFinalDataset
from homeequity.transforms.risk import buildRiskDataset


@pytest.fixture(scope="module")
def modelDf(rawDf):
    return prepareModelData(buildRiskDataset(buildFinalDataset(rawDf))).cache()


def test_model_data_has_no_missing_predictors(modelDf):
    condition = " OR ".join(f"{name} IS NULL" for name in MODEL_PREDICTORS)
    assert modelDf.filter(condition).count() == 0


def test_split_is_deterministic(modelDf):
    trainA, validA = splitTrainValid(modelDf, seed=42)
    trainB, validB = splitTrainValid(modelDf, seed=42)
    assert trainA.count() == trainB.count()
    assert validA.count() == validB.count()
    assert trainA.count() + validA.count() == modelDf.count()


def test_split_is_roughly_seventy_thirty(modelDf):
    train, valid = splitTrainValid(modelDf, seed=42)
    ratio = train.count() / modelDf.count()
    assert 0.65 <= ratio <= 0.75


def test_pipeline_scores_validation_set(modelDf):
    train, valid = splitTrainValid(modelDf, seed=42)
    model = buildPipeline().fit(train)
    scored = model.transform(valid)
    assert "probability" in scored.columns
    assert "prediction" in scored.columns
    assert scored.count() == valid.count()


def test_auc_beats_random(modelDf):
    train, valid = splitTrainValid(modelDf, seed=42)
    scored = buildPipeline().fit(train).transform(valid)
    metrics = evaluateModel(scored)
    assert metrics["auc"] > 0.6, f"AUC={metrics['auc']}"
    assert 0.0 <= metrics["accuracy"] <= 1.0


def test_confusion_matrix_totals_match(modelDf):
    train, valid = splitTrainValid(modelDf, seed=42)
    scored = buildPipeline().fit(train).transform(valid)
    matrix = confusionMatrix(scored)
    assert sum(row["N"] for row in matrix.collect()) == valid.count()


def test_categorical_predictors_are_encoded():
    assert CATEGORICAL_PREDICTORS == ["JOB", "REASON"]
    stages = buildPipeline().getStages()
    stageNames = [type(stage).__name__ for stage in stages]
    assert stageNames.count("StringIndexer") == 2
    assert "OneHotEncoder" in stageNames
    assert stageNames[-1] == "LogisticRegression"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_logistic.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'homeequity.model'`

- [ ] **Step 3: Write the implementation**

`src/homeequity/model/__init__.py`: empty file.

`src/homeequity/model/logistic.py`:

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit

NUMERIC_PREDICTORS = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ",
]
CATEGORICAL_PREDICTORS = ["JOB", "REASON"]
MODEL_PREDICTORS = NUMERIC_PREDICTORS + CATEGORICAL_PREDICTORS
LABEL_COLUMN = "BAD"
COMPLETE_CASE_COLUMNS = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "CLAGE"]


def prepareModelData(df: DataFrame) -> DataFrame:
    """SAS: data work.model_data — complete cases only (sas/05 step 1).

    DEROG, NINQ, JOB, and REASON are also required by the model matrix, so they are
    filtered too; SAS drops those rows implicitly at fit time.
    """
    condition = None
    for name in COMPLETE_CASE_COLUMNS + ["DEROG", "NINQ", "JOB", "REASON", LABEL_COLUMN]:
        clause = col(name).isNotNull()
        condition = clause if condition is None else condition & clause
    return df.filter(condition)


def splitTrainValid(
    df: DataFrame, trainFraction: float = 0.7, seed: int = 42
) -> tuple[DataFrame, DataFrame]:
    """SAS: proc surveyselect method=srs samprate=0.7 seed=42 (sas/05 step 2).

    `randomSplit` is a Bernoulli split, not SAS simple random sampling, so the
    selected rows differ even with the same seed. Deterministic within Spark.
    """
    train, valid = df.randomSplit([trainFraction, 1.0 - trainFraction], seed=seed)
    return train, valid


def buildPipeline() -> Pipeline:
    """SAS: proc logistic class JOB(ref='Other') REASON(ref='HomeImp') / param=ref."""
    indexers = [
        StringIndexer(inputCol=name, outputCol=f"{name}_IDX", handleInvalid="keep")
        for name in CATEGORICAL_PREDICTORS
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{name}_IDX" for name in CATEGORICAL_PREDICTORS],
        outputCols=[f"{name}_VEC" for name in CATEGORICAL_PREDICTORS],
        dropLast=True,
    )
    assembler = VectorAssembler(
        inputCols=NUMERIC_PREDICTORS + [f"{name}_VEC" for name in CATEGORICAL_PREDICTORS],
        outputCol="features",
    )
    classifier = LogisticRegression(
        featuresCol="features", labelCol=LABEL_COLUMN, maxIter=100, regParam=0.0
    )
    return Pipeline(stages=[*indexers, encoder, assembler, classifier])


def confusionMatrix(scoredDf: DataFrame, threshold: float = 0.5) -> DataFrame:
    """SAS: proc freq tables BAD * PREDICTED_BAD (sas/05 step 5)."""
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType

    probabilityOfDefault = udf(lambda vector: float(vector[1]), DoubleType())
    return (
        scoredDf.withColumn("PRED_PROB", probabilityOfDefault(col("probability")))
        .withColumn("PREDICTED_BAD", (col("PRED_PROB") >= lit(threshold)).cast("int"))
        .groupBy(LABEL_COLUMN, "PREDICTED_BAD")
        .agg(count(lit(1)).alias("N"))
        .orderBy(LABEL_COLUMN, "PREDICTED_BAD")
    )


def evaluateModel(scoredDf: DataFrame, threshold: float = 0.5) -> dict[str, float]:
    """SAS: ROC/AUC from `roc;` plus the confusion matrix counts (sas/05 steps 5-6)."""
    auc = BinaryClassificationEvaluator(
        labelCol=LABEL_COLUMN, rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(scoredDf)
    counts = {
        (int(row[LABEL_COLUMN]), int(row["PREDICTED_BAD"])): int(row["N"])
        for row in confusionMatrix(scoredDf, threshold).collect()
    }
    truePositives = counts.get((1, 1), 0)
    trueNegatives = counts.get((0, 0), 0)
    falsePositives = counts.get((0, 1), 0)
    falseNegatives = counts.get((1, 0), 0)
    total = truePositives + trueNegatives + falsePositives + falseNegatives
    return {
        "auc": auc,
        "accuracy": (truePositives + trueNegatives) / total if total else 0.0,
        "truePositives": truePositives,
        "trueNegatives": trueNegatives,
        "falsePositives": falsePositives,
        "falseNegatives": falseNegatives,
    }
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_logistic.py -v`
Expected: 7 passed

- [ ] **Step 5: Record the modeling deviations**

Append to `docs/deviations.md`:

```markdown
| D-008 | `proc surveyselect method=srs samprate=0.7 seed=42` | sas/05 | `randomSplit([0.7, 0.3], seed=42)` | Spark has no SRS-with-seed equivalent | Different rows selected; coefficients differ slightly from SAS. Reproducible within Spark |
| D-009 | `selection=stepwise slentry=0.05 slstay=0.05` | sas/05 | Full model, no automated selection (`regParam=0.0`) | Spark ML has no p-value stepwise selection | Model retains all predictors; compare AUC, not the variable list. Ask the model owner whether stepwise is a business requirement before adding a manual p-value loop |
| D-010 | `lackfit` (Hosmer-Lemeshow) and the `Association` concordance table | sas/05 | Not produced; `evaluateModel` reports AUC and the confusion matrix | No Spark equivalent | Goodness-of-fit testing must move to a pandas/statsmodels step on sampled data if required |
| D-011 | `class JOB(ref='Other') REASON(ref='HomeImp') / param=ref` | sas/05 | `StringIndexer` orders by frequency; `OneHotEncoder(dropLast=True)` drops the last level | Spark cannot pin a reference level declaratively | Coefficients are relative to a different baseline; predictions are unaffected |
| D-012 | `proc plm restore=...` scoring | sas/05 | `PipelineModel.transform` | Different serialization | Equivalent scoring path |
```

- [ ] **Step 6: Commit**

```bash
git add src/homeequity/model tests/test_logistic.py docs/deviations.md
git commit -m "feat: migrate sas/05 logistic regression into homeequity.model.logistic"
```

---

### Task 7: SAS parity harness

**Status update (2026-08-19, after PR #2 merged):** the golden set is no longer
hypothetical. `main` now contains real SAS 9.4 extracts under `tests/parity/golden/`
(`row_counts.csv`, `freq_loan_outcome.csv`, `means_by_outcome.csv` +
`means_by_outcome_formatted.csv`, `risk_segment_freq.csv`, plus `PROVENANCE.md` and the
full `sas_run.log`), a runnable exporter `sas/99_export_golden.sas`, and a working
harness (`tests/parity/pipeline.py` + `tests/parity/test_golden_parity.py`). This task
therefore changes from "create the harness" to:

1. **Rewire, don't duplicate**: replace the transformation logic duplicated in
   `tests/parity/pipeline.py` with imports from `src/homeequity` (Tasks 2-5 functions),
   keeping the golden comparisons in `tests/parity/test_golden_parity.py` green
   throughout. `pipeline.py` shrinks to (at most) thin adapters; the pinned schema moves
   to `homeequity.schema`. Note `pipeline.py` currently reads `APPDATE` as `DateType`,
   which parses the raw SAS day-count numerics to null — harmless there because no
   parity test touches `APPDATE`, but the rewire must use `loadHomeEquity`'s D-014
   conversion instead.
2. **Add the missing `freq_job` golden**: extend `sas/99_export_golden.sas` with the
   `PROC FREQ tables JOB` block below and add `test_job_frequencies_match_sas`. The
   golden CSV itself requires a SAS rerun (see `tests/parity/golden/README.md` for the
   procedure); until it is produced, only that one test skips — the four existing golden
   comparisons keep running.
3. Keep the existing harness conventions where they conflict with the sketches below:
   golden column names (`STAGE`/`N`, `FREQUENCY`, `<COL>_COUNT/_MEAN/_STDDEV`), the
   module-level skip, exact counts, and 1e-9 tolerances are already set by
   `test_golden_parity.py` and `PROVENANCE.md`.

**Files:**
- Modify: `sas/99_export_golden.sas` (add the `freq_job` export), `tests/parity/pipeline.py` (rewire onto `src/homeequity`), `tests/parity/test_golden_parity.py` (add the JOB test)
- Create: `tests/parity/golden/freq_job.csv` (from a real SAS rerun only; test skips while absent)
- Test: `tests/parity/test_golden_parity.py`

**Interfaces:**
- Consumes: Tasks 3-5 module functions.
- Produces: `tests/parity/test_golden_parity.py` (pytest module, skipped when golden files are absent).

- [ ] **Step 1: Extend the SAS export program**

`sas/99_export_golden.sas` already exists on `main`; add the `freq_job` block and `%dump(freq_job)` so the next SAS rerun emits it. Reference version with the addition:

```sas
/* Export golden outputs for PySpark parity testing. Run after 01-05. */
%let outdir = /data/golden;

/* Row counts at each stage */
proc sql;
    create table work.row_counts as
        select 'home_equity'       as STAGE length=20, count(*) as N from work.home_equity
        union all
        select 'home_equity_final' as STAGE length=20, count(*) as N from work.home_equity_final
        union all
        select 'home_equity_risk'  as STAGE length=20, count(*) as N from work.home_equity_risk;
quit;

proc freq data=work.home_equity_final noprint;
    tables LOAN_OUTCOME / out=work.freq_loan_outcome (rename=(COUNT=FREQUENCY));
run;

/* JOB has ~279 missing values in the raw extract, so this table exercises the
   missing-class divergence: PROC FREQ drops them by default (no MISSING option
   here), and the PySpark side must too. */
proc freq data=work.home_equity_final noprint;
    tables JOB / out=work.freq_job (rename=(COUNT=FREQUENCY));
run;

proc means data=work.home_equity_final noprint;
    class LOAN_OUTCOME;
    var LOAN DEBTINC;
    output out=work.means_by_outcome (where=(_TYPE_=1))
        n=LOAN_COUNT DEBTINC_COUNT
        mean=LOAN_MEAN DEBTINC_MEAN
        std=LOAN_STDDEV DEBTINC_STDDEV;
run;

proc freq data=work.home_equity_risk noprint;
    tables RISK_SEGMENT / out=work.risk_segment_freq (rename=(COUNT=FREQUENCY));
run;

%macro dump(ds);
    proc export data=work.&ds outfile="&outdir./&ds..csv" dbms=csv replace;
    run;
%mend;
%dump(row_counts);
%dump(freq_loan_outcome);
%dump(freq_job);
%dump(means_by_outcome);
%dump(risk_segment_freq);
```

- [ ] **Step 2: Obtain the `freq_job` golden file**

Four golden CSVs are already committed from a real SAS 9.4 run (see
`tests/parity/golden/PROVENANCE.md`). Only `freq_job.csv` is outstanding: it requires
rerunning `01`-`05` + the extended `99_export_golden.sas` in one SAS session per
`tests/parity/golden/README.md`, then committing the CSV together with a refreshed
`PROVENANCE.md`. Until then `test_job_frequencies_match_sas` skips. Do not fabricate
golden values, and never edit committed golden files by hand.

- [ ] **Step 3: Rewire the parity tests onto `src/homeequity`**

Keep `tests/parity/test_golden_parity.py` (its four tests, fixtures, tolerances, and
module-level skip), swap its `pipeline` calls for the package functions, and add the
JOB test. Sketch of the target shape (adapt to the existing file, do not replace it
wholesale):

```python
import csv
import os

import pytest

from homeequity.reporting import frequencyTable, summaryByGroup
from homeequity.transforms.cleaning import buildFinalDataset
from homeequity.transforms.risk import buildRiskDataset

GOLDEN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden")
COUNT_TOLERANCE = 0          # row counts must match exactly
STATISTIC_TOLERANCE = 1e-6   # deterministic statistics
APPROX_TOLERANCE = 0.01      # percentile-based statistics (see D-006)


def loadGolden(name: str) -> list[dict[str, str]]:
    path = os.path.join(GOLDEN_DIR, f"{name}.csv")
    if not os.path.exists(path):
        pytest.skip(f"golden file {name}.csv absent - run sas/99_export_golden.sas")
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture(scope="module")
def finalDf(rawDf):
    return buildFinalDataset(rawDf).cache()


@pytest.fixture(scope="module")
def riskDf(finalDf):
    return buildRiskDataset(finalDf).cache()


def test_row_counts_match_sas(rawDf, finalDf, riskDf):
    golden = {row["STAGE"]: int(float(row["N"])) for row in loadGolden("row_counts")}
    assert rawDf.count() == golden["home_equity"]
    assert finalDf.count() == golden["home_equity_final"]
    assert riskDf.count() == golden["home_equity_risk"]


def test_loan_outcome_frequencies_match_sas(finalDf):
    golden = {row["LOAN_OUTCOME"]: int(float(row["FREQUENCY"])) for row in loadGolden("freq_loan_outcome")}
    actual = {row["VALUE"]: row["FREQUENCY"] for row in frequencyTable(finalDf, "LOAN_OUTCOME").collect()}
    assert actual == golden


def test_job_frequencies_match_sas(finalDf):
    # JOB carries ~279 nulls in the raw extract, so this comparison fails if the
    # PySpark side counts the null group that SAS PROC FREQ drops by default
    # (D-016), or if untrimmed JOB values split a category (D-013).
    golden = {row["JOB"]: int(float(row["FREQUENCY"])) for row in loadGolden("freq_job")}
    actual = {row["VALUE"]: row["FREQUENCY"] for row in frequencyTable(finalDf, "JOB").collect()}
    assert None not in actual
    assert actual == golden
    assert sum(actual.values()) == finalDf.filter("JOB IS NOT NULL").count()


def test_risk_segment_frequencies_match_sas(riskDf):
    golden = {row["RISK_SEGMENT"]: int(float(row["FREQUENCY"])) for row in loadGolden("risk_segment_freq")}
    actual = {row["VALUE"]: row["FREQUENCY"] for row in frequencyTable(riskDf, "RISK_SEGMENT").collect()}
    assert actual == golden


def test_group_means_match_sas(finalDf):
    golden = {row["LOAN_OUTCOME"]: row for row in loadGolden("means_by_outcome")}
    summary = summaryByGroup(finalDf, ["LOAN_OUTCOME"], ["LOAN", "DEBTINC"])
    for row in summary.collect():
        expected = golden[row["LOAN_OUTCOME"]]
        assert row["LOAN_COUNT"] == int(float(expected["LOAN_COUNT"]))
        assert row["LOAN_MEAN"] == pytest.approx(float(expected["LOAN_MEAN"]), rel=STATISTIC_TOLERANCE)
        assert row["LOAN_STDDEV"] == pytest.approx(float(expected["LOAN_STDDEV"]), rel=STATISTIC_TOLERANCE)
        assert row["DEBTINC_MEAN"] == pytest.approx(float(expected["DEBTINC_MEAN"]), rel=STATISTIC_TOLERANCE)
```

- [ ] **Step 4: Run the parity tests**

Run: `python -m pytest tests/parity -v`
Expected with the four committed goldens and no `freq_job.csv`: 4 passed, 1 skipped
(the JOB test, "golden file ... absent"). After the SAS rerun lands `freq_job.csv`:
5 passed.

- [ ] **Step 5: Investigate any mismatch before touching the assertion**

A row-count mismatch means a filter differs (most likely SAS missing-value semantics in `removeOutliers`); a mean mismatch on `DEBTINC` means null handling differs. Fix the module, not the test. Only if the difference is intentional, add it to `docs/deviations.md` and switch that assertion to `APPROX_TOLERANCE`.

- [ ] **Step 6: Commit**

```bash
git add sas/99_export_golden.sas tests/parity
git commit -m "test: rewire parity harness onto homeequity package and add JOB coverage"
```

---

### Task 8: Job wrappers, docs, and retirement of the demo test file

**Files:**
- Modify: `jobs/01_data_loading.py`, `jobs/02_data_cleaning.py`, `jobs/03_aggregation_reporting.py`, `jobs/04_risk_segmentation.py`, `jobs/05_logistic_regression.py`, `README.md`, `migration_guide/sas_to_pyspark_mapping.md`
- Delete: `tests/test_pyspark_outputs.py`
- Test: full suite

**Interfaces:**
- Consumes: every module from Tasks 2-6.
- Produces: five runnable jobs; no new importable API.

- [ ] **Step 1: Confirm every old assertion has a new home**

Run: `grep -n "def test" tests/test_pyspark_outputs.py`
For each of the 14 methods, point at the replacement in `tests/test_io.py`, `tests/test_cleaning.py`, `tests/test_risk.py`, `tests/test_reporting.py`, or `tests/test_logistic.py`. If any behavior is uncovered, add the test to the matching per-module file first, run it, and commit before continuing.

- [ ] **Step 2: Delete the demo test file**

```bash
git rm tests/test_pyspark_outputs.py
```

- [ ] **Step 3: Rewrite a job as a thin wrapper**

`jobs/02_data_cleaning.py`:

```python
"""Job: data cleaning. SAS equivalent: sas/02_data_cleaning.sas."""

from homeequity.io import loadHomeEquity
from homeequity.reporting import summaryByGroup
from homeequity.session import getSparkSession
from homeequity.transforms.cleaning import buildFinalDataset


def main() -> None:
    spark = getSparkSession("HomeEquity_DataCleaning")
    finalDf = buildFinalDataset(loadHomeEquity(spark))

    print("Clean Dataset Summary (SAS: proc means on work.home_equity_final)")
    summaryByGroup(finalDf, ["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]).show(
        truncate=False
    )
    finalDf.select(
        "BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
    ).show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Rewrite the remaining four jobs the same way**

- `jobs/01_data_loading.py`: `loadHomeEquity` -> `applyLabels` -> `printSchema()` -> `show(20)`.
- `jobs/03_aggregation_reporting.py`: `buildFinalDataset` -> `frequencyTable` for `JOB`, `REASON`, `LOAN_OUTCOME`, `REGION` -> `summaryByGroup` -> `crossTabDefaultRate("JOB", "REGION")` -> `topStatesByAverageLoan()`.
- `jobs/04_risk_segmentation.py`: `buildFinalDataset` -> `buildRiskDataset` -> `frequencyTable` for `RISK_SEGMENT`, `LTV_RISK_CAT`, `DTI_RISK_CAT`, `DELINQ_RISK_CAT` -> `summaryByGroup(riskDf, ["RISK_SEGMENT"], ["BAD", "LOAN", "LTV", "DEBTINC"])`.
- `jobs/05_logistic_regression.py`: `buildFinalDataset` -> `buildRiskDataset` -> `prepareModelData` -> `splitTrainValid` -> `buildPipeline().fit(train)` -> `transform(valid)` -> print `evaluateModel(scored)` and `confusionMatrix(scored).show()`.

Each job must contain no transformation logic — only calls, prints, and `spark.stop()`.

- [ ] **Step 5: Run every job end to end**

```bash
for job in jobs/0*.py; do echo "== $job"; PYTHONPATH=src python "$job" || exit 1; done
```

Expected: all five exit 0 and print their tables.

- [ ] **Step 6: Update the docs**

In `README.md`: replace the `python pyspark/0X_....py` commands with `PYTHONPATH=src python jobs/0X_....py`, update the repository-structure tree (`jobs/`, `src/homeequity/`, `docs/`), add an install step (`pip install -r requirements.txt`), and link `docs/deviations.md`.

In `migration_guide/sas_to_pyspark_mapping.md`: add a "Where the code lives now" column or section pointing each SAS construct at the module function that implements it (for example `proc format` -> `homeequity.transforms.risk.addRiskCategories`). Preserve the "Policy note: `MERGE ... BY` is not a SQL join" section already in the guide — `MERGE` is unused in this codebase, but the note is standing policy for future programs (many-to-many BY keys must not be translated to a bare `.join()`), and the same section documents that `dropDuplicates()` is not a `PROC SORT NODUPKEY` equivalent.

- [ ] **Step 7: Run the full suite and lint**

Run: `ruff check src tests jobs && python -m pytest tests -v`
Expected: all tests pass (parity tests may skip), ruff clean.

- [ ] **Step 8: Commit**

```bash
git add jobs README.md migration_guide/sas_to_pyspark_mapping.md
git commit -m "refactor: make jobs thin wrappers over homeequity modules and update docs"
```

---

## Self-Review

**1. Spec coverage** — every SAS program and every question in the brief maps to a task:

| Source requirement | Task |
|---|---|
| `sas/01` PROC IMPORT / labels / formats / CONTENTS / PRINT | 2, 8 (job 01) |
| `sas/02` derived columns, arrays, filters, PROC MEANS | 3, 5 (`summaryByGroup`), 8 |
| `sas/03` PROC FREQ / MEANS / TABULATE / SQL | 5, 8 |
| `sas/04` PROC FORMAT, risk score, segment analysis | 4, 8 |
| `sas/05` SURVEYSELECT, PROC LOGISTIC, PLM, ROC | 6, 8 |
| Q1 scale | Repository Assessment §1 |
| Q2 prioritization | Repository Assessment §2, encoded in the wave map |
| Q3 external dependencies | Repository Assessment §3, handled in Tasks 1, 2, 7 |
| Q4 parallel paths | Repository Assessment §4 |
| Correctness evidence vs SAS | 7 |

**2. Placeholder scan** — no "TBD"/"add error handling"/"similar to Task N" steps; every code step carries runnable code. Two intentional fill-ins remain and are both instructions to a human with SAS access, not to the implementer: the SAS version/date lines in `tests/parity/golden/README.md` (Task 7 Step 2) and the golden CSVs themselves.

**3. Type consistency** — checked across tasks: `loadHomeEquity(spark, path=None) -> DataFrame` (Task 2) is called with one argument in Tasks 3-8; `buildFinalDataset` (Task 3) is the sole input of `buildRiskDataset` (Task 4), `frequencyTable`/`summaryByGroup` (Task 5), and `prepareModelData` (Task 6); `MISSING_FLAG_COLUMNS` is defined once in Task 3 and imported in its test only; `evaluateModel` returns the dict keys asserted in Task 6's tests; `frequencyTable(df, column, includeMissing=False)` emits `VALUE`/`FREQUENCY`/`PERCENT`, which is exactly what Task 7's parity tests read (both `freq_loan_outcome` and `freq_job` use the default missing-dropping call, matching `PROC FREQ` without `MISSING`); `summaryByGroup` emits `<COL>_COUNT`/`_MEAN`/`_MEDIAN`/`_STDDEV`/`_MIN`/`_MAX`, matching both Task 5's and Task 7's assertions and the golden columns emitted by `sas/99_export_golden.sas`.

**4. SAS/PySpark semantic traps** — each known divergence is handled where it bites and carries a register row: trailing-blank char comparison -> blanket ingest `trim()` (Task 2, D-013); SAS 1960 date epoch -> explicit `date_add` conversion plus an epoch-detecting test (Task 2, D-014); special missing values `.A`-`.Z`/`._` -> verified absent, scan documented for refreshes (Task 2, D-015); missing class values in FREQ/MEANS/TABULATE -> dropped by default with an `includeMissing` opt-in, exercised by the `freq_job` golden (Tasks 5+7, D-016); unstable tie ordering under `.limit()` -> deterministic secondary sort key (Task 5, D-017); `MERGE ... BY` many-to-many and `NODUPKEY` -> policy notes in the mapping guide (Task 8).

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-19-full-codebase-migration.md`. Two execution options:

1. **Subagent-Driven (recommended)** — a fresh subagent per task, review between tasks, fast iteration. Wave 1 (Tasks 3-6) runs four subagents concurrently.
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints.
