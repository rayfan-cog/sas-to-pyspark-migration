# SAS to PySpark Migration Demo

This repository demonstrates how [Devin](https://devin.ai) can accelerate SAS-to-PySpark migration for financial services organizations. Using a real-world home equity loan dataset, we show five complete migration examples — from basic data loading to logistic regression modeling — with side-by-side SAS and PySpark code, inline comments mapping each SAS construct to its PySpark equivalent, and validation tests to ensure correctness.

Built for teams evaluating a move from SAS to open-source distributed computing on platforms like Databricks, Amazon EMR, or Azure Synapse.

---

## Why Migrate?

| Driver | Details |
|---|---|
| **Scalability** | PySpark runs on distributed clusters, processing terabytes of data across hundreds of nodes — far beyond SAS's single-server architecture. |
| **Cost** | PySpark is open-source (Apache 2.0). Eliminating SAS licensing fees can save millions annually for large enterprises. |
| **Cloud-Native** | PySpark is the native compute engine for Databricks, Amazon EMR, Azure Synapse, and Google Dataproc — enabling seamless cloud adoption. |
| **Modern ML Ecosystem** | Direct integration with scikit-learn, TensorFlow, PyTorch, MLflow, and the broader Python data science ecosystem. |

---

## Migration Examples

| # | SAS Program | Job wrapper | Package module | Description |
|---|---|---|---|---|
| 1 | `sas/01_data_loading.sas` | `jobs/01_data_loading.py` | `homeequity.io`, `homeequity.metadata` | Load CSV with a pinned schema, apply labels/formats, inspect metadata, preview data |
| 2 | `sas/02_data_cleaning.sas` | `jobs/02_data_cleaning.py` | `homeequity.transforms.cleaning` | Derived columns (LTV, loan outcome), missing flags, null and outlier filters |
| 3 | `sas/03_aggregation_reporting.sas` | `jobs/03_aggregation_reporting.py` | `homeequity.reporting` | Frequency tables, summary statistics, cross-tabulation, top-states query |
| 4 | `sas/04_risk_segmentation.sas` | `jobs/04_risk_segmentation.py` | `homeequity.transforms.risk` | Risk bucketing (LTV, DTI, delinquency), composite scoring, segment analysis |
| 5 | `sas/05_logistic_regression.sas` | `jobs/05_logistic_regression.py` | `homeequity.model.logistic` | Logistic regression pipeline, train/valid split, AUC and confusion matrix |

All transformation logic lives in the `src/homeequity/` package; the `jobs/` scripts are
thin wrappers that call it and print the results. Every intentional SAS/PySpark
behavioral difference is recorded in [`docs/deviations.md`](docs/deviations.md).

---

## How Devin Helps

Devin automates the heavy lifting of SAS-to-PySpark migration:

1. **Reads and understands SAS code** — Devin parses DATA steps, PROC procedures, macro variables, and SAS-specific constructs to understand the full intent of each program.

2. **Generates idiomatic PySpark equivalents** — Rather than a literal line-by-line translation, Devin produces clean, Pythonic PySpark code that follows Spark best practices (lazy evaluation, DataFrame API, ML Pipelines).

3. **Writes validation tests** — Devin automatically generates test suites that verify row counts, column existence, derived column calculations, and model outputs match expected results.

4. **Documents the migration mapping** — Devin produces comprehensive reference guides mapping every SAS construct (PROC MEANS, PROC FREQ, DATA step arrays, macro variables) to its PySpark equivalent.

---

## Migration Patterns

The [`migration_guide/`](migration_guide/) folder contains detailed reference documentation:

- **[SAS to PySpark Mapping](migration_guide/sas_to_pyspark_mapping.md)** — Complete reference table covering DATA step operations, statistical procedures, ML, macro language, and data management constructs.
- **[Common Patterns](migration_guide/common_patterns.md)** — Seven detailed side-by-side examples of the most frequently encountered migration patterns, including data loading, conditional logic, missing values, aggregation, joins, macros, and output writing.

---

## Getting Started

### Prerequisites

- Python 3.10+
- Java 8, 11, or 17 (required by PySpark)

### Installation

```bash
pip install -r requirements.txt
```

### Running the Jobs

Each job imports the `homeequity` package from `src/`, so run it from the repository
root with `src` on the path (`data/home_equity.csv` is resolved relative to the root):

```bash
# From the repository root directory
PYTHONPATH=src python jobs/01_data_loading.py
PYTHONPATH=src python jobs/02_data_cleaning.py
PYTHONPATH=src python jobs/03_aggregation_reporting.py
PYTHONPATH=src python jobs/04_risk_segmentation.py
PYTHONPATH=src python jobs/05_logistic_regression.py
```

### Running the Tests

`pyproject.toml` already puts `src` on the pytest path, so no `PYTHONPATH` is needed:

```bash
python -m pytest tests -v
```

The suite covers each package module (`tests/test_io.py`, `test_cleaning.py`,
`test_reporting.py`, `test_risk.py`, `test_logistic.py`, `test_session.py`) plus the SAS
golden parity tests under `tests/parity/`.

---

### SAS parity (golden outputs)

Proving the PySpark code matches SAS requires reference numbers from a real SAS run.
`sas/99_export_golden.sas` exports them and the checked-in set under
[`tests/parity/golden/`](tests/parity/golden/README.md) came from an actual SAS 9.4
session (provenance and log committed alongside it). Compare against them with:

```bash
python -m pytest tests/parity -v
```

`tests/parity/pipeline.py` reimplements loading, cleaning and risk segmentation as
importable functions, and `tests/parity/test_golden_parity.py` asserts row counts and
frequencies exactly and continuous statistics to 1e-9. The tests skip themselves if the
golden files are absent — never hand-write them.

---

## Dataset

The `data/home_equity.csv` file contains 5,960 home equity loan records with 18 columns:

| Column | Description |
|---|---|
| `BAD` | Loan default flag (1 = default, 0 = paid) |
| `LOAN` | Amount of loan request |
| `MORTDUE` | Amount due on existing mortgage |
| `VALUE` | Value of current property |
| `REASON` | Loan purpose (HomeImp or DebtCon) |
| `JOB` | Job category |
| `YOJ` | Years at present job |
| `DEROG` | Number of derogatory reports |
| `DELINQ` | Number of delinquent credit lines |
| `CLAGE` | Age of oldest credit line (months) |
| `NINQ` | Number of recent credit inquiries |
| `CLNO` | Number of credit lines |
| `DEBTINC` | Debt to income ratio |
| `APPDATE` | Loan application date |
| `CITY` | City |
| `STATE` | State |
| `DIVISION` | Census division |
| `REGION` | Census region |

Source: [COG-GTM/sas-viya-quick-start](https://github.com/COG-GTM/sas-viya-quick-start)

---

## Repository Structure

```
sas-to-pyspark-migration/
├── README.md                                  # This file
├── data/
│   └── home_equity.csv                        # Home equity loan dataset (5,960 records)
├── sas/
│   ├── 01_data_loading.sas                    # PROC IMPORT, PROC CONTENTS, PROC PRINT
│   ├── 02_data_cleaning.sas                   # DATA step, arrays, PROC MEANS
│   ├── 03_aggregation_reporting.sas           # PROC FREQ, PROC MEANS, PROC TABULATE, PROC SQL
│   ├── 04_risk_segmentation.sas               # PROC FORMAT, risk scoring, PROC FREQ
│   ├── 05_logistic_regression.sas             # PROC LOGISTIC, stepwise, ROC/AUC
│   └── 99_export_golden.sas                   # Exports golden outputs for parity testing
├── src/homeequity/                            # The migrated package (all logic lives here)
│   ├── session.py                             # SparkSession builder (cluster-safe master)
│   ├── schema.py                              # Pinned StructType replacing inferSchema
│   ├── io.py                                  # loadHomeEquity: PROC IMPORT equivalent
│   ├── metadata.py                            # SAS labels/formats + applyLabels
│   ├── transforms/cleaning.py                 # Derived columns, missing flags, filters
│   ├── transforms/risk.py                     # PROC FORMAT buckets, risk score, segment
│   ├── reporting.py                           # PROC FREQ / MEANS / TABULATE / SQL
│   └── model/logistic.py                      # PROC LOGISTIC pipeline and evaluation
├── jobs/                                      # Thin runnable wrappers over the package
│   ├── 01_data_loading.py                     # loadHomeEquity, applyLabels, printSchema
│   ├── 02_data_cleaning.py                    # buildFinalDataset, summaryByGroup
│   ├── 03_aggregation_reporting.py            # frequencyTable, crossTab, topStates
│   ├── 04_risk_segmentation.py                # buildRiskDataset, segment frequencies
│   └── 05_logistic_regression.py              # buildPipeline, evaluateModel
├── docs/
│   ├── deviations.md                          # Register of intentional SAS/PySpark diffs
│   └── superpowers/plans/                     # Migration plans (read the newest first)
├── migration_guide/
│   ├── sas_to_pyspark_mapping.md              # Complete SAS → PySpark construct reference
│   └── common_patterns.md                     # Side-by-side migration pattern examples
└── tests/
    ├── test_io.py, test_cleaning.py, ...      # One test module per package module
    └── parity/
        ├── pipeline.py                        # PySpark reimplementation used for comparison
        ├── test_golden_parity.py              # Asserts PySpark output == SAS golden output
        └── golden/                            # SAS reference outputs (see its README)
```

---

## License

This project is provided as a demonstration. The home equity dataset is sourced from [SAS Viya Quick Start](https://github.com/COG-GTM/sas-viya-quick-start) materials.
