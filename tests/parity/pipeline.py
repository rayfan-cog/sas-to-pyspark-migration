"""Adapters between the `homeequity` package and the golden SAS extracts.

Every transformation and statistic comes from `src/homeequity` — the modules the
jobs run — so a parity failure points at production code, not at a copy of it
kept in the test tree. The functions here only reshape package output into the
`{key: value}` form the golden CSVs are compared in.

Artifacts mirrored from sas/99_export_golden.sas: row_counts, freq_loan_outcome,
freq_job, means_by_outcome, risk_segment_freq.
"""

import os

from pyspark.sql import DataFrame

from homeequity.reporting import frequencyTable, summaryByGroup
from homeequity.transforms.cleaning import buildFinalDataset
from homeequity.transforms.risk import buildRiskDataset

GOLDEN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden")


def clean(df: DataFrame) -> DataFrame:
    """sas/02_data_cleaning.sas -> work.home_equity_final."""
    return buildFinalDataset(df)


def segment(df: DataFrame) -> DataFrame:
    """sas/04_risk_segmentation.sas -> work.home_equity_risk."""
    return buildRiskDataset(df)


def row_counts(raw: DataFrame, final: DataFrame, risk: DataFrame) -> dict:
    """Counterpart of WORK.ROW_COUNTS in sas/99_export_golden.sas."""
    return {
        "home_equity": raw.count(),
        "home_equity_final": final.count(),
        "home_equity_risk": risk.count(),
    }


def _frequency(df: DataFrame, column: str) -> dict:
    """Counterpart of PROC FREQ output: {level: (frequency, percent)}."""
    return {
        row["VALUE"]: (row["FREQUENCY"], row["PERCENT"])
        for row in frequencyTable(df, column).collect()
    }


def freq_loan_outcome(final: DataFrame) -> dict:
    return _frequency(final, "LOAN_OUTCOME")


def freq_job(final: DataFrame) -> dict:
    return _frequency(final, "JOB")


def risk_segment_freq(risk: DataFrame) -> dict:
    return _frequency(risk, "RISK_SEGMENT")


def means_by_outcome(final: DataFrame) -> dict:
    """Counterpart of PROC MEANS with CLASS LOAN_OUTCOME, VAR LOAN DEBTINC.

    PROC MEANS excludes missing values per analysis variable, which is what
    Spark's count/mean/stddev do as well; stddev is the sample (n-1) form in
    both engines.
    """
    rows = summaryByGroup(
        final.filter(final["LOAN_OUTCOME"].isNotNull()), ["LOAN_OUTCOME"], ["LOAN", "DEBTINC"]
    ).collect()
    return {row["LOAN_OUTCOME"]: row.asDict() for row in rows}
