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
