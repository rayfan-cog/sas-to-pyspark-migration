import pytest

from homeequity.transforms.cleaning import (
    MISSING_FLAG_COLUMNS,
    addDerivedColumns,
    addMissingFlags,
    buildFinalDataset,
    filterCriticalNulls,
    removeOutliers,
)

# SAS baselines from the golden run recorded in tests/parity/golden/PROVENANCE.md:
# WORK.HOME_EQUITY_FILTERED = 5848 obs, WORK.HOME_EQUITY_FINAL = 5337 obs.
SAS_FILTERED_ROWS = 5848
SAS_FINAL_ROWS = 5337

# Null counts in the raw extract per flagged column; the flag sums must equal
# these exactly because sas/02 flags missing values without imputing them.
RAW_NULL_COUNTS = {
    "LOAN_MISS": 0,
    "MORTDUE_MISS": 518,
    "VALUE_MISS": 112,
    "YOJ_MISS": 515,
    "DEROG_MISS": 708,
    "DELINQ_MISS": 580,
    "CLAGE_MISS": 308,
    "NINQ_MISS": 510,
}


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
    # initcap capitalizes space-delimited words only, while SAS propcase also
    # splits on hyphens (deviation D-004): the extract's single hyphenated city
    # comes out as 'Winston-salem' where SAS produces 'Winston-Salem'.
    cities = [r["CITY"] for r in derivedDf.select("CITY").distinct().collect() if r["CITY"]]
    hyphenated = [city for city in cities if "-" in city]
    assert hyphenated == ["Winston-salem"]
    assert all(city == city.title() for city in cities if "-" not in city)


def test_missing_flags_are_zero_or_one_and_match_nulls(derivedDf):
    flagged = addMissingFlags(derivedDf)
    assert len(MISSING_FLAG_COLUMNS) == 8
    assert flagged.filter("LOAN IS NULL AND LOAN_MISS <> 1").count() == 0
    assert flagged.filter("LOAN IS NOT NULL AND LOAN_MISS <> 0").count() == 0


def test_missing_flag_sums_match_raw_null_counts(derivedDf):
    flagged = addMissingFlags(derivedDf)
    sums = flagged.groupBy().sum(*MISSING_FLAG_COLUMNS).collect()[0]
    for flagCol in MISSING_FLAG_COLUMNS:
        assert sums[f"sum({flagCol})"] == RAW_NULL_COUNTS[flagCol], flagCol


def test_filter_drops_rows_missing_critical_fields(derivedDf):
    filtered = filterCriticalNulls(derivedDf)
    assert filtered.filter("LOAN IS NULL OR VALUE IS NULL OR BAD IS NULL").count() == 0


def test_filter_row_count_matches_sas(derivedDf):
    # SAS log: WORK.HOME_EQUITY_FILTERED has 5848 observations.
    assert filterCriticalNulls(derivedDf).count() == SAS_FILTERED_ROWS


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


def test_final_row_count_matches_sas_golden(rawDf):
    # tests/parity/golden/row_counts.csv: home_equity_final,5337.
    assert buildFinalDataset(rawDf).count() == SAS_FINAL_ROWS


def test_final_flags_still_match_nulls_after_filters(rawDf):
    # Flags are computed before the critical-null and outlier filters, so in the
    # final dataset each flag must still equal the column's nullness row-by-row.
    finalDf = buildFinalDataset(rawDf)
    for flagCol in MISSING_FLAG_COLUMNS:
        srcCol = flagCol.removesuffix("_MISS")
        assert finalDf.filter(f"({srcCol} IS NULL) <> ({flagCol} = 1)").count() == 0, flagCol
