"""Parity harness: PySpark pipeline vs the golden SAS outputs.

The golden CSVs in tests/parity/golden/ were produced by an actual SAS 9.4 run
(see golden/PROVENANCE.md). If they are absent the whole module skips rather
than asserting against invented numbers.
"""

import csv
import os

import pytest

from tests.parity import pipeline

GOLDEN_FILES = {
    "row_counts": "row_counts.csv",
    "freq_loan_outcome": "freq_loan_outcome.csv",
    "freq_job": "freq_job.csv",
    "freq_reason": "freq_reason.csv",
    "means_by_outcome": "means_by_outcome.csv",
    "risk_segment_freq": "risk_segment_freq.csv",
    "freq_ltv_risk_cat": "freq_ltv_risk_cat.csv",
    "freq_dti_risk_cat": "freq_dti_risk_cat.csv",
    "freq_delinq_risk_cat": "freq_delinq_risk_cat.csv",
    "freq_risk_score": "freq_risk_score.csv",
    "crosstab_risk_outcome": "crosstab_risk_outcome.csv",
    "means_by_risk_segment": "means_by_risk_segment.csv",
    "quantiles_numeric": "quantiles_numeric.csv",
    "missing_flag_totals": "missing_flag_totals.csv",
    "edge_row_counts": os.path.join("edge", "row_counts.csv"),
    "edge_clean_rows": os.path.join("edge", "clean_rows.csv"),
    "edge_final_rows": os.path.join("edge", "final_rows.csv"),
    "edge_risk_rows": os.path.join("edge", "risk_rows.csv"),
    "edge_risk_segment_freq": os.path.join("edge", "risk_segment_freq.csv"),
}

SUMMARY_COLUMNS = ("LOAN", "MORTDUE", "VALUE", "DEBTINC", "LTV", "CLAGE", "YOJ", "CLNO")

# Counts must match exactly. Continuous statistics are compared with a relative
# tolerance: SAS and Spark both accumulate in IEEE 754 doubles but in a
# different order, so the last couple of digits can differ.
STAT_RTOL = 1e-9
STAT_ABSTOL = 1e-12
PERCENT_ABSTOL = 1e-9

missing_golden = [
    name for name, filename in GOLDEN_FILES.items()
    if not os.path.exists(os.path.join(pipeline.GOLDEN_DIR, filename))
]

pytestmark = pytest.mark.skipif(
    bool(missing_golden),
    reason=f"golden SAS outputs missing: {missing_golden}; see tests/parity/golden/README.md",
)


def read_golden(name: str) -> list:
    path = os.path.join(pipeline.GOLDEN_DIR, GOLDEN_FILES[name])
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def golden_level(value: str):
    """PROC FREQ writes the missing level as an empty field."""
    return value if value != "" else None


def golden_number(value: str):
    return float(value) if value != "" else None


def assert_frequency(actual: dict, golden: list, column: str):
    assert set(actual) == {golden_level(row[column]) for row in golden}
    for row in golden:
        frequency, percent = actual[golden_level(row[column])]
        assert frequency == int(row["FREQUENCY"]), row[column]
        expected_percent = golden_number(row["PERCENT"])
        if expected_percent is None:
            assert percent is None, row[column]
        else:
            assert percent == pytest.approx(expected_percent, abs=PERCENT_ABSTOL), row[column]


@pytest.fixture(scope="module")
def spark():
    session = pipeline.get_spark()
    yield session
    session.stop()


@pytest.fixture(scope="module")
def frames(spark):
    raw = pipeline.load_home_equity(spark).cache()
    final = pipeline.clean(raw).cache()
    risk = pipeline.segment(final).cache()
    return raw, final, risk


@pytest.fixture(scope="module")
def edge_frames(spark):
    raw = pipeline.load_edge_cases(spark).cache()
    final = pipeline.clean(raw).cache()
    risk = pipeline.segment(final).cache()
    return raw, final, risk


def test_row_counts(frames):
    raw, final, risk = frames
    expected = {row["STAGE"]: int(row["N"]) for row in read_golden("row_counts")}
    assert pipeline.row_counts(raw, final, risk) == expected


def test_freq_loan_outcome(frames):
    _, final, _ = frames
    actual = pipeline.freq_loan_outcome(final)
    golden = read_golden("freq_loan_outcome")

    assert set(actual) == {row["LOAN_OUTCOME"] for row in golden}
    for row in golden:
        frequency, percent = actual[row["LOAN_OUTCOME"]]
        assert frequency == int(row["FREQUENCY"])
        assert percent == pytest.approx(float(row["PERCENT"]), abs=PERCENT_ABSTOL)


def test_risk_segment_freq(frames):
    _, _, risk = frames
    actual = pipeline.risk_segment_freq(risk)
    golden = read_golden("risk_segment_freq")

    assert set(actual) == {row["RISK_SEGMENT"] for row in golden}
    for row in golden:
        frequency, percent = actual[row["RISK_SEGMENT"]]
        assert frequency == int(row["FREQUENCY"])
        assert percent == pytest.approx(float(row["PERCENT"]), abs=PERCENT_ABSTOL)


@pytest.mark.parametrize(
    ("golden_name", "column"),
    [
        ("freq_job", "JOB"),
        ("freq_reason", "REASON"),
    ],
)
def test_categorical_frequencies(frames, golden_name, column):
    _, final, _ = frames
    assert_frequency(pipeline.frequency(final, column), read_golden(golden_name), column)


@pytest.mark.parametrize(
    ("golden_name", "column"),
    [
        ("freq_ltv_risk_cat", "LTV_RISK_CAT"),
        ("freq_dti_risk_cat", "DTI_RISK_CAT"),
        ("freq_delinq_risk_cat", "DELINQ_RISK_CAT"),
    ],
)
def test_risk_category_frequencies(frames, golden_name, column):
    _, _, risk = frames
    assert_frequency(pipeline.frequency(risk, column), read_golden(golden_name), column)


def test_freq_risk_score(frames):
    _, _, risk = frames
    actual = pipeline.frequency(risk, "RISK_SCORE")
    golden = read_golden("freq_risk_score")

    assert set(actual) == {float(row["RISK_SCORE"]) for row in golden}
    for row in golden:
        frequency, percent = actual[float(row["RISK_SCORE"])]
        assert frequency == int(row["FREQUENCY"]), row["RISK_SCORE"]
        assert percent == pytest.approx(float(row["PERCENT"]), abs=PERCENT_ABSTOL)


def test_crosstab_risk_outcome(frames):
    _, _, risk = frames
    actual = pipeline.crosstab_risk_outcome(risk)
    golden = read_golden("crosstab_risk_outcome")

    assert set(actual) == {(row["RISK_SEGMENT"], row["LOAN_OUTCOME"]) for row in golden}
    for row in golden:
        cell = (row["RISK_SEGMENT"], row["LOAN_OUTCOME"])
        frequency, percent = actual[cell]
        assert frequency == int(row["FREQUENCY"]), cell
        assert percent == pytest.approx(float(row["PERCENT"]), abs=PERCENT_ABSTOL), cell


def test_means_by_risk_segment(frames):
    _, _, risk = frames
    actual = pipeline.means_by_risk_segment(risk)
    golden = read_golden("means_by_risk_segment")

    assert set(actual) == {row["RISK_SEGMENT"] for row in golden}
    for row in golden:
        stats = actual[row["RISK_SEGMENT"]]
        assert stats["_FREQ_"] == int(row["_FREQ_"]), row["RISK_SEGMENT"]
        for variable in ("BAD", "LOAN", "LTV", "DEBTINC"):
            assert stats[f"{variable}_COUNT"] == int(row[f"{variable}_COUNT"]), variable
            for statistic in ("MEAN", "STDDEV"):
                column = f"{variable}_{statistic}"
                assert stats[column] == pytest.approx(
                    float(row[column]), rel=STAT_RTOL, abs=STAT_ABSTOL
                ), column


def test_numeric_summary(frames):
    _, final, _ = frames
    actual = pipeline.numeric_summary(final, SUMMARY_COLUMNS)
    golden = read_golden("quantiles_numeric")[0]

    for column in SUMMARY_COLUMNS:
        stats = actual[column]
        assert stats["N"] == int(golden[f"{column}_N"]), column
        assert stats["NMISS"] == int(golden[f"{column}_NMISS"]), column
        for statistic in ("MIN", "P25", "MEDIAN", "P75", "MAX"):
            assert stats[statistic] == pytest.approx(
                float(golden[f"{column}_{statistic}"]), rel=STAT_RTOL
            ), f"{column}_{statistic}"


def test_missing_flag_totals(frames):
    _, final, _ = frames
    golden = read_golden("missing_flag_totals")[0]
    expected = {
        f"{column}_MISS": int(golden[f"{column}_MISS"])
        for column in pipeline.MISSING_FLAG_COLUMNS
    }
    assert pipeline.missing_flag_totals(final) == expected


def test_means_by_outcome(frames):
    _, final, _ = frames
    actual = pipeline.means_by_outcome(final)
    golden = read_golden("means_by_outcome")

    assert set(actual) == {row["LOAN_OUTCOME"] for row in golden}
    for row in golden:
        stats = actual[row["LOAN_OUTCOME"]]
        assert stats["LOAN_COUNT"] == int(row["LOAN_COUNT"])
        assert stats["DEBTINC_COUNT"] == int(row["DEBTINC_COUNT"])
        for column in ("LOAN_MEAN", "DEBTINC_MEAN", "LOAN_STDDEV", "DEBTINC_STDDEV"):
            assert stats[column] == pytest.approx(float(row[column]), rel=STAT_RTOL), column

# --- Edge-case fixture -------------------------------------------------------
# data/home_equity_edge.csv is 38 synthetic rows sitting on every threshold in
# 02 and 04. SAS exported these row by row (CITY carries the case name), so a
# divergence points at the exact rule that differs instead of at an aggregate
# where two errors can cancel out.


def edge_rows_by_city(df, columns) -> dict:
    # Projected before collecting: APPDATE holds raw SAS date numbers that
    # Python's date type cannot represent, and no golden output uses it.
    rows = df.select("CITY", *columns).collect()
    return {row["CITY"]: {column: row[column] for column in columns} for row in rows}


def assert_rows_match(actual: dict, golden: list, numeric: tuple, text: tuple):
    assert set(actual) == {row["CITY"] for row in golden}
    for row in golden:
        observed = actual[row["CITY"]]
        for column in numeric:
            expected = golden_number(row[column])
            if expected is None:
                assert observed[column] is None, f"{row['CITY']}/{column}"
            else:
                assert observed[column] == pytest.approx(expected, rel=STAT_RTOL), (
                    f"{row['CITY']}/{column}"
                )
        for column in text:
            assert observed[column] == golden_level(row[column]), f"{row['CITY']}/{column}"


def test_edge_row_counts(edge_frames):
    raw, final, risk = edge_frames
    golden = {row["STAGE"]: int(row["N"]) for row in read_golden("edge_row_counts")}
    assert raw.count() == golden["home_equity"]
    assert final.count() == golden["home_equity_final"]
    assert risk.count() == golden["home_equity_risk"]


def test_edge_derived_columns_before_filtering(spark):
    """LTV and LOAN_OUTCOME for every fixture row, including the dropped ones."""
    unfiltered = pipeline.clean_derivations(pipeline.load_edge_cases(spark))
    golden = read_golden("edge_clean_rows")
    assert_rows_match(
        edge_rows_by_city(unfiltered, ("LTV", "LOAN_OUTCOME")),
        golden,
        numeric=("LTV",),
        text=("LOAN_OUTCOME",),
    )


def test_edge_missing_flags(edge_frames):
    _, final, _ = edge_frames
    flags = tuple(f"{column}_MISS" for column in pipeline.MISSING_FLAG_COLUMNS)
    assert_rows_match(
        edge_rows_by_city(final, ("LTV",) + flags),
        read_golden("edge_final_rows"),
        numeric=("LTV",) + flags,
        text=(),
    )


def test_edge_risk_assignment(edge_frames):
    _, _, risk = edge_frames
    categories = ("LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT", "RISK_SEGMENT")
    assert_rows_match(
        edge_rows_by_city(risk, ("RISK_SCORE",) + categories),
        read_golden("edge_risk_rows"),
        numeric=("RISK_SCORE",),
        text=categories,
    )


def test_edge_risk_segment_freq(edge_frames):
    _, _, risk = edge_frames
    assert_frequency(
        pipeline.risk_segment_freq(risk),
        read_golden("edge_risk_segment_freq"),
        "RISK_SEGMENT",
    )
