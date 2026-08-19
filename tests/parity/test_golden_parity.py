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
    "means_by_outcome": "means_by_outcome.csv",
    "risk_segment_freq": "risk_segment_freq.csv",
}

# Counts must match exactly. Continuous statistics are compared with a relative
# tolerance: SAS and Spark both accumulate in IEEE 754 doubles but in a
# different order, so the last couple of digits can differ.
STAT_RTOL = 1e-9
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
