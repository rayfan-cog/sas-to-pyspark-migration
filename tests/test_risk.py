import csv
import os

import pytest

from homeequity.transforms.risk import buildRiskDataset

GOLDEN_SEGMENT_FREQ = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "parity", "golden", "risk_segment_freq.csv"
)


def _buildFinalDataset(rawDf):
    """SAS work.home_equity_final.

    Task 3 publishes `homeequity.transforms.cleaning.buildFinalDataset`; while that
    module is still on a sibling branch of this stack, the parity harness's
    equivalent `sas/02` chain provides the same 5,337-row input.
    """
    try:
        from homeequity.transforms.cleaning import buildFinalDataset
    except ImportError:
        from tests.parity.pipeline import clean as buildFinalDataset
    return buildFinalDataset(rawDf)


@pytest.fixture(scope="module")
def riskDf(rawDf):
    return buildRiskDataset(_buildFinalDataset(rawDf)).cache()


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
    medium = "RISK_SCORE >= 3 AND RISK_SCORE < 5 AND RISK_SEGMENT <> 'Medium Risk'"
    assert riskDf.filter(medium).count() == 0
    high = "RISK_SCORE >= 5 AND RISK_SCORE < 7 AND RISK_SEGMENT <> 'High Risk'"
    assert riskDf.filter(high).count() == 0
    assert riskDf.filter("RISK_SCORE >= 7 AND RISK_SEGMENT <> 'Very High Risk'").count() == 0


def test_segment_frequencies_match_sas_golden(riskDf):
    """RISK_SEGMENT frequencies must equal the SAS `proc freq` golden (sas/04 step 3)."""
    with open(GOLDEN_SEGMENT_FREQ, newline="") as handle:
        golden = {
            row["RISK_SEGMENT"]: (int(row["FREQUENCY"]), float(row["PERCENT"]))
            for row in csv.DictReader(handle)
        }
    assert golden == {
        "Low Risk": (3118, 58.422334645),
        "Medium Risk": (1776, 33.277121979),
        "High Risk": (427, 8.0007494847),
        "Very High Risk": (16, 0.2997938917),
    }

    total = riskDf.count()
    assert total == 5337  # SAS work.home_equity_risk row count
    actual = {
        row["RISK_SEGMENT"]: row["FREQUENCY"]
        for row in riskDf.groupBy("RISK_SEGMENT").count().withColumnRenamed(
            "count", "FREQUENCY"
        ).collect()
    }
    assert actual == {segment: freq for segment, (freq, _) in golden.items()}
    for segment, (freq, percent) in golden.items():
        assert 100.0 * actual[segment] / total == pytest.approx(percent, abs=1e-6)


def test_risk_score_distribution_matches_sas_row_counts(riskDf):
    # Score components are deterministic arithmetic, so the score histogram is
    # exactly reproducible; these counts sum to the 5,337 SAS rows.
    scores = {
        row["RISK_SCORE"]: row["N"]
        for row in riskDf.groupBy("RISK_SCORE").count().withColumnRenamed("count", "N").collect()
    }
    assert sum(scores.values()) == 5337
    assert min(scores) >= 0.0
    assert max(scores) <= 10.0
    assert riskDf.filter("RISK_SEGMENT = 'Very High Risk'").count() == 16
    assert riskDf.filter("LTV_RISK_CAT = 'High'").count() == riskDf.filter("LTV >= 0.80").count()
