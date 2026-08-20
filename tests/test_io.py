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


def test_job_null_count_matches_sas_baseline(rawDf):
    # SAS PROC FREQ on JOB reports 5,681 non-missing rows out of 5,960: the
    # 279-row gap is the null-JOB rows (see D-016 groundwork in the plan).
    assert rawDf.filter("JOB IS NULL").count() == 279


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
