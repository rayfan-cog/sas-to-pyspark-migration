"""Reference PySpark pipeline used by the parity harness.

Mirrors sas/01_data_loading.sas -> sas/04_risk_segmentation.sas so the four
artifacts exported by sas/99_export_golden.sas can be reproduced in Spark and
compared against the golden SAS outputs.

The scripts under pyspark/ are standalone demonstrations that print to stdout;
this module reimplements the same logic as importable functions returning
DataFrames.
"""

import os
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit, mean, stddev, udf
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")
EDGE_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity_edge.csv")
GOLDEN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden")
EDGE_GOLDEN_DIR = os.path.join(GOLDEN_DIR, "edge")

# Pinned to mirror SAS 8-byte numerics; inferSchema would give LongType for
# integral columns and change division/aggregation behaviour.
SCHEMA = StructType(
    [
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
        StructField("APPDATE", DateType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("DIVISION", StringType(), True),
        StructField("REGION", StringType(), True),
    ]
)

MISSING_FLAG_COLUMNS = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]


def get_spark(app_name: str = "HomeEquity_Parity") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_home_equity(spark: SparkSession, data_path: str = DATA_PATH) -> DataFrame:
    """sas/01_data_loading.sas: PROC IMPORT of data/home_equity.csv."""
    return spark.read.csv(data_path, header=True, schema=SCHEMA)


def load_edge_cases(spark: SparkSession) -> DataFrame:
    """sas/97_load_edge_cases.sas: PROC IMPORT of the synthetic boundary fixture."""
    return spark.read.csv(EDGE_DATA_PATH, header=True, schema=SCHEMA)


# SAS PROPCASE capitalises the first letter after a blank, forward slash,
# hyphen, open parenthesis, period or tab. Spark's initcap only breaks on
# whitespace, so it renders "winston-salem" as "Winston-salem" where SAS gives
# "Winston-Salem" - the edge fixture pins that difference down.
# pyspark/02_data_cleaning.py currently uses initcap.
_PROPCASE_WORD = re.compile(r"(^|[ /\-(.\t])([a-z])")


@udf(returnType=StringType())
def propcase(value):
    if value is None:
        return None
    return _PROPCASE_WORD.sub(lambda match: match.group(1) + match.group(2).upper(), value.lower())


def clean_derivations(df: DataFrame) -> DataFrame:
    """sas/02_data_cleaning.sas up to (but excluding) its WHERE filters."""
    cleaned = (
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
        .withColumn("CITY", propcase(col("CITY")))
    )

    for column in MISSING_FLAG_COLUMNS:
        cleaned = cleaned.withColumn(
            f"{column}_MISS", when(col(column).isNull(), lit(1)).otherwise(lit(0))
        )

    return cleaned


def clean(df: DataFrame) -> DataFrame:
    """sas/02_data_cleaning.sas: derived columns, missing flags, filters."""
    filtered = clean_derivations(df).filter(
        col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull()
    )

    return filtered.filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )


def segment(df: DataFrame) -> DataFrame:
    """sas/04_risk_segmentation.sas: risk categories, score and segment."""
    ltv_score = (
        when(col("LTV").isNull(), lit(0.0))
        .when(col("LTV") >= 0.80, lit(3.0))
        .when(col("LTV") >= 0.60, lit(1.5))
        .otherwise(lit(0.0))
    )
    dti_score = (
        when(col("DEBTINC").isNull(), lit(0.0))
        .when(col("DEBTINC") >= 50, lit(3.0))
        .when(col("DEBTINC") >= 40, lit(2.0))
        .when(col("DEBTINC") >= 30, lit(1.0))
        .otherwise(lit(0.0))
    )
    delinq_score = (
        when(col("DELINQ").isNull(), lit(0.0))
        .when(col("DELINQ") >= 4, lit(2.0))
        .when(col("DELINQ") >= 2, lit(1.5))
        .when(col("DELINQ") == 1, lit(0.5))
        .otherwise(lit(0.0))
    )
    derog_score = (
        when(col("DEROG").isNull(), lit(0.0))
        .when(col("DEROG") >= 3, lit(2.0))
        .when(col("DEROG") >= 1, lit(1.0))
        .otherwise(lit(0.0))
    )

    return (
        df.withColumn(
            "LTV_RISK_CAT",
            when(col("LTV").isNull(), lit(None).cast(StringType()))
            .when(col("LTV") < 0.60, lit("Low"))
            .when(col("LTV") < 0.80, lit("Medium"))
            .otherwise(lit("High")),
        )
        .withColumn(
            "DTI_RISK_CAT",
            when(col("DEBTINC").isNull(), lit(None).cast(StringType()))
            .when(col("DEBTINC") < 30, lit("Low"))
            .when(col("DEBTINC") < 40, lit("Medium"))
            .when(col("DEBTINC") < 50, lit("High"))
            .otherwise(lit("Very High")),
        )
        .withColumn(
            "DELINQ_RISK_CAT",
            when(col("DELINQ").isNull(), lit(None).cast(StringType()))
            .when(col("DELINQ") == 0, lit("None"))
            .when(col("DELINQ") == 1, lit("Low"))
            .when(col("DELINQ") <= 3, lit("Medium"))
            .otherwise(lit("High")),
        )
        .withColumn("RISK_SCORE", ltv_score + dti_score + delinq_score + derog_score)
        .withColumn(
            "RISK_SEGMENT",
            when(col("RISK_SCORE") < 3, lit("Low Risk"))
            .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
            .when(col("RISK_SCORE") < 7, lit("High Risk"))
            .otherwise(lit("Very High Risk")),
        )
    )


def row_counts(raw: DataFrame, final: DataFrame, risk: DataFrame) -> dict:
    """Counterpart of WORK.ROW_COUNTS in sas/99_export_golden.sas."""
    return {
        "home_equity": raw.count(),
        "home_equity_final": final.count(),
        "home_equity_risk": risk.count(),
    }


def frequency(df: DataFrame, column: str) -> dict:
    """Counterpart of PROC FREQ out=: {level: (frequency, percent)}.

    PROC FREQ reports the missing level with a frequency but no percent, and
    computes percents over the non-missing rows only; the ``None`` key mirrors
    that.
    """
    present = df.filter(col(column).isNotNull())
    total = present.count()
    result = {
        row[column]: (row["FREQUENCY"], 100.0 * row["FREQUENCY"] / total)
        for row in present.groupBy(column).agg(count(lit(1)).alias("FREQUENCY")).collect()
    }
    missing = df.count() - total
    if missing:
        result[None] = (missing, None)
    return result


def freq_loan_outcome(final: DataFrame) -> dict:
    return frequency(final, "LOAN_OUTCOME")


def risk_segment_freq(risk: DataFrame) -> dict:
    return frequency(risk, "RISK_SEGMENT")


def crosstab_risk_outcome(risk: DataFrame) -> dict:
    """Counterpart of TABLES RISK_SEGMENT * LOAN_OUTCOME: {(seg, outcome): (n, pct)}."""
    present = risk.filter(col("RISK_SEGMENT").isNotNull() & col("LOAN_OUTCOME").isNotNull())
    total = present.count()
    rows = present.groupBy("RISK_SEGMENT", "LOAN_OUTCOME").agg(count(lit(1)).alias("FREQUENCY"))
    return {
        (row["RISK_SEGMENT"], row["LOAN_OUTCOME"]): (
            row["FREQUENCY"],
            100.0 * row["FREQUENCY"] / total,
        )
        for row in rows.collect()
    }


def means_by_risk_segment(risk: DataFrame) -> dict:
    """Counterpart of PROC MEANS with CLASS RISK_SEGMENT, VAR BAD LOAN LTV DEBTINC."""
    variables = ("BAD", "LOAN", "LTV", "DEBTINC")
    aggregations = []
    for variable in variables:
        aggregations.append(count(col(variable)).alias(f"{variable}_COUNT"))
        aggregations.append(mean(col(variable)).alias(f"{variable}_MEAN"))
        aggregations.append(stddev(col(variable)).alias(f"{variable}_STDDEV"))

    rows = (
        risk.filter(col("RISK_SEGMENT").isNotNull())
        .groupBy("RISK_SEGMENT")
        .agg(count(lit(1)).alias("_FREQ_"), *aggregations)
        .collect()
    )
    return {row["RISK_SEGMENT"]: row.asDict() for row in rows}


def missing_flag_totals(final: DataFrame) -> dict:
    """Counterpart of PROC MEANS SUM= over the *_MISS flags set by 02."""
    columns = [f"{column}_MISS" for column in MISSING_FLAG_COLUMNS]
    row = final.agg(*[spark_sum(col(column)).alias(column) for column in columns]).collect()[0]
    return {column: int(row[column]) for column in columns}


def _sas_percentile(values: list, fraction: float) -> float:
    """PROC MEANS percentile, PCTLDEF=5 (SAS default).

    With ``np = n * fraction``: an integral ``np`` averages order statistics
    ``np`` and ``np + 1``, otherwise the ``ceil(np)``-th order statistic is
    taken. Spark's ``percentile`` interpolates instead, so it cannot be used
    to reproduce SAS quartiles.
    """
    n = len(values)
    position = n * fraction
    index = int(position)
    if position == index:
        return (values[index - 1] + values[index]) / 2.0
    return values[index]


def numeric_summary(final: DataFrame, columns: tuple) -> dict:
    """Counterpart of the N/NMISS/MIN/P25/MEDIAN/P75/MAX export in 99."""
    summary = {}
    total = final.count()
    for column in columns:
        values = sorted(
            row[column]
            for row in final.select(column).filter(col(column).isNotNull()).collect()
        )
        summary[column] = {
            "N": len(values),
            "NMISS": total - len(values),
            "MIN": values[0],
            "P25": _sas_percentile(values, 0.25),
            "MEDIAN": _sas_percentile(values, 0.50),
            "P75": _sas_percentile(values, 0.75),
            "MAX": values[-1],
        }
    return summary


def means_by_outcome(final: DataFrame) -> dict:
    """Counterpart of PROC MEANS with CLASS LOAN_OUTCOME, VAR LOAN DEBTINC.

    PROC MEANS excludes missing values per analysis variable, which is what
    Spark's count/mean/stddev do as well; stddev is the sample (n-1) form in
    both engines.
    """
    rows = (
        final.filter(col("LOAN_OUTCOME").isNotNull())
        .groupBy("LOAN_OUTCOME")
        .agg(
            count(col("LOAN")).alias("LOAN_COUNT"),
            count(col("DEBTINC")).alias("DEBTINC_COUNT"),
            mean(col("LOAN")).alias("LOAN_MEAN"),
            mean(col("DEBTINC")).alias("DEBTINC_MEAN"),
            stddev(col("LOAN")).alias("LOAN_STDDEV"),
            stddev(col("DEBTINC")).alias("DEBTINC_STDDEV"),
        )
        .collect()
    )
    return {row["LOAN_OUTCOME"]: row.asDict() for row in rows}
