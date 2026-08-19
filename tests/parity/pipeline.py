"""Reference PySpark pipeline used by the parity harness.

Mirrors sas/01_data_loading.sas -> sas/04_risk_segmentation.sas so the four
artifacts exported by sas/99_export_golden.sas can be reproduced in Spark and
compared against the golden SAS outputs.

The scripts under pyspark/ are standalone demonstrations that print to stdout;
this module reimplements the same logic as importable functions returning
DataFrames.
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, initcap, lit, mean, stddev, when
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "home_equity.csv")
GOLDEN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden")

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


def clean(df: DataFrame) -> DataFrame:
    """sas/02_data_cleaning.sas: derived columns, missing flags, filters."""
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
        .withColumn("CITY", initcap(col("CITY")))
    )

    for column in MISSING_FLAG_COLUMNS:
        cleaned = cleaned.withColumn(
            f"{column}_MISS", when(col(column).isNull(), lit(1)).otherwise(lit(0))
        )

    filtered = cleaned.filter(
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


def _frequency(df: DataFrame, column: str) -> dict:
    """Counterpart of PROC FREQ output: {level: (frequency, percent)}."""
    total = df.filter(col(column).isNotNull()).count()
    rows = df.filter(col(column).isNotNull()).groupBy(column).agg(count(lit(1)).alias("FREQUENCY"))
    return {
        row[column]: (row["FREQUENCY"], 100.0 * row["FREQUENCY"] / total)
        for row in rows.collect()
    }


def freq_loan_outcome(final: DataFrame) -> dict:
    return _frequency(final, "LOAN_OUTCOME")


def risk_segment_freq(risk: DataFrame) -> dict:
    return _frequency(risk, "RISK_SEGMENT")


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
