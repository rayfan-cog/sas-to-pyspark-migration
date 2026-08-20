from pyspark.sql import DataFrame
from pyspark.sql.functions import col, initcap, lit, when

NUMERIC_FLAG_SOURCES = ["LOAN", "MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ"]
MISSING_FLAG_COLUMNS = [f"{name}_MISS" for name in NUMERIC_FLAG_SOURCES]


def addDerivedColumns(df: DataFrame) -> DataFrame:
    """SAS: data work.home_equity_clean (sas/02 step 1)."""
    return (
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


def addMissingFlags(df: DataFrame) -> DataFrame:
    """SAS: array num_vars / num_flags do-loop (sas/02 step 2)."""
    flagged = df
    for name in NUMERIC_FLAG_SOURCES:
        flagged = flagged.withColumn(
            f"{name}_MISS", when(col(name).isNull(), lit(1)).otherwise(lit(0))
        )
    return flagged


def filterCriticalNulls(df: DataFrame) -> DataFrame:
    """SAS: if LOAN ne . and VALUE ne . and BAD ne . (sas/02 step 3)."""
    return df.filter(col("LOAN").isNotNull() & col("VALUE").isNotNull() & col("BAD").isNotNull())


def removeOutliers(df: DataFrame) -> DataFrame:
    """SAS: if LTV > 0 and LTV < 5; if LOAN > 0; if VALUE > 0 (sas/02 step 5)."""
    return df.filter(
        (col("LTV") > 0) & (col("LTV") < 5) & (col("LOAN") > 0) & (col("VALUE") > 0)
    )


def buildFinalDataset(df: DataFrame) -> DataFrame:
    """SAS: work.home_equity_final — the input of sas/03, sas/04, sas/05."""
    return removeOutliers(filterCriticalNulls(addMissingFlags(addDerivedColumns(df))))
