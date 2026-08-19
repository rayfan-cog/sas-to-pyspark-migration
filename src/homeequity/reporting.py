from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, expr, lit, mean, stddev
from pyspark.sql.functions import max as sparkMax
from pyspark.sql.functions import min as sparkMin


def frequencyTable(df: DataFrame, column: str, includeMissing: bool = False) -> DataFrame:
    """SAS: proc freq tables <column> / nocum (sas/03 step 1).

    PROC FREQ drops observations with a missing class value and uses the
    non-missing total as the percent denominator unless `missing` is specified;
    `includeMissing=True` is that option. Spark `groupBy` would otherwise emit
    null as its own group and inflate the denominator (D-016).
    """
    counted = df if includeMissing else df.filter(col(column).isNotNull())
    total = counted.count()
    return (
        counted.groupBy(col(column).alias("VALUE"))
        .agg(count(lit(1)).alias("FREQUENCY"))
        .withColumn("PERCENT", col("FREQUENCY") / lit(total) * lit(100.0))
        .orderBy(col("VALUE").asc_nulls_last())
    )


def summaryByGroup(df: DataFrame, groupCols: list[str], valueCols: list[str]) -> DataFrame:
    """SAS: proc means n mean median std min max; class ...; var ... (sas/03 steps 2, 5).

    The class columns used in this codebase (LOAN_OUTCOME, RISK_SEGMENT) are
    never null after cleaning; if a nullable class column is ever passed, filter
    its nulls first — PROC MEANS with CLASS also drops missing class values (D-016).
    """
    aggregations = []
    for valueCol in valueCols:
        aggregations.extend([
            count(col(valueCol)).alias(f"{valueCol}_COUNT"),
            mean(col(valueCol)).alias(f"{valueCol}_MEAN"),
            expr(f"percentile_approx({valueCol}, 0.5)").alias(f"{valueCol}_MEDIAN"),
            stddev(col(valueCol)).alias(f"{valueCol}_STDDEV"),
            sparkMin(col(valueCol)).alias(f"{valueCol}_MIN"),
            sparkMax(col(valueCol)).alias(f"{valueCol}_MAX"),
        ])
    return df.groupBy(*groupCols).agg(*aggregations)


def crossTabDefaultRate(
    df: DataFrame, rowCol: str, colCol: str, includeMissing: bool = False
) -> DataFrame:
    """SAS: proc tabulate class JOB REGION; var BAD; (sas/03 step 3).

    PROC TABULATE has no PySpark equivalent; a long-format grouped frame carries the
    same numbers (N and mean of BAD per cell) without the printed layout.
    Like PROC TABULATE, rows with a missing class value are dropped unless
    `includeMissing=True` (the SAS `missing` option; see D-016).
    """
    counted = (
        df
        if includeMissing
        else df.filter(col(rowCol).isNotNull() & col(colCol).isNotNull())
    )
    return (
        counted.groupBy(rowCol, colCol)
        .agg(count(lit(1)).alias("N"), avg(col("BAD")).alias("DEFAULT_RATE"))
        .orderBy(rowCol, colCol)
    )


def topStatesByAverageLoan(df: DataFrame, minLoans: int = 10, limit: int = 10) -> DataFrame:
    """SAS: proc sql outobs=10 ... having count(*) >= 10 order by avg_loan desc (sas/03 step 4).

    STATE is a deterministic secondary sort key: Spark `orderBy` does not
    guarantee stable ordering of ties, so `.limit()` after a single-key sort
    could return different rows across runs (D-017).
    """
    return (
        df.groupBy("STATE")
        .agg(
            count(lit(1)).alias("NUM_LOANS"),
            avg(col("LOAN")).alias("AVG_LOAN"),
            avg(col("VALUE")).alias("AVG_PROPERTY_VALUE"),
            avg(col("BAD")).alias("DEFAULT_RATE"),
        )
        .filter(col("NUM_LOANS") >= lit(minLoans))
        .orderBy(col("AVG_LOAN").desc(), col("STATE").asc())
        .limit(limit)
    )
