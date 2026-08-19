from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


def addRiskCategories(df: DataFrame) -> DataFrame:
    """SAS: proc format value ltv_risk / dti_risk / delinq_risk applied in sas/04 step 2."""
    return (
        df.withColumn(
            "LTV_RISK_CAT",
            when(col("LTV").isNull(), lit(None).cast("string"))
            .when(col("LTV") < 0.60, lit("Low"))
            .when(col("LTV") < 0.80, lit("Medium"))
            .otherwise(lit("High")),
        )
        .withColumn(
            "DTI_RISK_CAT",
            when(col("DEBTINC").isNull(), lit(None).cast("string"))
            .when(col("DEBTINC") < 30, lit("Low"))
            .when(col("DEBTINC") < 40, lit("Medium"))
            .when(col("DEBTINC") < 50, lit("High"))
            .otherwise(lit("Very High")),
        )
        .withColumn(
            "DELINQ_RISK_CAT",
            when(col("DELINQ").isNull(), lit(None).cast("string"))
            .when(col("DELINQ") == 0, lit("None"))
            .when(col("DELINQ") == 1, lit("Low"))
            .when(col("DELINQ") <= 3, lit("Medium"))
            .otherwise(lit("High")),
        )
    )


def addRiskScore(df: DataFrame) -> DataFrame:
    """SAS: composite RISK_SCORE accumulation in sas/04 step 2.

    A missing component contributes 0, matching the SAS `if X ne . then do;` guards.
    """
    ltvPoints = (
        when(col("LTV").isNull(), lit(0.0))
        .when(col("LTV") >= 0.80, lit(3.0))
        .when(col("LTV") >= 0.60, lit(1.5))
        .otherwise(lit(0.0))
    )
    dtiPoints = (
        when(col("DEBTINC").isNull(), lit(0.0))
        .when(col("DEBTINC") >= 50, lit(3.0))
        .when(col("DEBTINC") >= 40, lit(2.0))
        .when(col("DEBTINC") >= 30, lit(1.0))
        .otherwise(lit(0.0))
    )
    delinqPoints = (
        when(col("DELINQ").isNull(), lit(0.0))
        .when(col("DELINQ") >= 4, lit(2.0))
        .when(col("DELINQ") >= 2, lit(1.5))
        .when(col("DELINQ") == 1, lit(0.5))
        .otherwise(lit(0.0))
    )
    derogPoints = (
        when(col("DEROG").isNull(), lit(0.0))
        .when(col("DEROG") >= 3, lit(2.0))
        .when(col("DEROG") >= 1, lit(1.0))
        .otherwise(lit(0.0))
    )
    return df.withColumn("RISK_SCORE", ltvPoints + dtiPoints + delinqPoints + derogPoints)


def addRiskSegment(df: DataFrame) -> DataFrame:
    """SAS: length RISK_SEGMENT $14 assignment in sas/04 step 2."""
    return df.withColumn(
        "RISK_SEGMENT",
        when(col("RISK_SCORE") < 3, lit("Low Risk"))
        .when(col("RISK_SCORE") < 5, lit("Medium Risk"))
        .when(col("RISK_SCORE") < 7, lit("High Risk"))
        .otherwise(lit("Very High Risk")),
    )


def buildRiskDataset(df: DataFrame) -> DataFrame:
    """SAS: work.home_equity_risk."""
    return addRiskSegment(addRiskScore(addRiskCategories(df)))
