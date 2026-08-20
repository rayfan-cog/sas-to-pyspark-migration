from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_add, lit, to_date, trim
from pyspark.sql.types import StringType

from homeequity.schema import DATA_PATH, HOME_EQUITY_SCHEMA

# SAS stores dates as day counts from 1960-01-01 — 3,653 days before the
# Unix/Spark epoch (1970-01-01). Convert from the SAS epoch, never via Unix.
SAS_EPOCH = "1960-01-01"


def loadHomeEquity(spark: SparkSession, path: str | None = None) -> DataFrame:
    """Load the home equity CSV with the pinned schema.

    SAS equivalent: `proc import datafile=... dbms=csv guessingrows=5960`.
    An explicit schema replaces `inferSchema=True` so column types cannot change
    with the data.

    Two mandatory ingest normalizations:
    - Every StringType column is trimmed (blanket policy, not per-column): SAS
      character comparison ignores trailing blanks, Spark string equality is
      exact, so untrimmed values silently split groups and miss join keys (D-013).
    - APPDATE arrives as a raw SAS date numeric and is converted to DateType
      from the 1960-01-01 SAS epoch (D-014).
    """
    df = spark.read.csv(path or DATA_PATH, header=True, schema=HOME_EQUITY_SCHEMA)
    for field in HOME_EQUITY_SCHEMA.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df.withColumn(
        "APPDATE", date_add(to_date(lit(SAS_EPOCH)), col("APPDATE").cast("int"))
    )
