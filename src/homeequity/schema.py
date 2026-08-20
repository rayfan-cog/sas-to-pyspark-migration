import os

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(PACKAGE_ROOT, "data", "home_equity.csv")

# Mirrors `proc import ... guessingrows=5960` in sas/01_data_loading.sas.
# SAS numerics are all 8-byte floats, so every numeric column is DoubleType.
HOME_EQUITY_SCHEMA = StructType([
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
    # Raw SAS date value (days since 1960-01-01); loadHomeEquity converts it to
    # DateType. See D-014.
    StructField("APPDATE", DoubleType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("DIVISION", StringType(), True),
    StructField("REGION", StringType(), True),
])
