import os

from pyspark.sql import SparkSession


def getSparkSession(appName: str) -> SparkSession:
    """Build (or fetch) a SparkSession.

    Only forces local mode when no master is supplied by the environment, so the
    same code runs unchanged on Databricks, EMR, and Synapse.
    """
    builder = SparkSession.builder.appName(appName)
    if not os.environ.get("SPARK_MASTER") and not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        builder = builder.master("local[*]")
    builder = builder.config("spark.sql.shuffle.partitions", "4")
    return builder.getOrCreate()
