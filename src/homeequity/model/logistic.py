from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, udf
from pyspark.sql.types import DoubleType

NUMERIC_PREDICTORS = [
    "LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "DEROG", "CLAGE", "NINQ",
]
CATEGORICAL_PREDICTORS = ["JOB", "REASON"]
MODEL_PREDICTORS = NUMERIC_PREDICTORS + CATEGORICAL_PREDICTORS
LABEL_COLUMN = "BAD"
COMPLETE_CASE_COLUMNS = ["LOAN", "MORTDUE", "VALUE", "DEBTINC", "DELINQ", "CLAGE"]


def prepareModelData(df: DataFrame) -> DataFrame:
    """SAS: data work.model_data — complete cases only (sas/05 step 1).

    DEROG, NINQ, JOB, and REASON are also required by the model matrix, so they are
    filtered too; SAS drops those rows implicitly at fit time.
    """
    condition = None
    for name in COMPLETE_CASE_COLUMNS + ["DEROG", "NINQ", "JOB", "REASON", LABEL_COLUMN]:
        clause = col(name).isNotNull()
        condition = clause if condition is None else condition & clause
    return df.filter(condition)


def splitTrainValid(
    df: DataFrame, trainFraction: float = 0.7, seed: int = 42
) -> tuple[DataFrame, DataFrame]:
    """SAS: proc surveyselect method=srs samprate=0.7 seed=42 (sas/05 step 2).

    `randomSplit` is a Bernoulli split, not SAS simple random sampling, so the
    selected rows differ even with the same seed. Deterministic within Spark.
    """
    train, valid = df.randomSplit([trainFraction, 1.0 - trainFraction], seed=seed)
    return train, valid


def buildPipeline() -> Pipeline:
    """SAS: proc logistic class JOB(ref='Other') REASON(ref='HomeImp') / param=ref."""
    indexers = [
        StringIndexer(inputCol=name, outputCol=f"{name}_IDX", handleInvalid="keep")
        for name in CATEGORICAL_PREDICTORS
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{name}_IDX" for name in CATEGORICAL_PREDICTORS],
        outputCols=[f"{name}_VEC" for name in CATEGORICAL_PREDICTORS],
        dropLast=True,
    )
    assembler = VectorAssembler(
        inputCols=NUMERIC_PREDICTORS + [f"{name}_VEC" for name in CATEGORICAL_PREDICTORS],
        outputCol="features",
    )
    classifier = LogisticRegression(
        featuresCol="features", labelCol=LABEL_COLUMN, maxIter=100, regParam=0.0
    )
    return Pipeline(stages=[*indexers, encoder, assembler, classifier])


def confusionMatrix(scoredDf: DataFrame, threshold: float = 0.5) -> DataFrame:
    """SAS: proc freq tables BAD * PREDICTED_BAD (sas/05 step 5)."""
    probabilityOfDefault = udf(lambda vector: float(vector[1]), DoubleType())
    return (
        scoredDf.withColumn("PRED_PROB", probabilityOfDefault(col("probability")))
        .withColumn("PREDICTED_BAD", (col("PRED_PROB") >= lit(threshold)).cast("int"))
        .groupBy(LABEL_COLUMN, "PREDICTED_BAD")
        .agg(count(lit(1)).alias("N"))
        .orderBy(LABEL_COLUMN, "PREDICTED_BAD")
    )


def evaluateModel(scoredDf: DataFrame, threshold: float = 0.5) -> dict[str, float]:
    """SAS: ROC/AUC from `roc;` plus the confusion matrix counts (sas/05 steps 5-6)."""
    auc = BinaryClassificationEvaluator(
        labelCol=LABEL_COLUMN, rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(scoredDf)
    counts = {
        (int(row[LABEL_COLUMN]), int(row["PREDICTED_BAD"])): int(row["N"])
        for row in confusionMatrix(scoredDf, threshold).collect()
    }
    truePositives = counts.get((1, 1), 0)
    trueNegatives = counts.get((0, 0), 0)
    falsePositives = counts.get((0, 1), 0)
    falseNegatives = counts.get((1, 0), 0)
    total = truePositives + trueNegatives + falsePositives + falseNegatives
    return {
        "auc": auc,
        "accuracy": (truePositives + trueNegatives) / total if total else 0.0,
        "truePositives": truePositives,
        "trueNegatives": trueNegatives,
        "falsePositives": falsePositives,
        "falseNegatives": falseNegatives,
    }
