"""Job: logistic regression. SAS equivalent: sas/05_logistic_regression.sas."""

from homeequity.io import loadHomeEquity
from homeequity.model.logistic import (
    buildPipeline,
    confusionMatrix,
    evaluateModel,
    prepareModelData,
    splitTrainValid,
)
from homeequity.session import getSparkSession
from homeequity.transforms.cleaning import buildFinalDataset
from homeequity.transforms.risk import buildRiskDataset


def main() -> None:
    spark = getSparkSession("HomeEquity_LogisticRegression")
    modelDf = prepareModelData(buildRiskDataset(buildFinalDataset(loadHomeEquity(spark))))
    train, valid = splitTrainValid(modelDf)

    print("Model Data (SAS: data work.model_data — complete cases only)")
    print(f"Complete cases: {modelDf.count()}")
    print(f"Train rows: {train.count()}  Valid rows: {valid.count()}  (SAS: proc surveyselect)")

    scored = buildPipeline().fit(train).transform(valid)

    print("\nModel Evaluation (SAS: proc logistic ... roc; see D-009, D-010, D-018)")
    for name, value in evaluateModel(scored).items():
        print(f"  {name:15s} {value}")

    print("\nConfusion Matrix (SAS: proc freq tables BAD * PREDICTED_BAD)")
    confusionMatrix(scored).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
