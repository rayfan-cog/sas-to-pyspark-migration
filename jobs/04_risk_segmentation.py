"""Job: risk segmentation. SAS equivalent: sas/04_risk_segmentation.sas."""

from homeequity.io import loadHomeEquity
from homeequity.reporting import frequencyTable, summaryByGroup
from homeequity.session import getSparkSession
from homeequity.transforms.cleaning import buildFinalDataset
from homeequity.transforms.risk import buildRiskDataset


def main() -> None:
    spark = getSparkSession("HomeEquity_RiskSegmentation")
    riskDf = buildRiskDataset(buildFinalDataset(loadHomeEquity(spark))).cache()

    print("Risk Category Frequencies (SAS: proc format + proc freq)")
    for column in ["RISK_SEGMENT", "LTV_RISK_CAT", "DTI_RISK_CAT", "DELINQ_RISK_CAT"]:
        print(f"\n--- {column} ---")
        frequencyTable(riskDf, column).show(truncate=False)

    print("\nSegment Profile (SAS: proc means; class RISK_SEGMENT)")
    summaryByGroup(riskDf, ["RISK_SEGMENT"], ["BAD", "LOAN", "LTV", "DEBTINC"]).show(
        truncate=False
    )
    spark.stop()


if __name__ == "__main__":
    main()
