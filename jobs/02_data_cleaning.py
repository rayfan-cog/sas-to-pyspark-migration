"""Job: data cleaning. SAS equivalent: sas/02_data_cleaning.sas."""

from homeequity.io import loadHomeEquity
from homeequity.reporting import summaryByGroup
from homeequity.session import getSparkSession
from homeequity.transforms.cleaning import buildFinalDataset


def main() -> None:
    spark = getSparkSession("HomeEquity_DataCleaning")
    finalDf = buildFinalDataset(loadHomeEquity(spark))

    print("Clean Dataset Summary (SAS: proc means on work.home_equity_final)")
    summaryByGroup(finalDf, ["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "LTV", "DEBTINC"]).show(
        truncate=False
    )
    finalDf.select(
        "BAD", "LOAN", "MORTDUE", "VALUE", "LTV", "LOAN_OUTCOME", "DEBTINC", "JOB", "REASON"
    ).show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
