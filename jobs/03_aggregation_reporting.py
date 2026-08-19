"""Job: aggregation and reporting. SAS equivalent: sas/03_aggregation_reporting.sas."""

from homeequity.io import loadHomeEquity
from homeequity.reporting import (
    crossTabDefaultRate,
    frequencyTable,
    summaryByGroup,
    topStatesByAverageLoan,
)
from homeequity.session import getSparkSession
from homeequity.transforms.cleaning import buildFinalDataset


def main() -> None:
    spark = getSparkSession("HomeEquity_AggregationReporting")
    finalDf = buildFinalDataset(loadHomeEquity(spark)).cache()

    print("Frequency Tables (SAS: proc freq ... / nocum)")
    for column in ["JOB", "REASON", "LOAN_OUTCOME", "REGION"]:
        print(f"\n--- {column} ---")
        frequencyTable(finalDf, column).show(truncate=False)

    print("\nSummary Statistics by Loan Outcome (SAS: proc means; class LOAN_OUTCOME)")
    summaryByGroup(finalDf, ["LOAN_OUTCOME"], ["LOAN", "MORTDUE", "VALUE", "DEBTINC"]).show(
        truncate=False
    )

    print("Default Rate by JOB x REGION (SAS: proc tabulate; see D-005)")
    crossTabDefaultRate(finalDf, "JOB", "REGION").show(50, truncate=False)

    print("Top 10 States by Average Loan Amount (SAS: proc sql outobs=10)")
    topStatesByAverageLoan(finalDf).show(truncate=False)

    print("Loan Distribution by Reason and Outcome (SAS: proc means; class REASON LOAN_OUTCOME)")
    summaryByGroup(finalDf, ["REASON", "LOAN_OUTCOME"], ["LOAN", "LTV", "DEBTINC"]).show(
        truncate=False
    )
    spark.stop()


if __name__ == "__main__":
    main()
