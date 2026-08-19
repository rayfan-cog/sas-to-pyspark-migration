from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# sas/01_data_loading.sas, `proc datasets ... label`.
COLUMN_LABELS = {
    "BAD": "Loan Status (1=Default, 0=Paid)",
    "LOAN": "Amount of Loan Request",
    "MORTDUE": "Amount Due on Existing Mortgage",
    "VALUE": "Value of Current Property",
    "REASON": "Loan Purpose (HomeImp or DebtCon)",
    "JOB": "Job Category",
    "YOJ": "Years at Present Job",
    "DEROG": "Number of Derogatory Reports",
    "DELINQ": "Number of Delinquent Credit Lines",
    "CLAGE": "Age of Oldest Credit Line (months)",
    "NINQ": "Number of Recent Credit Inquiries",
    "CLNO": "Number of Credit Lines",
    "DEBTINC": "Debt to Income Ratio",
    "APPDATE": "Loan Application Date",
    "CITY": "City",
    "STATE": "State",
    "DIVISION": "Census Division",
    "REGION": "Census Region",
    "LTV": "Loan to Value Ratio",
    "LOAN_OUTCOME": "Loan Outcome",
    "LTV_RISK_CAT": "LTV Risk Category",
    "DTI_RISK_CAT": "Debt-to-Income Risk Category",
    "DELINQ_RISK_CAT": "Delinquency Risk Category",
    "RISK_SCORE": "Composite Risk Score (0-10)",
    "RISK_SEGMENT": "Risk Segment",
}

# sas/01_data_loading.sas, `proc datasets ... format`; sas/02 adds LTV percent8.2.
# Display-only in SAS: these drive formatting helpers, never storage types.
COLUMN_FORMATS = {
    "LOAN": "dollar12.",
    "MORTDUE": "dollar12.",
    "VALUE": "dollar12.",
    "APPDATE": "date9.",
    "DEBTINC": "8.1",
    "CLAGE": "comma8.1",
    "LTV": "percent8.2",
}


def applyLabels(df: DataFrame) -> DataFrame:
    """Attach SAS variable labels as Spark column comments.

    Spark has no label concept; column metadata `comment` is the closest analog and
    surfaces in `DESCRIBE TABLE` and in Databricks catalogs.
    """
    labelled = df
    for colName in df.columns:
        label = COLUMN_LABELS.get(colName)
        if label is not None:
            labelled = labelled.withColumn(
                colName, col(colName).alias(colName, metadata={"comment": label})
            )
    return labelled
