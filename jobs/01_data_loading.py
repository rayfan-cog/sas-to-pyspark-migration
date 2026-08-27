"""Job: data loading and metadata inspection. SAS equivalent: sas/01_data_loading.sas."""

from homeequity.io import loadHomeEquity
from homeequity.metadata import COLUMN_FORMATS, COLUMN_LABELS, applyLabels
from homeequity.session import getSparkSession


def main() -> None:
    spark = getSparkSession("HomeEquity_DataLoading")
    labelledDf = applyLabels(loadHomeEquity(spark))

    print("Column Labels (SAS: proc datasets ... label)")
    for colName in labelledDf.columns:
        print(f"  {colName:12s} -> {COLUMN_LABELS[colName]}")

    print("\nColumn Formats (SAS: proc datasets ... format; display only, see D-002)")
    for colName, formatName in COLUMN_FORMATS.items():
        print(f"  {colName:12s} -> {formatName}")

    print("\nDataset Schema (SAS: proc contents)")
    labelledDf.printSchema()
    print(f"Number of rows: {labelledDf.count()}")
    print(f"Number of columns: {len(labelledDf.columns)}")

    print("\nFirst 20 Observations (SAS: proc print obs=20)")
    labelledDf.show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
