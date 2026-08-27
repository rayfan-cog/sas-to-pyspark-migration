# SAS to PySpark Migration Mapping Reference

A comprehensive reference table mapping SAS constructs to their PySpark equivalents.

## Where the code lives now

The generic tables below cover SAS constructs in general. For the constructs actually
used by the programs in `sas/`, this table points at the function in `src/homeequity/`
that implements them; the `jobs/` scripts only call these functions and print the
results, so they are never the place to look for logic.

| SAS construct (program) | Implemented by | Job that calls it |
|---|---|---|
| `proc import` + `guessingrows` (sas/01) | `homeequity.schema.HOME_EQUITY_SCHEMA`, `homeequity.io.loadHomeEquity` | `jobs/01_data_loading.py` |
| `proc datasets ... label` / `format` (sas/01) | `homeequity.metadata.COLUMN_LABELS`, `COLUMN_FORMATS`, `applyLabels` | `jobs/01_data_loading.py` |
| `proc contents` / `proc print` (sas/01) | `DataFrame.printSchema()` / `DataFrame.show()` | `jobs/01_data_loading.py` |
| DATA step derived columns (LTV, LOAN_OUTCOME, `propcase`) (sas/02) | `homeequity.transforms.cleaning.addDerivedColumns` | `jobs/02_data_cleaning.py` |
| `array` missing-flag do-loop (sas/02) | `homeequity.transforms.cleaning.addMissingFlags` | `jobs/02_data_cleaning.py` |
| `if X ne .` null filters (sas/02) | `homeequity.transforms.cleaning.filterCriticalNulls` | `jobs/02_data_cleaning.py` |
| Outlier subsetting `if` statements (sas/02) | `homeequity.transforms.cleaning.removeOutliers` | `jobs/02_data_cleaning.py` |
| `work.home_equity_final` (sas/02 output) | `homeequity.transforms.cleaning.buildFinalDataset` | jobs 02-05 |
| `proc freq ... / nocum` (sas/03, sas/04) | `homeequity.reporting.frequencyTable` | `jobs/03`, `jobs/04` |
| `proc means ... class ...` (sas/02, sas/03, sas/04) | `homeequity.reporting.summaryByGroup` | `jobs/02`, `jobs/03`, `jobs/04` |
| `proc tabulate` (sas/03) | `homeequity.reporting.crossTabDefaultRate` | `jobs/03_aggregation_reporting.py` |
| `proc sql outobs=10 ... having` (sas/03) | `homeequity.reporting.topStatesByAverageLoan` | `jobs/03_aggregation_reporting.py` |
| `proc format value ltv_risk / dti_risk / delinq_risk` (sas/04) | `homeequity.transforms.risk.addRiskCategories` | `jobs/04_risk_segmentation.py` |
| Composite `RISK_SCORE` accumulation (sas/04) | `homeequity.transforms.risk.addRiskScore` | `jobs/04_risk_segmentation.py` |
| `RISK_SEGMENT` assignment (sas/04) | `homeequity.transforms.risk.addRiskSegment` | `jobs/04_risk_segmentation.py` |
| `data work.model_data` complete cases (sas/05) | `homeequity.model.logistic.prepareModelData` | `jobs/05_logistic_regression.py` |
| `proc surveyselect method=srs samprate=0.7` (sas/05) | `homeequity.model.logistic.splitTrainValid` | `jobs/05_logistic_regression.py` |
| `proc logistic class ... / param=ref` + `model` (sas/05) | `homeequity.model.logistic.buildPipeline` | `jobs/05_logistic_regression.py` |
| `proc plm restore=` scoring (sas/05) | `PipelineModel.transform` | `jobs/05_logistic_regression.py` |
| `roc;` / `proc freq tables BAD*PREDICTED_BAD` (sas/05) | `homeequity.model.logistic.evaluateModel`, `confusionMatrix` | `jobs/05_logistic_regression.py` |
| SAS session start | `homeequity.session.getSparkSession` | every job |

Where a translation is not exact, the reason and its impact are recorded as a row in
[`docs/deviations.md`](../docs/deviations.md) (for example D-005 for `proc tabulate`,
D-016 for missing class values in `proc freq`, D-011 for `param=ref` reference levels).

## Data Access & I/O

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC IMPORT` (CSV) | `spark.read.csv()` | Prefer `header=True` with an explicit `schema=` over `inferSchema=True`, so types cannot change with the data (D-003) |
| `PROC IMPORT` (Excel) | `spark.read.format("com.crealytics.spark.excel")` | Requires external package |
| `PROC IMPORT` (database) | `spark.read.jdbc()` | Provide JDBC URL and connection properties |
| `PROC EXPORT` (CSV) | `df.write.csv()` | Use `header=True, mode="overwrite"` |
| `PROC EXPORT` (Parquet) | `df.write.parquet()` | Native Spark format, preferred for performance |
| `LIBNAME` (file path) | `spark.read` with path | Configure path or connection string |
| `LIBNAME` (database) | `spark.read.jdbc()` / `spark.read.format()` | Use JDBC or native connectors |

## DATA Step Operations

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `DATA step` (new dataset) | `df.withColumn()`, `df.select()`, `df.filter()` | Chain DataFrame transformations |
| `SET` (read dataset) | Source DataFrame reference | PySpark DataFrames are immutable |
| `IF/THEN/ELSE` | `when().otherwise()` | From `pyspark.sql.functions` |
| `WHERE` clause | `.filter()` / `.where()` | Both are aliases |
| `KEEP` / `DROP` | `.select()` / `.drop()` | Select or drop columns |
| `RENAME` | `.withColumnRenamed()` | Rename one column at a time |
| `LENGTH` statement | `.withColumn(col.cast())` | Cast to appropriate type |
| `FORMAT` / `INFORMAT` | No direct equivalent | Handle in display or write logic |
| `LABEL` | No direct equivalent | Store as column `comment` metadata (`homeequity.metadata.applyLabels`, D-002) |
| `ARRAY` processing | List comprehension + `withColumn` loop | Iterate over column list |
| `RETAIN` | Window functions with `lag()` | Use `Window.orderBy()` |
| `FIRST.` / `LAST.` | Window functions with `row_number()` | Partition and order window |
| `BY` group processing | `.groupBy()` | Group-level operations |
| `MERGE` (join) | `.join()` | Specify join type and condition; **not** equivalent for many-to-many keys — see policy note below |
| `OUTPUT` | `.union()` / write operations | Append rows or write results |
| `propcase()` | `initcap()` | From `pyspark.sql.functions` |
| `substr()` | `substring()` | From `pyspark.sql.functions` |
| `compress()` | `regexp_replace()` | Remove characters with regex |
| `catx()` | `concat_ws()` | Concatenate with separator |
| `input()` | `.cast()` | Type conversion |
| `put()` | `.cast("string")` | Convert to string |
| Missing value (`.`) | `None` / `null` | Use `.isNull()` / `.isNotNull()` |

### Policy note: `MERGE ... BY` is not a SQL join

For one-to-one and one-to-many keys, `MERGE ... BY` behaves like a full outer join.
For **many-to-many** keys it does not: a SQL join produces the Cartesian product of
the matching rows (n × m), while the DATA step MERGE pairs rows positionally within
the BY group and retains the last value from the shorter side (max(n, m) rows).
No program in this codebase currently uses `MERGE`, so no translation exists here —
but as policy, any future SAS program containing `MERGE ... BY` must first be checked
for many-to-many keys (e.g. a `PROC SQL` count of duplicate BY values on both inputs).
If many-to-many pairings exist, do not translate to `.join()` blindly; replicate the
positional pairing with `row_number()` over the BY key on both sides and join on
(key, row_number), then document the choice in `docs/deviations.md`.

## Statistical Procedures

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC MEANS` | `.groupBy().agg()` / `.describe()` | Use `mean`, `stddev`, `min`, `max` from functions |
| `PROC SUMMARY` | `.groupBy().agg()` | Same as PROC MEANS equivalent |
| `PROC FREQ` | `.groupBy().count()` / `.crosstab()` | For frequency tables and cross-tabs; FREQ drops missing class values unless `MISSING` is given, `groupBy` does not (D-016) |
| `PROC TABULATE` | `.groupBy().pivot().agg()` | Pivot for cross-tabulation layout |
| `PROC SQL` | `spark.sql()` | Full SQL support via Spark SQL |
| `PROC SORT` | `.orderBy()` / `.sort()` | Specify ascending/descending |
| `PROC SORT NODUPKEY` | `.dropDuplicates()` | Not equivalent: NODUPKEY keeps the *first* row in sort order; `dropDuplicates()` keeps an arbitrary row per key. Force determinism with `row_number()` over an explicit ordering when the kept row matters |
| `PROC TRANSPOSE` | `.pivot()` / stack patterns | Wide-to-long or long-to-wide |
| `PROC CONTENTS` | `.printSchema()` / `.dtypes` | Schema and metadata inspection |
| `PROC PRINT` | `.show()` | Display rows |
| `PROC UNIVARIATE` | `.describe()` + `.approxQuantile()` | Combine for full distribution stats |
| `PROC CORR` | `Correlation.corr()` | From `pyspark.ml.stat` |
| `PROC FORMAT` | `when().otherwise()` or UDFs | Map value ranges to labels; prefer `when()` chains over UDFs (`homeequity.transforms.risk.addRiskCategories`) |

## Machine Learning

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC LOGISTIC` | `pyspark.ml.classification.LogisticRegression` | Use Pipeline with feature preparation |
| `PROC REG` | `pyspark.ml.regression.LinearRegression` | Similar pipeline approach |
| `PROC GLMSELECT` | `pyspark.ml.regression` + `CrossValidator` | Model selection via cross-validation |
| `CLASS` statement | `StringIndexer` + `OneHotEncoder` | Encode categorical variables |
| `MODEL` statement | `VectorAssembler` + ML algorithm | Assemble features, then fit |
| `SELECTION=STEPWISE` | `elasticNetParam` / `CrossValidator` | Regularization for feature selection |
| `OUTPUT PREDICTED=` | `model.transform()` | Produces `prediction` and `probability` columns |
| `PROC SCORE` | `model.transform()` | Apply fitted model to new data |
| `ODS OUTPUT` | Evaluate with `Evaluator` classes | `BinaryClassificationEvaluator`, etc. |
| `PROC CLUSTER` | `pyspark.ml.clustering.KMeans` | K-Means, Bisecting K-Means, GMM |
| `PROC TREE` | `pyspark.ml.classification.DecisionTreeClassifier` | Tree-based classification |
| `PROC FOREST` | `pyspark.ml.classification.RandomForestClassifier` | Ensemble methods |

## Macro Language

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `%LET` macro variable | Python variable | `myVar = "value"` |
| `%MACRO` / `%MEND` | Python function | `def myFunction():` |
| `&macrovar` resolution | f-string / `.format()` | `f"SELECT * FROM {tableName}"` |
| `%IF / %THEN / %ELSE` | Python `if / elif / else` | Standard control flow |
| `%DO` loop | Python `for` loop | `for i in range(n):` |
| `%INCLUDE` | `import` / `exec(open().read())` | Module imports or script execution |
| `%SYSFUNC()` | Python built-in functions | Direct function calls |
| `CALL SYMPUTX()` | Variable assignment | `result = df.collect()[0][0]` |

## Data Management

| SAS Construct | PySpark Equivalent | Notes |
|---|---|---|
| `PROC APPEND` | `.union()` / `.unionByName()` | Combine DataFrames vertically |
| `PROC COPY` | `df.write` | Write to new location |
| `PROC DELETE` | `spark.catalog.dropTempView()` | Remove temp views |
| `PROC DATASETS` | Catalog operations | Manage tables in catalog |
| `PROC COMPARE` | Custom comparison logic | Compare two DataFrames |
| Permanent dataset | `.write.saveAsTable()` | Save to Hive metastore |
| Temporary dataset | `.createOrReplaceTempView()` | Session-scoped temp table |
