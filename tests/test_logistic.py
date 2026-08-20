import pytest

from homeequity.model.logistic import (
    CATEGORICAL_PREDICTORS,
    MODEL_PREDICTORS,
    buildPipeline,
    confusionMatrix,
    evaluateModel,
    prepareModelData,
    splitTrainValid,
)

try:  # the cleaning/risk modules land on sibling wave-1 branches
    from homeequity.transforms.cleaning import buildFinalDataset
    from homeequity.transforms.risk import buildRiskDataset
except ModuleNotFoundError:  # pragma: no cover - upstream modules not merged yet
    buildFinalDataset = None
    buildRiskDataset = None


def buildUpstream(rawDf):
    """SAS: work.home_equity_risk — sas/02 then sas/04.

    Falls back to the raw loader output while the cleaning and risk modules are on
    unmerged sibling branches; the model matrix only uses source columns, so the
    fallback exercises the same predictors.
    """
    if buildFinalDataset is None or buildRiskDataset is None:
        return rawDf
    return buildRiskDataset(buildFinalDataset(rawDf))


@pytest.fixture(scope="module")
def modelDf(rawDf):
    return prepareModelData(buildUpstream(rawDf)).cache()


def test_model_data_has_no_missing_predictors(modelDf):
    condition = " OR ".join(f"{name} IS NULL" for name in MODEL_PREDICTORS)
    assert modelDf.filter(condition).count() == 0


def test_split_is_deterministic(modelDf):
    trainA, validA = splitTrainValid(modelDf, seed=42)
    trainB, validB = splitTrainValid(modelDf, seed=42)
    assert trainA.count() == trainB.count()
    assert validA.count() == validB.count()
    assert trainA.count() + validA.count() == modelDf.count()


def test_split_is_roughly_seventy_thirty(modelDf):
    train, valid = splitTrainValid(modelDf, seed=42)
    ratio = train.count() / modelDf.count()
    assert 0.65 <= ratio <= 0.75


def test_pipeline_scores_validation_set(modelDf):
    train, valid = splitTrainValid(modelDf, seed=42)
    model = buildPipeline().fit(train)
    scored = model.transform(valid)
    assert "probability" in scored.columns
    assert "prediction" in scored.columns
    assert scored.count() == valid.count()


def test_auc_beats_random(modelDf):
    train, valid = splitTrainValid(modelDf, seed=42)
    scored = buildPipeline().fit(train).transform(valid)
    metrics = evaluateModel(scored)
    assert metrics["auc"] > 0.6, f"AUC={metrics['auc']}"
    assert 0.0 <= metrics["accuracy"] <= 1.0


def test_metrics_are_reproducible_with_the_same_seed(modelDf):
    def run():
        train, valid = splitTrainValid(modelDf, seed=42)
        return evaluateModel(buildPipeline().fit(train).transform(valid))

    first, second = run(), run()
    assert first["auc"] == pytest.approx(second["auc"], rel=1e-9)
    assert {key: first[key] for key in first if key != "auc"} == {
        key: second[key] for key in second if key != "auc"
    }


def test_confusion_matrix_totals_match(modelDf):
    train, valid = splitTrainValid(modelDf, seed=42)
    scored = buildPipeline().fit(train).transform(valid)
    matrix = confusionMatrix(scored)
    assert sum(row["N"] for row in matrix.collect()) == valid.count()


def test_categorical_predictors_are_encoded():
    assert CATEGORICAL_PREDICTORS == ["JOB", "REASON"]
    stages = buildPipeline().getStages()
    stageNames = [type(stage).__name__ for stage in stages]
    assert stageNames.count("StringIndexer") == 2
    assert "OneHotEncoder" in stageNames
    assert stageNames[-1] == "LogisticRegression"
