import pytest

from homeequity.io import loadHomeEquity
from homeequity.metadata import applyLabels
from homeequity.session import getSparkSession


@pytest.fixture(scope="session")
def spark():
    session = getSparkSession("HomeEquityTests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def rawDf(spark):
    return loadHomeEquity(spark).cache()


@pytest.fixture(scope="session")
def labelledDf(rawDf):
    return applyLabels(rawDf)
