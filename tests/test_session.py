from homeequity.session import getSparkSession


def test_session_has_app_name(spark):
    assert spark.conf.get("spark.app.name") == "HomeEquityTests"


def test_session_is_reused():
    first = getSparkSession("HomeEquityTests")
    second = getSparkSession("HomeEquityTests")
    assert first is second
