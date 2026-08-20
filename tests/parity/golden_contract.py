"""The contract the golden SAS outputs must satisfy.

Documented in tests/parity/golden/README.md: each parity assertion depends on a
specific file, header and set of key rows. Keeping the contract here lets the
preflight test (test_golden_contract.py) validate the golden set before the
comparison tests consume it, and lets test_golden_parity.py share the same file
list and the REQUIRE_GOLDEN switch.
"""

import csv
import os

GOLDEN_FILES = {
    "row_counts": "row_counts.csv",
    "freq_loan_outcome": "freq_loan_outcome.csv",
    "means_by_outcome": "means_by_outcome.csv",
    "risk_segment_freq": "risk_segment_freq.csv",
}

# name -> (header columns, key column, key values that must be present)
GOLDEN_CONTRACT = {
    "row_counts": (
        ["STAGE", "N"],
        "STAGE",
        ["home_equity", "home_equity_final", "home_equity_risk"],
    ),
    "freq_loan_outcome": (
        ["LOAN_OUTCOME", "FREQUENCY", "PERCENT"],
        "LOAN_OUTCOME",
        ["Default", "Paid"],
    ),
    "risk_segment_freq": (
        ["RISK_SEGMENT", "FREQUENCY", "PERCENT"],
        "RISK_SEGMENT",
        ["Low Risk", "Medium Risk", "High Risk", "Very High Risk"],
    ),
    "means_by_outcome": (
        [
            "LOAN_OUTCOME",
            "_TYPE_",
            "_FREQ_",
            "LOAN_COUNT",
            "DEBTINC_COUNT",
            "LOAN_MEAN",
            "DEBTINC_MEAN",
            "LOAN_STDDEV",
            "DEBTINC_STDDEV",
        ],
        "LOAN_OUTCOME",
        ["Default", "Paid"],
    ),
}

REQUIRE_GOLDEN_ENV = "REQUIRE_GOLDEN"


def golden_required() -> bool:
    """True when a missing golden set must fail instead of skip.

    CI and eval runs export REQUIRE_GOLDEN=1 so parity can never be silently
    uncovered; local development leaves it unset.
    """
    value = os.environ.get(REQUIRE_GOLDEN_ENV, "")
    return value.strip().lower() not in ("", "0", "false", "no")


def golden_path(name: str, golden_dir: str) -> str:
    return os.path.join(golden_dir, GOLDEN_FILES[name])


def read_golden_rows(path: str) -> list:
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))
