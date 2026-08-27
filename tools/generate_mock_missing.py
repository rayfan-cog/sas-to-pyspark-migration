"""Generate data/home_equity_missing.csv, the missingness/imputation fixture.

The production extract and the edge fixture (data/home_equity_edge.csv) never
put a row on every combination of missing numeric predictors, so an aggregate
golden cannot pin down per-column missing-flag and imputation semantics. This
fixture enumerates all 512 missing/non-missing combinations of the 9 numeric
predictors {MORTDUE, VALUE, YOJ, DEROG, DELINQ, CLAGE, NINQ, CLNO, DEBTINC}
(one row per combination), adds JOB/REASON mode-imputation coverage, and a
small block that fixes each column's median and mode deterministically.

Every row is keyed by a unique CITY value naming what it tests (PR #15 style),
so a row-level parity failure names the failing rule. The output is fully
synthetic; no real data.

Deterministic: no randomness, stable ordering. Run from the repo root:

    python tools/generate_mock_missing.py
"""

import csv
import itertools
from pathlib import Path

HEADER = [
    "BAD", "LOAN", "MORTDUE", "VALUE", "REASON", "JOB", "YOJ", "DEROG",
    "DELINQ", "CLAGE", "NINQ", "CLNO", "DEBTINC", "APPDATE", "CITY",
    "STATE", "DIVISION", "REGION",
]

# The 9 numeric predictors whose missingness combinations are enumerated,
# in a fixed order so combination i always maps to the same CITY key.
PREDICTORS = ["MORTDUE", "VALUE", "YOJ", "DEROG", "DELINQ", "CLAGE", "NINQ",
              "CLNO", "DEBTINC"]

# Fixed non-missing values. MORTDUE/VALUE give LTV = 0.6 (Medium boundary);
# DEBTINC = 35 (Medium); DELINQ = 1 (Low); DEROG = 1. Each value is distinct
# from every value in the median/mode block so an imputed value can never be
# mistaken for an observed one.
BASE = {
    "MORTDUE": "60000",
    "VALUE": "100000",
    "YOJ": "5",
    "DEROG": "1",
    "DELINQ": "1",
    "CLAGE": "100",
    "NINQ": "1",
    "CLNO": "10",
    "DEBTINC": "35",
}

GEO = {
    "STATE": "New York",
    "DIVISION": "Middle Atlantic",
    "REGION": "Northeast",
}


def row(city, *, bad="0", loan="10000", reason="HomeImp", job="Other",
        appdate="22040", overrides=None):
    values = dict(BASE)
    if overrides:
        values.update(overrides)
    return {
        "BAD": bad, "LOAN": loan, "REASON": reason, "JOB": job,
        "APPDATE": appdate, "CITY": city, **GEO, **values,
    }


def combination_rows():
    """One row per subset of PREDICTORS set to missing (512 rows)."""
    rows = []
    for combo in itertools.chain.from_iterable(
            itertools.combinations(PREDICTORS, k)
            for k in range(len(PREDICTORS) + 1)):
        missing = set(combo)
        city = "miss " + "+".join(combo) if combo else "miss none"
        overrides = {var: "" for var in missing}
        rows.append(row(city.lower(), overrides=overrides))
    return rows


def categorical_rows():
    """JOB missing, REASON missing, and both missing (mode imputation)."""
    return [
        row("cat job missing", job=""),
        row("cat reason missing", reason=""),
        row("cat job+reason missing", job="", reason=""),
    ]


def median_mode_rows():
    """Five complete rows that fix each predictor's median and mode.

    Per column the five values are v, v, m, h1, h2 with v < m < h1 < h2:
    the median is m (unique middle order statistic, no interpolation under
    any PCTLDEF) and the mode is v (the only repeated value). None of these
    values equals the BASE value used in the combination block.
    """
    cols = {
        #            v        v        m        h1       h2
        "MORTDUE": ["40000", "40000", "50000", "70000", "90000"],
        "VALUE":   ["150000", "150000", "200000", "250000", "300000"],
        "YOJ":     ["2", "2", "7", "12", "20"],
        "DEROG":   ["0", "0", "2", "3", "4"],
        "DELINQ":  ["0", "0", "2", "3", "4"],
        "CLAGE":   ["50", "50", "150", "200", "300"],
        "NINQ":    ["0", "0", "2", "3", "4"],
        "CLNO":    ["5", "5", "15", "20", "30"],
        "DEBTINC": ["10", "10", "25", "42", "55"],
    }
    jobs = ["Office", "Office", "Mgr", "Sales", "Self"]
    reasons = ["DebtCon", "DebtCon", "HomeImp", "DebtCon", "HomeImp"]
    rows = []
    for i in range(5):
        overrides = {var: vals[i] for var, vals in cols.items()}
        rows.append(row(f"anchor median mode {i + 1}",
                        job=jobs[i], reason=reasons[i], overrides=overrides))
    return rows


def main():
    out = Path(__file__).resolve().parent.parent / "data" / "home_equity_missing.csv"
    rows = combination_rows() + categorical_rows() + median_mode_rows()
    cities = [r["CITY"] for r in rows]
    assert len(cities) == len(set(cities)), "CITY keys must be unique"
    assert len(rows) == 512 + 3 + 5
    with out.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {out}")


if __name__ == "__main__":
    main()
