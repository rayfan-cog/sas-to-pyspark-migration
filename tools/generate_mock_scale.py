"""Generate data/home_equity_scale.csv, a ~20,000-row synthetic fixture.

Fully synthetic (no real data), deterministic (fixed seed), in the exact
18-column schema of data/home_equity.csv. Designed to stress what the small
production extract cannot:

- every JOB and REASON level plus missing values for both;
- all 4 REGIONs and all 9 census DIVISIONs;
- per-column missingness at or above the production rates (DEBTINC especially);
- dense clusters of values sitting exactly on, just under, and just over the
  cleaning filters in sas/02_data_cleaning.sas (LOAN > 0, VALUE > 0,
  0 < LTV < 5) and every >=/< threshold in sas/04_risk_segmentation.sas
  (LTV 0.60/0.80, DEBTINC 30/40/50, DELINQ 1/2/4, DEROG 1/3,
  RISK_SCORE 3/5/7), so segment counts shift if any comparison regresses;
- CITY strings exercising PROPCASE delimiter handling (hyphens, slashes,
  parentheses, periods, multiple blanks).

Usage: python tools/generate_mock_scale.py
"""

import csv
import random
from pathlib import Path

SEED = 20260827
TOTAL_ROWS = 20000

OUT_PATH = Path(__file__).resolve().parent.parent / "data" / "home_equity_scale.csv"

COLUMNS = [
    "BAD", "LOAN", "MORTDUE", "VALUE", "REASON", "JOB", "YOJ", "DEROG",
    "DELINQ", "CLAGE", "NINQ", "CLNO", "DEBTINC", "APPDATE", "CITY",
    "STATE", "DIVISION", "REGION",
]

# (STATE, DIVISION, REGION) covering all 9 divisions and all 4 regions.
GEOGRAPHY = [
    ("Wisconsin", "East North Central", "Midwest"),
    ("Ohio", "East North Central", "Midwest"),
    ("Illinois", "East North Central", "Midwest"),
    ("Minnesota", "West North Central", "Midwest"),
    ("Missouri", "West North Central", "Midwest"),
    ("New York", "Middle Atlantic", "Northeast"),
    ("Pennsylvania", "Middle Atlantic", "Northeast"),
    ("Massachusetts", "New England", "Northeast"),
    ("Connecticut", "New England", "Northeast"),
    ("Maryland", "South Atlantic", "South"),
    ("Florida", "South Atlantic", "South"),
    ("Georgia", "South Atlantic", "South"),
    ("Tennessee", "East South Central", "South"),
    ("Alabama", "East South Central", "South"),
    ("Texas", "West South Central", "South"),
    ("Louisiana", "West South Central", "South"),
    ("Colorado", "Mountain", "West"),
    ("Arizona", "Mountain", "West"),
    ("California", "Pacific", "West"),
    ("Oregon", "Pacific", "West"),
]

# Weighted like production: Other is the modal category, '' is missing.
JOBS = ["Other", "ProfExe", "Office", "Mgr", "Self", "Sales", ""]
JOB_WEIGHTS = [38, 20, 15, 12, 4, 3, 8]

REASONS = ["DebtCon", "HomeImp", ""]
REASON_WEIGHTS = [62, 30, 8]

# PROPCASE delimiter stress strings (hyphen, slash, parens, period, runs of
# blanks) mixed with plain lowercase names.
CITIES = [
    "springfield",
    "winston-salem",
    "new york-city",
    "dallas/fort worth",
    "raleigh (research triangle)",
    "st. petersburg",
    "port  saint  lucie",
    "wilkes-barre/scranton",
    "o'fallon",
    "mc.henry-on/the (lake)  shores",
    "coeur d'alene",
    "boise",
    "",
]
CITY_WEIGHTS = [20, 8, 8, 8, 8, 8, 8, 6, 6, 4, 6, 8, 2]

# Missingness rates >= production (production: MORTDUE 8.7%, VALUE 1.9%,
# YOJ 8.6%, DEROG 11.9%, DELINQ 9.7%, CLAGE 5.2%, NINQ 8.6%, CLNO 3.7%,
# DEBTINC 21.3%).
MISS = {
    "MORTDUE": 0.10,
    "VALUE": 0.03,
    "YOJ": 0.10,
    "DEROG": 0.13,
    "DELINQ": 0.11,
    "CLAGE": 0.07,
    "NINQ": 0.10,
    "CLNO": 0.05,
    "DEBTINC": 0.25,
}

# Cleaning / risk thresholds. Offsets place values exactly on, just under and
# just over each boundary so >= vs > and < vs <= regressions change counts.
LTV_TARGETS = [0.0, 0.599999, 0.6, 0.600001, 0.799999, 0.8, 0.800001, 4.999999, 5.0, 5.000001]
DTI_TARGETS = [29.999999, 30.0, 30.000001, 39.999999, 40.0, 40.000001, 49.999999, 50.0, 50.000001]


def maybe_missing(rng, col, value):
    return "" if rng.random() < MISS[col] else value


def fmt(x):
    if x == "":
        return ""
    if isinstance(x, float):
        return f"{x:.6f}".rstrip("0").rstrip(".")
    return str(x)


def bulk_row(rng):
    """A realistic row with heavy mass near every threshold."""
    state, division, region = rng.choice(GEOGRAPHY)
    bad = 1 if rng.random() < 0.22 else 0
    loan = rng.choice([100, 1000, rng.randrange(1100, 90000, 100)])
    value = rng.choice([20000, rng.randrange(20000, 400000, 500), 855909])

    # MORTDUE driven off an LTV target ~40% of the time, otherwise free.
    if rng.random() < 0.40:
        ltv = rng.choice(LTV_TARGETS)
        mortdue = round(ltv * value, 2)
    else:
        mortdue = rng.randrange(2000, 300000, 250)

    yoj = round(rng.uniform(0, 41), 1)
    derog = rng.choice([0, 0, 0, 0, 1, 1, 2, 3, 4, 10])
    delinq = rng.choice([0, 0, 0, 0, 1, 1, 2, 3, 4, 5, 15])
    clage = round(rng.uniform(0, 1168.23), 6)
    ninq = rng.choice([0, 0, 1, 1, 2, 3, 5, 17])
    clno = rng.choice([0, rng.randrange(1, 71)])
    dti = rng.choice(DTI_TARGETS) if rng.random() < 0.45 else round(rng.uniform(0.5, 203.31), 6)
    appdate = rng.randrange(20090, 23010)  # SAS date values, ~1995-2022

    return {
        "BAD": bad,
        "LOAN": loan,
        "MORTDUE": maybe_missing(rng, "MORTDUE", mortdue),
        "VALUE": maybe_missing(rng, "VALUE", value),
        "REASON": rng.choices(REASONS, REASON_WEIGHTS)[0],
        "JOB": rng.choices(JOBS, JOB_WEIGHTS)[0],
        "YOJ": maybe_missing(rng, "YOJ", yoj),
        "DEROG": maybe_missing(rng, "DEROG", derog),
        "DELINQ": maybe_missing(rng, "DELINQ", delinq),
        "CLAGE": maybe_missing(rng, "CLAGE", clage),
        "NINQ": maybe_missing(rng, "NINQ", ninq),
        "CLNO": maybe_missing(rng, "CLNO", clno),
        "DEBTINC": maybe_missing(rng, "DEBTINC", dti),
        "APPDATE": appdate,
        "CITY": rng.choices(CITIES, CITY_WEIGHTS)[0],
        "STATE": state,
        "DIVISION": division,
        "REGION": region,
    }


def boundary_rows(rng):
    """Rows pinned exactly to the cleaning filters and rejection rules."""
    rows = []
    base_value = 100000

    # 02 filter rules: LOAN > 0, VALUE > 0, 0 < LTV < 5, non-missing LOAN/VALUE/BAD.
    reject_specs = [
        {"LOAN": 0}, {"LOAN": -5000}, {"LOAN": ""},
        {"VALUE": 0}, {"VALUE": ""}, {"BAD": ""},
        {"MORTDUE": 0},                       # LTV = 0 -> dropped
        {"MORTDUE": 500000},                  # LTV = 5 exactly -> dropped
        {"MORTDUE": 500001},                  # LTV > 5 -> dropped
        {"MORTDUE": 499999},                  # LTV just under 5 -> kept
        {"MORTDUE": ""},                      # LTV missing -> dropped (fails LTV > 0)
        {"BAD": 2},                           # blank LOAN_OUTCOME branch
    ]
    for i, spec in enumerate(reject_specs):
        for _ in range(40):
            state, division, region = rng.choice(GEOGRAPHY)
            row = {
                "BAD": rng.choice([0, 1]),
                "LOAN": rng.randrange(1000, 50000, 100),
                "MORTDUE": rng.randrange(10000, 90000, 100),
                "VALUE": base_value,
                "REASON": rng.choices(REASONS, REASON_WEIGHTS)[0],
                "JOB": rng.choices(JOBS, JOB_WEIGHTS)[0],
                "YOJ": rng.choice(["", round(rng.uniform(0, 40), 1)]),
                "DEROG": rng.choice(["", 0, 1, 2, 3, 4]),
                "DELINQ": rng.choice(["", 0, 1, 2, 3, 4, 5]),
                "CLAGE": round(rng.uniform(10, 600), 6),
                "NINQ": rng.choice(["", 0, 1, 2]),
                "CLNO": rng.randrange(0, 60),
                "DEBTINC": rng.choice(["", rng.choice(DTI_TARGETS)]),
                "APPDATE": rng.randrange(20090, 23010),
                "CITY": rng.choices(CITIES, CITY_WEIGHTS)[0],
                "STATE": state,
                "DIVISION": division,
                "REGION": region,
            }
            row.update(spec)
            rows.append(row)

    # Rows engineered so RISK_SCORE lands exactly on segment cuts 3, 5, 7 and
    # half-point neighbours, via known component combinations.
    score_specs = [
        # (mortdue ltv-mult, debtinc, delinq, derog) -> score
        (0.85, 25, 0, 0),   # 3.0 exactly (LTV 3 only)
        (0.70, 25, 0, 0),   # 1.5
        (0.70, 35, 1, 0),   # 3.0 (1.5+1+0.5)
        (0.85, 45, 0, 0),   # 5.0 (3+2)
        (0.85, 35, 1, 0),   # 4.5
        (0.85, 55, 0, 0),   # 6.0
        (0.85, 45, 2, 0),   # 6.5
        (0.85, 55, 1, 0),   # 6.5
        (0.85, 55, 2, 0),   # 7.5
        (0.85, 55, 4, 3),   # 10.0 max
        (0.85, 45, 0, 1),   # 6.0
        (0.85, 45, 1, 1),   # 6.5
        (0.85, 45, 2, 1),   # 7.0 exactly
        (0.59, 29, 0, 0),   # 0.0
        (0.60, 30, 1, 1),   # 4.0 (1.5+1+0.5+1)
        (0.80, 40, 2, 1),   # 7.0 (3+2+1.5+... = 7.5) - boundary-adjacent
    ]
    for mult, dti, delinq, derog in score_specs:
        for _ in range(25):
            state, division, region = rng.choice(GEOGRAPHY)
            rows.append({
                "BAD": rng.choice([0, 1]),
                "LOAN": rng.randrange(1000, 50000, 100),
                "MORTDUE": round(mult * base_value, 2),
                "VALUE": base_value,
                "REASON": rng.choices(REASONS, REASON_WEIGHTS)[0],
                "JOB": rng.choices(JOBS, JOB_WEIGHTS)[0],
                "YOJ": round(rng.uniform(0, 40), 1),
                "DEROG": derog,
                "DELINQ": delinq,
                "CLAGE": round(rng.uniform(10, 600), 6),
                "NINQ": rng.choice([0, 1, 2]),
                "CLNO": rng.randrange(0, 60),
                "DEBTINC": dti,
                "APPDATE": rng.randrange(20090, 23010),
                "CITY": rng.choices(CITIES, CITY_WEIGHTS)[0],
                "STATE": state,
                "DIVISION": division,
                "REGION": region,
            })
    return rows


def main():
    rng = random.Random(SEED)
    rows = boundary_rows(rng)
    while len(rows) < TOTAL_ROWS:
        rows.append(bulk_row(rng))
    rng.shuffle(rows)

    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        for row in rows:
            writer.writerow([fmt(row[c]) for c in COLUMNS])
    print(f"wrote {len(rows)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
