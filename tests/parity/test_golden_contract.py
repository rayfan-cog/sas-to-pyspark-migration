"""Preflight: validate the golden SAS set before comparing anything against it.

Runs ahead of test_golden_parity.py (alphabetical collection order) so a
truncated, mis-exported or partially committed golden set fails as a golden-set
defect instead of surfacing as a parity mismatch -- or, worse, as a silent skip.
The contract mirrors tests/parity/golden/README.md.
"""

import os

import pytest

from tests.parity import pipeline
from tests.parity.golden_contract import (
    GOLDEN_CONTRACT,
    GOLDEN_FILES,
    golden_path,
    golden_required,
    read_golden_rows,
)


@pytest.mark.golden_contract
@pytest.mark.parametrize("name", sorted(GOLDEN_FILES))
def test_golden_file_matches_contract(name):
    path = golden_path(name, pipeline.GOLDEN_DIR)
    header, key_column, required_keys = GOLDEN_CONTRACT[name]

    if not os.path.exists(path):
        message = (
            f"golden file {GOLDEN_FILES[name]} is missing; "
            "see tests/parity/golden/README.md for how to regenerate it"
        )
        if golden_required():
            pytest.fail(f"REQUIRE_GOLDEN is set and {message}")
        pytest.skip(message)

    assert os.path.getsize(path) > 0, f"{GOLDEN_FILES[name]} is empty"

    rows = read_golden_rows(path)
    assert rows, f"{GOLDEN_FILES[name]} has a header but no data rows"
    assert list(rows[0]) == header, (
        f"{GOLDEN_FILES[name]} header {list(rows[0])} does not match the "
        f"contract {header}"
    )

    keys = [row[key_column] for row in rows]
    missing_keys = [key for key in required_keys if key not in keys]
    assert not missing_keys, (
        f"{GOLDEN_FILES[name]} is missing {key_column} rows {missing_keys}"
    )
    for row in rows:
        for column in header:
            assert row[column] not in (None, ""), (
                f"{GOLDEN_FILES[name]}: empty {column} for {key_column}="
                f"{row[key_column]}"
            )
