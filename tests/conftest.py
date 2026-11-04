from pathlib import Path

import pytest

from fixtures import write_inputs

GOLDEN = Path(__file__).parent / "golden"


@pytest.fixture(scope="session")
def inputs(tmp_path_factory):
    return write_inputs(tmp_path_factory.mktemp("inputs"))


@pytest.fixture(scope="session")
def golden():
    return GOLDEN
