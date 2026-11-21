"""A fixture breaching a stated threshold raises, and one within it
passes."""

import pytest


@pytest.mark.skip(reason="candidate implements this")
def test_dq_gate_threshold(spark):
    raise NotImplementedError
