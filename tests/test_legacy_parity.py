"""The ported query and the naive translation differ by a stated row-count
difference and a stated CPL scaling factor, matching the behavioural
differences you found and listed in your submission."""

import pytest


@pytest.mark.skip(reason="candidate implements this")
def test_legacy_parity_naive_vs_ported(spark):
    raise NotImplementedError
