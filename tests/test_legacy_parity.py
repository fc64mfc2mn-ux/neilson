"""The ported query and the naive translation differ by a stated row-count
difference and a stated CPL scaling factor, matching the behavioural
differences you found and listed in your submission."""

import pytest


@pytest.mark.skip(
    reason="Step 14 (legacy port, stretch): the port of sp_MarketingLeadSummary.sql and its "
    "parity test are built there, after the core pipeline. Not implemented yet."
)
def test_legacy_parity_naive_vs_ported(spark):
    raise NotImplementedError
