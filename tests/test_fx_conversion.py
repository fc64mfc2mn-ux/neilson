"""A USD-cents row converts to the same GBP value as the equivalent
USD-dollar row, and a month with no rate raises or quarantines rather than
producing null silently."""

import pytest


@pytest.mark.skip(reason="candidate implements this")
def test_fx_conversion_cents_and_missing_rate(spark):
    raise NotImplementedError
