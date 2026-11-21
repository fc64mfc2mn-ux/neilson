"""A minutes-denominated row and a seconds-denominated row for the same
logical duration produce equal `duration_seconds`, and an AU timestamp
shifts by the correct offset."""

import pytest


@pytest.mark.skip(reason="candidate implements this")
def test_normalise_calls_units_and_timezone(spark):
    raise NotImplementedError
