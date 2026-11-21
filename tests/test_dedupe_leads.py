"""A fixture of 8 leads in 3 identity clusters collapses to 3 surviving
leads, and `lead_dedup_map` has 8 rows."""

import pytest


@pytest.mark.skip(reason="candidate implements this")
def test_dedupe_leads_collapses_clusters(spark):
    raise NotImplementedError
