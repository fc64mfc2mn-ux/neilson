"""The runner stops on any stage that is not built or fails (rule 11: no skipping)."""

from __future__ import annotations

import logging

import pytest

from src.run_pipeline import StageFailedError, run_stages


def _stage(name, calls, error=None):
    def stage(spark, config):
        calls.append(name)
        if error:
            raise error

    return stage


def test_all_stages_run_in_order():
    calls: list[str] = []
    stages = [(n, _stage(n, calls)) for n in ("a", "b", "c")]
    run_stages(None, {}, stages, logging.getLogger("test"))
    assert calls == ["a", "b", "c"]


@pytest.mark.parametrize("error", [NotImplementedError("not built"), ValueError("bad data")])
def test_an_unbuilt_or_failing_stage_stops_the_run(error):
    calls: list[str] = []
    stages = [
        ("a", _stage("a", calls)),
        ("b", _stage("b", calls, error)),
        ("c", _stage("c", calls)),
    ]
    with pytest.raises(StageFailedError, match="Stage b"):
        run_stages(None, {}, stages, logging.getLogger("test"))
    assert calls == ["a", "b"]  # c never runs: nothing is skipped past
