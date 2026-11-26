"""Tests for the configuration of the segment-vessel-daily pipeline."""

import pytest

from gfw.common.config import PipelineConfigError

from .test_main import make_config


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("single_ident_min_freq", 1.5),
        ("most_common_min_freq", -0.1),
        ("window_days", -1),
        ("spoofing_threshold", -1),
    ],
)
def test_config_rejects_invalid_parameters(name: str, value: float) -> None:
    """Checks that the frequencies must be between 0 and 1, and the amounts not negative."""
    with pytest.raises(PipelineConfigError, match=name):
        make_config(**{name: value})
