"""Tests for the configuration of the vessel-info pipeline."""

import pytest

from gfw.common.config import PipelineConfigError

from .test_main import make_config


@pytest.mark.parametrize("frequency", [-0.1, 1.5])
def test_config_rejects_a_frequency_out_of_range(frequency: float) -> None:
    """Checks that most_common_min_freq must be between 0 and 1."""
    with pytest.raises(PipelineConfigError, match="most_common_min_freq"):
        make_config(most_common_min_freq=frequency)
