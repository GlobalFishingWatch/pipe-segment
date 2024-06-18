import pytest
from datetime import date, timedelta

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipe_segment.transform.tag_with_fragid_and_timebin import TagWithFragIdAndTimeBin


def test_tag_with_fragid_and_timebin():
    bins_per_day = 4

    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 3)
    middle_date = date(2024, 1, 2)

    frag_id = 1234
    ssvid = "44332211"

    op = TagWithFragIdAndTimeBin(
        start_date=start_date, end_date=end_date, bins_per_day=bins_per_day)

    _input = {"ssvid": ssvid, "frag_id": frag_id, "date": middle_date}

    res = list(op.tag_frags(_input))
    for bin_idx in range(bins_per_day):
        assert res[bin_idx] == ((ssvid, frag_id, str(middle_date), bin_idx), _input)

    with pytest.raises(StopIteration):
        _input_out_of_range = {
            "ssvid": ssvid, "frag_id": frag_id, "date": start_date - timedelta(days=1)}

        res = next(op.tag_frags(_input_out_of_range))

    # Beam
    inputs = [_input]

    with TestPipeline() as p:
        pcoll = p | beam.Create(inputs)
        output = pcoll | op

        assert_that(output, equal_to(res))
