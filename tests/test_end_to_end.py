import pytest
import pandas as pd
from collections import Counter
from functools import lru_cache
import numpy as np
import subprocess
import shlex


max_timing_offset_s = 30

example_mmsi = [
    "725002300",
    "512445000",
    "725001619",
    "512000113",
    "412217999",
    "634125000",
    "304043000",
    "770576439",
    "412000000",
    "319088200",
    "248402000",
    "273840800",
    "740349000",
    "503564000",
    "273810710",
    "503432000",
    "512000058",
    "503674000",
    "226327000",
    "368926089"
]

mmsi_str = ', '.join(f'"{x}"' for x in example_mmsi)


# Only look at Spire here since spire is the only one getting pruned by satellite and this
# way we avoid deduping of messages across Spire / Orbcom
pipeline_cmd_str = f"""
python -m pipe_segment \
    --runner=DataflowRunner \
    --requirements_file=./requirements.txt \
    --staging_location=gs://pipe-temp-ttl30/dataflow_staging \
    --disk_size_gb=100 \
    --worker_machine_type=custom-1-16384-ext \
    --project=world-fishing-827 \
    --in_normalized_messages_table=bq://world-fishing-827:pipe_ais_sources_v20190222.normalized_spire_ \
    --in_normalized_sat_offset_messages_table bq://world-fishing-827:pipe_ais_sources_v20190222.normalized_spire_ \
    --runner=DataflowRunner \
    --labels=airflow-version=v1-10-2 \
    --temp_shards_per_day=100 \
    --experiments=shuffle_mode=service \
    --max_num_workers=100 \
    --temp_location=gs://pipe-temp-ttl30/dataflow_temp \
    --setup_file=./setup.py \
    --region=us-central1 \
    --job_name=pytest-segmenter \
    --look_ahead=1 \
    --max_timing_offset_s {max_timing_offset_s} \
    --out_sat_offsets_table bq://world-fishing-827:0_ttl24h.segmenter_test_sat_offsets_ \
    --out_segments_table=bq://world-fishing-827:0_ttl24h.segmenter_test_segments_ \
    --out_segmented_messages_table=bq://world-fishing-827:0_ttl24h.segmenter_test_messages_ \
    --date_range=2018-07-01,2018-07-01 \
    --wait_for_job \
    --ssvid_filter_query 'select ssvid from unnest([{mmsi_str}]) as ssvid'"""  # noqa: E501

pipeline_cmd = shlex.split(pipeline_cmd_str.strip())

pipeline_cmd_nosat_str = f"""
python -m pipe_segment \
    --runner=DataflowRunner \
    --requirements_file=./requirements.txt \
    --staging_location=gs://pipe-temp-ttl30/dataflow_staging \
    --disk_size_gb=100 \
    --worker_machine_type=custom-1-16384-ext \
    --project=world-fishing-827 \
    --in_normalized_messages_table=bq://world-fishing-827:pipe_ais_sources_v20190222.normalized_spire_ \
    --runner=DataflowRunner \
    --labels=airflow-version=v1-10-2 \
    --temp_shards_per_day=100 \
    --experiments=shuffle_mode=service \
    --max_num_workers=100 \
    --temp_location=gs://pipe-temp-ttl30/dataflow_temp \
    --setup_file=./setup.py \
    --region=us-central1 \
    --job_name=pytest-segmenter-nosat \
    --look_ahead=1 \
    --max_timing_offset_s {max_timing_offset_s} \
    --out_segments_table=bq://world-fishing-827:0_ttl24h.segmenter_test_nosat_segments_ \
    --out_segmented_messages_table=bq://world-fishing-827:0_ttl24h.segmenter_test_nosat_messages_ \
    --date_range=2018-07-01,2018-07-01 \
    --wait_for_job \
    --ssvid_filter_query 'select ssvid from unnest([{mmsi_str}]) as ssvid'"""  # noqa E501

pipeline_cmd_nosat = shlex.split(pipeline_cmd_nosat_str.strip())


@pytest.mark.slow
class TestEndToEnd:

    _table_names = None
    _dropped_hours = None

    def run_pipeline(self):
        print('Running segmenter pipeline to generate temp tables')
        subprocess.run(pipeline_cmd, check=True, capture_output=True)
        # subprocess.run(pipeline_cmd_nosat, check=True, capture_output=True)
        self._table_names = dict(
            sat_offsets='0_ttl24h.segmenter_test_sat_offsets_',
            segments='0_ttl24h.segmenter_test_segments_',
            messages='0_ttl24h.segmenter_test_messages_',
            # nosat_messages = '0_ttl24h.segmenter_test_nosat_messages_'
        )

    @property
    @lru_cache(1)
    def sat_offsets_table(self):
        if self._table_names is None:
            self.run_pipeline()
        return self._table_names['sat_offsets']

    @property
    @lru_cache(1)
    def messages_table(self):
        if self._table_names is None:
            self.run_pipeline()
        return self._table_names['messages']

    # @property
    # @lru_cache(1)
    # def nosat_messages_table(self):
    #     if self._table_names is None:
    #         self.run_pipeline()
    #     return self._table_names['nosat_messages']

    @property
    @lru_cache(1)
    def hourly_offsets(self):
        query = f"""
            select receiver, dt, hour
            from `{self.sat_offsets_table}201807*`
            order by receiver, hour
            """
        return pd.read_gbq(query, project_id='world-fishing-827')

    @property
    @lru_cache(1)
    def drop_mask(self):
        drop_mask = np.zeros(len(self.hourly_offsets), dtype=bool)
        for rcvr in self.hourly_offsets.receiver.unique():
            mask = (self.hourly_offsets.receiver == rcvr)
            offsets = self.hourly_offsets[mask].copy()

            sub_mask = (abs(offsets.dt.values) > max_timing_offset_s)
            sub_mask[1:] |= sub_mask[:-1]
            sub_mask[:-1] |= sub_mask[1:]
            drop_mask[mask] = sub_mask
        return drop_mask

    @property
    @lru_cache(1)
    def receivers(self):
        "Return receivers in order from most bad hours to fewest"
        bad_cnts = Counter(self.hourly_offsets[self.drop_mask].receiver.values)
        return [name for (name, cnt) in bad_cnts.most_common()]

    def test_that_bad_hours_dropped(self):
        worst_rcvr = self.receivers[0]
        examples = self.hourly_offsets.hour[self.drop_mask &
                                            (self.hourly_offsets.receiver == worst_rcvr)].unique()
        xmpl_str = ', '.join(f'timestamp("{x}")' for x in examples)
        query = f"""
            select *
            from `{self.messages_table}20180701`
            where receiver = "{worst_rcvr}"
              and timestamp_trunc(timestamp, hour) in ({xmpl_str})
            limit 100
            """
        should_be_empty = pd.read_gbq(query, project_id='world-fishing-827')
        assert len(should_be_empty) == 0

    def test_that_good_hours_kept(self):
        worst_rcvr = self.receivers[0]
        examples = self.hourly_offsets.hour[self.drop_mask &
                                            (self.hourly_offsets.receiver == worst_rcvr)].unique()
        xmpl_str = ', '.join(f'timestamp("{x}")' for x in examples)
        query = f"""
            select *, timestamp_trunc(timestamp, hour) hour
            from `{self.messages_table}20180701`
            where receiver = "{worst_rcvr}"
              and timestamp_trunc(timestamp, hour) not in ({xmpl_str})
              and source = 'spire'
            limit 1000
            """
        none_should_dropped = pd.read_gbq(query, project_id='world-fishing-827')
        query = f"""
            select *, timestamp_trunc(timestamp, hour) hour
            from `pipe_ais_sources_v20190222.normalized_spire_20180701`
            where receiver = "{worst_rcvr}"
              and timestamp_trunc(timestamp, hour) not in ({xmpl_str})
              and ssvid in ({mmsi_str})
              and source = 'spire'
            limit 1000
            """
        baseline = pd.read_gbq(query, project_id='world-fishing-827')
        assert len(none_should_dropped) == len(baseline)
