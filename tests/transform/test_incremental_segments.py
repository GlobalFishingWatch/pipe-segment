import json
import pickle
import random

from collections import defaultdict
from datetime import datetime, timedelta, timezone

import apache_beam as beam
import pytest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from google.cloud import bigquery

from pipe_segment.transform.create_segment_map import CreateSegmentMap
from pipe_segment.transform.create_segments import CreateSegments
from pipe_segment.transform.full_history_segments import SEGMAP, SEGMENTS, FullHistorySegments
from pipe_segment.transform.incremental_segments import IncrementalSegments, SegmentUpdater
from pipe_segment.transform.read_prior_segments import ReadPriorSegments


SSVID = "123456789"
FIRST_DAY = datetime(2024, 4, 1, tzinfo=timezone.utc)
DAYS = [FIRST_DAY + timedelta(days=n) for n in range(12)]
IDENTITIES = [
    {"shipname": "ALPHA", "callsign": "A1", "imo": None, "transponder_type": "A",
     "length": 100.5, "width": 20.0},
    {"shipname": "ALPHA", "callsign": "A1", "imo": "1234567", "transponder_type": "A",
     "length": 100.5, "width": 20.0},
    {"shipname": "BETA", "callsign": None, "imo": None, "transponder_type": "B",
     "length": None, "width": None},
]
DESTINATIONS = [{"destination": "PORT A"}, {"destination": "PORT B"}]


def make_fragments(rng, vessels=2):
    """Builds the daily fragments of vessels that move slowly and sometimes disappear."""
    fragments = []
    origins = [(rng.uniform(-5, 5), rng.uniform(-5, 5)) for _ in range(vessels)]
    velocities = [(rng.uniform(-0.3, 0.3), rng.uniform(-0.3, 0.3)) for _ in range(vessels)]

    def position(vessel, hours_since_start):
        lat, lon = origins[vessel]
        d_lat, d_lon = velocities[vessel]
        return lat + d_lat * hours_since_start / 24, lon + d_lon * hours_since_start / 24

    for day_number, day in enumerate(DAYS):
        for vessel in range(vessels):
            if rng.random() < 0.2:
                continue
            first_hour = rng.uniform(0, 8)
            last_hour = rng.uniform(first_hour + 1, 24)
            first_lat, first_lon = position(vessel, day_number * 24 + first_hour)
            last_lat, last_lon = position(vessel, day_number * 24 + last_hour)
            stopped = rng.random() < 0.15
            first_time = day + timedelta(hours=first_hour)
            last_time = day + timedelta(hours=last_hour)
            fragments.append({
                "frag_id": f"{SSVID}-{first_time.isoformat()}-{vessel}",
                "ssvid": SSVID,
                "timestamp": day.timestamp(),
                "first_msg_timestamp": first_time.timestamp(),
                "first_msg_lat": first_lat,
                "first_msg_lon": first_lon,
                "first_msg_course": rng.uniform(0, 359),
                "first_msg_speed": rng.uniform(0, 5),
                "last_msg_timestamp": last_time.timestamp(),
                "last_msg_lat": last_lat,
                "last_msg_lon": last_lon,
                "last_msg_course": None if stopped else rng.uniform(0, 359),
                "last_msg_speed": 0.0 if stopped else rng.uniform(0, 5),
                "msg_count": rng.randint(1, 500),
                "identities": [
                    {**rng.choice(IDENTITIES), "count": rng.randint(1, 20)},
                    {**rng.choice(IDENTITIES), "count": rng.randint(1, 20)},
                ],
                "destinations": [{**rng.choice(DESTINATIONS), "count": rng.randint(1, 20)}],
            })
    return fragments


def full_history(fragments):
    """Segment map and segments computed from all the fragments, as the full history path does."""
    segmap = list(CreateSegmentMap().merge_fragments((SSVID, fragments)))
    seg_id_by_frag_id = {row["frag_id"]: row["seg_id"] for row in segmap}
    fragments_by_seg_id = defaultdict(list)
    for fragment in fragments:
        fragment = fragment.copy()
        fragment["seg_id"] = seg_id_by_frag_id[fragment["frag_id"]]
        fragments_by_seg_id[fragment["seg_id"]].append(fragment)

    segments = []
    for item in fragments_by_seg_id.items():
        segments.extend(CreateSegments().update_msgs(item))
    return segmap, segments


def as_stored(records):
    """Identity records as they come back from the segments table: all fields are strings."""
    stored = []
    for record in records:
        record = dict(record)
        count = record.pop("count")
        for name in ("length", "width"):
            if record.get(name) is not None:
                record[name] = repr(record[name])
        stored.append({"count": count, **record})
    return stored


def prior_state_rows(segments, fragments):
    """What the prior segments query returns for the segments of a day."""
    fragment_by_id = {fragment["frag_id"]: fragment for fragment in fragments}
    rows = []
    for segment in segments:
        fragment = fragment_by_id[segment["frag_id"]]
        rows.append({
            "seg_id": segment["seg_id"],
            "frag_id": segment["frag_id"],
            "ssvid": segment["ssvid"],
            "first_timestamp": segment["first_timestamp"],
            "cumulative_msg_count": segment["cumulative_msg_count"],
            "cumulative_identities": as_stored(segment["cumulative_identities"]),
            "cumulative_destinations": as_stored(segment["cumulative_destinations"]),
            "last_msg_timestamp": fragment["last_msg_timestamp"],
            "last_msg_lat": fragment["last_msg_lat"],
            "last_msg_lon": fragment["last_msg_lon"],
            "last_msg_course": fragment["last_msg_course"],
            "last_msg_speed": fragment["last_msg_speed"],
        })
    return rows


def day_by_day(fragments, days=DAYS, prior_rows=()):
    """Segment map and segments computed one run per day, each starting from the day before."""
    updater = SegmentUpdater({})
    segmap, segments = [], []
    for day in days:
        day_fragments = [f for f in fragments if f["timestamp"] == day.timestamp()]
        outputs = list(updater.update((SSVID, {"fragments": day_fragments, "state": prior_rows})))
        day_segmap = [o.value for o in outputs if getattr(o, "tag", None) == SEGMAP]
        day_segments = [o for o in outputs if not hasattr(o, "tag")]
        segmap.extend(day_segmap)
        segments.extend(day_segments)
        prior_rows = prior_state_rows(day_segments, day_fragments)
    return segmap, segments


def normalized(rows):
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


@pytest.mark.parametrize("seed", range(25))
def test_day_by_day_matches_full_history(seed):
    fragments = make_fragments(random.Random(seed), vessels=random.Random(seed).randint(1, 3))

    expected_segmap, expected_segments = full_history(fragments)
    segmap, segments = day_by_day(fragments)

    assert normalized(segmap) == normalized(expected_segmap)
    assert normalized(segments) == normalized(expected_segments)


def test_generated_tracks_continue_segments():
    """Documents that the generated fragments exercise matching, not only new segments."""
    _, segments = full_history(make_fragments(random.Random(1), vessels=2))

    days_by_seg_id = defaultdict(int)
    for segment in segments:
        days_by_seg_id[segment["seg_id"]] += 1

    assert max(days_by_seg_id.values()) > 3
    assert any(segment["cumulative_identities"] for segment in segments)


def test_range_of_days_matches_day_by_day():
    fragments = make_fragments(random.Random(3), vessels=2)
    first_days, last_days = DAYS[:5], DAYS[5:]

    _, first_segments = day_by_day(fragments, first_days)
    prior_rows = prior_state_rows(
        [s for s in first_segments if s["timestamp"] == first_days[-1].timestamp()],
        fragments,
    )

    expected_segmap, expected_segments = day_by_day(fragments, last_days, prior_rows)

    updater = SegmentUpdater({})
    last_fragments = [f for f in fragments if f["timestamp"] >= last_days[0].timestamp()]
    outputs = list(updater.update((SSVID, {"fragments": last_fragments, "state": prior_rows})))
    segmap = [o.value for o in outputs if getattr(o, "tag", None) == SEGMAP]
    segments = [o for o in outputs if not hasattr(o, "tag")]

    assert normalized(segmap) == normalized(expected_segmap)
    assert normalized(segments) == normalized(expected_segments)


def test_state_without_new_fragments_yields_nothing():
    updater = SegmentUpdater({})
    fragments = make_fragments(random.Random(2), vessels=1)
    _, segments = day_by_day(fragments, DAYS[:1])
    state = prior_state_rows(segments, fragments)

    assert list(updater.update((SSVID, {"fragments": [], "state": state}))) == []


def test_incremental_segments_pipeline():
    fragments = make_fragments(random.Random(4), vessels=2)
    day, next_day = DAYS[0], DAYS[1]
    first_fragments = [f for f in fragments if f["timestamp"] == day.timestamp()]
    next_fragments = [f for f in fragments if f["timestamp"] == next_day.timestamp()]
    _, first_segments = day_by_day(fragments, [day])
    state = prior_state_rows(first_segments, first_fragments)
    expected_segmap, expected_segments = day_by_day(fragments, [next_day], state)

    transform = IncrementalSegments(
        prior_segments=beam.Create([(SSVID, row) for row in state]),
        merge_params={},
    )

    with TestPipeline() as p:
        new_fragments = p | beam.Create(next_fragments)
        outputs = new_fragments | transform

        assert_that(
            outputs[SEGMAP] | "SegmapJson" >> beam.Map(lambda x: normalized([x])[0]),
            equal_to(normalized(expected_segmap)),
            label="CheckSegmap",
        )
        assert_that(
            outputs[SEGMENTS] | "SegmentsJson" >> beam.Map(lambda x: normalized([x])[0]),
            equal_to(normalized(expected_segments)),
            label="CheckSegments",
        )


class UnpicklableClient:
    """Refuses to be pickled, like a BigQuery client does."""

    def __reduce__(self):
        raise pickle.PicklingError("Pickling client objects is explicitly not supported.")


class BigQueryHelperWithClient:
    labels = {}

    def __init__(self):
        self.client = UnpicklableClient()

    def fetch_table(self, table_id):
        return bigquery.Table(table_id)


def test_incremental_segments_graph_does_not_pickle_the_bigquery_client():
    prior_segments = ReadPriorSegments(
        BigQueryHelperWithClient(), "p.d.segments", "p.d.fragments", DAYS[0].date()
    )

    p = TestPipeline()
    new_fragments = p | beam.Create([])
    new_fragments | IncrementalSegments(prior_segments=prior_segments, merge_params={})


def test_full_history_segments_graph_does_not_pickle_the_bigquery_client():
    transform = FullHistorySegments(
        BigQueryHelperWithClient(), "p.d.fragments", DAYS[1].date(), DAYS[1].date(), {}
    )

    p = TestPipeline()
    new_fragments = p | beam.Create([])
    new_fragments | transform
