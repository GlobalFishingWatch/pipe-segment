import pytest
import itertools as it
import collections

import stats


# recipe from itertools documentation
def consume(iterator, n):
    "Advance the iterator n-steps ahead. If n is none, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(it.islice(iterator, n, n), None)


class TestStats:

    @pytest.mark.parametrize("data, subset, expected", [
        ({'a': 1, 'b': 2, 'c': 3}, ['a', 'b'], {'a': 1, 'b': 2}),
        ({'a': 1, 'b': 2, 'c': 3}, ['a', 'not_present'], {'a': 1}),
        ({'a': 1, 'b': 2, 'c': 3}, ['not_present'], {})
    ])
    def test_dict_subset(self, data, subset, expected):
        assert expected == stats.dict_subset(data, subset)

    @pytest.mark.parametrize("messages, filter_fields, test_field, expected", [
        ([], [], 'a', []),
        ([], ['a'], 'a', []),
        ([{'a': 'apple', 'b': 'boat'}], ['a'], 'a', [('apple', 1)]),
        ([{'a': 'apple', 'b': 'boat'}], ['a'], 'b', []),
        ([
            {'a': 'apple', 'b': 'boat'},
            {'a': 'pear', 'b': 'boat'},
            {'a': 'apple', 'b': 'boat'},
        ], ['a'], 'a', [('apple', 2)]),
    ])
    def test_MessageFieldCounter(self, messages, filter_fields, test_field, expected):
        mfc = stats.MessageFieldCounter(messages, filter_fields)
        consume(mfc, n=None)  # run all the messages through the counter
        assert list(mfc.most_common(test_field)) == expected

    def test_MessageStats(self):
        messages = [
            {'mmsi': 123, 'timestamp': 0.0, 'lat': 1, 'speed': 2},
            {'mmsi': 123, 'timestamp': 1.0, 'lat': 0, 'speed': 3},
            {'mmsi': 123, 'timestamp': 1.0, 'name': 'boaty'},
            {'mmsi': 123, 'timestamp': 2.0, 'lat': 2, 'speed': 2}
        ]

        numeric_fields = ['timestamp', 'lat', 'speed']
        frequency_fields = ['mmsi', 'name']

        ms = stats.MessageStats(messages, numeric_fields, frequency_fields)

        actual = ms.numeric_stats('lat').items()
        expected = {'min': 0, 'max': 2, 'first': 1, 'last': 2, 'count': 3}.items()
        assert set(expected).issubset(set(actual))

        assert ms.frequency_counter.most_common('name') == [('boaty', 1)]

    def test_MessageStats_edge_cases(self):
        messages = [
            {'mmsi': 123, 'timestamp': 0.0, 'lat': 1, 'speed': 2},
        ]

        numeric_fields = ['not_present']
        frequency_fields = []

        ms = stats.MessageStats(messages, numeric_fields, frequency_fields)

        assert ms.numeric_stats('not_present') == {}

    def test_MessageStats_none(self):
        messages = [{'not_present': None}]

        numeric_fields = ['not_present']
        frequency_fields = []

        ms = stats.MessageStats(messages, numeric_fields, frequency_fields)

        assert ms.numeric_stats('not_present') == {}
