import pandas as pd
import numpy as np
import itertools as it
from collections import defaultdict
from collections import Counter


def dict_subset(d, fields):
    # return a subset of the provided dict containing only the
    # fields specified in fields
    return {k: v for k, v in d.iteritems() if k in fields}


class MessageFieldCounter:
    """
    Count occurrences of values in a stream of messages for a specified set of fields
    Usage:

       messages = [
             {'a': 'apple', 'b': 'boat'},
             {'a': 'pear', 'b': 'boat'},
             {'a': 'apple', 'b': 'boat'},
             ]
       fields = ['a', 'b']
       mfc = MessageFieldCounter(messages, fields)
       # this class is designed to pass through a long stream of messages
       # so we have to pull them through in order to count them
       for msg in mcf:
              pass
        print mfc.most_common('a')
        >>> [('apple', 2)]
    """

    def __init__(self, messages, fields):
        self.fields = set(fields)
        self.messages = messages
        self.counters = defaultdict(Counter)

    def __iter__(self):
        return self.process()

    def process(self):
        for msg in self.messages:
            for key in self.fields:
                value = msg.get(key, None)
                if value is not None:
                    self.counters[key][value] += 1

            yield msg

    def most_common(self, field, n=1):
        return self.counters[field].most_common(n)


class MessageStats():
    """
    Extract a set of stats from as stream of messages.

    numeric_fields: list of field names to compute numeric stats (eg min, max, avg)
    frequency_fields: list of field names to compute frequency of values
    """

    def __init__(self, messages, numeric_fields, frequency_fields):
        self.numeric_fields = numeric_fields
        self.frequency_fields = frequency_fields

        self.counter = MessageFieldCounter(messages, frequency_fields)
        messages = self.counter.process()
        messages = it.imap(dict_subset, messages, it.repeat(numeric_fields))
        self.df = pd.DataFrame(list(messages))

    @property
    def frequency_counter(self):
        return self.counter

    @property
    def data_frame(self):
        return self.df

    def numeric_stats(self, field):
        col = self.df[field]
        return dict(
            min=np.nanmin(col),
            max=np.nanmax(col),
            first=col[col.first_valid_index()],
            last=col[col.last_valid_index()],
            count=np.count_nonzero(~np.isnan(col)),
        )
