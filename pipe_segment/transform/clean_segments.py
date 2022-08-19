import logging

from apache_beam import FlatMap, PTransform
from .util import swap_null, convert_idents


class CleanSegments(PTransform):

    def aggregate_cumulative_fields(self, seg):
        '''
        Combines identical entries in cumulative fields 
        (i.e. cumulative_identities) with different
        counts into a single entry with the counts summed together. 
        Multiple entries for the same identity can happen when 
        the segmenter is run into multiple time chunks (i.e. monthly).

        Args:
            seg: current segment in iterator

        Returns:
            Segment with only one entry per identity or destination
            in cumulative fields.
        '''
        cumulative_idents = {}
        cumulative_dests = {}

        for ident in seg["cumulative_identities"]:
            ident = ident.copy()
            ident_cnt = ident.pop("count")
            key = tuple([(k, str(swap_null(v))) for (k, v) in sorted(ident.items())])
            cumulative_idents[key] = cumulative_idents.get(key, 0) + ident_cnt

        for dest in seg["cumulative_destinations"]:
            dest = dest.copy()
            dest_cnt = dest.pop("count")
            key = tuple([(k, str(swap_null(v))) for (k, v) in sorted(dest.items())])
            cumulative_dests[key] = cumulative_dests.get(key, 0) + dest_cnt

        seg_clean = seg.copy()
        seg_clean["cumulative_identities"] = convert_idents(cumulative_idents)
        seg_clean["cumulative_destinations"] = convert_idents(cumulative_dests)

        yield seg_clean

    def expand(self, pcoll):
        return pcoll | FlatMap(self.aggregate_cumulative_fields)
