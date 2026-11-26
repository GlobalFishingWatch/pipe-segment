"""Transform that keeps only the fields of the segmented messages table."""

import apache_beam as beam
from pipe_segment.schemas import message_schema


class WhitelistFields(beam.PTransform):
    """Keeps only the whitelisted fields of each message.

    Args:
        fieldnames:
            Names of the fields to keep. By default, the fields of the segmented messages table.
    """

    def __init__(self, fieldnames=message_schema.fieldnames):
        """Initializes the transform."""
        self._fieldnames = fieldnames

    def whitelist(self, elem):
        """Returns the whitelisted fields of a message.

        Args:
            elem:
                The message.
        """
        return {field: elem[field] for field in self._fieldnames if field in elem}

    def expand(self, xs):
        """Whitelists the fields of each message."""
        return xs | beam.Map(self.whitelist)
