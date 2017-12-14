from apache_beam.io.filesystems import FileSystems


def parse_gcp_path(path):
    """ parse a custom gcp path and determine which apache beam source/sink type that path refers to.

    returns a tuple of (service, path)
    where service indicates the apache beam source/sink type to use and path is the provided path with
    the scheme:// stripped off the front


    Examples

    Bigquery table          bq://project:dataset.table          (table, project:dataset.table)
    Bigquery query          "query://select * from [Table]"     (query, select * from [Table])
    GCS file                gs://bucket/path/file               (text, gs://bucket/path/file )
    Local file              file://path/file                    (text, file://path/file)
    Local file (absolute)   /path/file                          (text, /path/file )
    Local file (relative)   ./path/file                         (text, ./path/file)
    Bigquery query          "select * from [Table]"             (query, select * from [Table])
    """

    scheme = FileSystems.get_scheme(path)

    if scheme == 'query':
        # path contains a sql query
        # strip off the scheme and just return the rest
        return 'query', path[8:]
    elif scheme == 'bq':
        # path is a reference to a big query table
        # strip off the scheme and just return the table id in path
        return 'table', path[5:]
    elif scheme == 'gs':
        # path is a Google Cloud Storage reference
        return 'file', path
    elif scheme is None:
        # could be a local file or a sql query
        if path[0] in ('.', '/'):
            return 'file', path
        else:
            return 'query', path
    else:
        raise ValueError("Unknown scheme %s" % scheme)