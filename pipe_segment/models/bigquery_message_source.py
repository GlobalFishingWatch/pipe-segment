from datetime import datetime
from jinja2 import Template


class BigQueryMessagesSource:

    def __init__(
        self,
        table_id: str,
        filter_template: str = None,
        is_sharded: bool = True
    ):
        self.table_id = table_id
        self.filter_template = filter_template
        self.is_table_date_sharded = is_sharded

    def filter_messages(self, start_date: datetime, end_date: datetime) -> str:
        template = self.filter_template
        if template is None:
          if self.is_table_date_sharded:
              template = "_TABLE_SUFFIX BETWEEN '{{ start_date.strftime('%Y%m%d') }}' AND '{{ end_date.strftime('%Y%m%d') }}'"
          else:
              template = "timestamp BETWEEN '{{ start_date.strftime('%Y-%m-%d') }}' AND '{{ end_date.strftime('%Y-%m-%d') }}'"
            
        return Template(template).render({'start_date': start_date, 'end_date': end_date})

    @property
    def qualified_source_messages(self) -> str:
        return f'{self.table_id}{'*' if self.is_table_date_sharded else ''}'