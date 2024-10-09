from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Optional


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 table: str = "",
                 sql_query: str = "",
                 *args, **kwargs) -> None:
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context) -> None:
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Starting data load into fact table {self.table}")
        formatted_sql = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(formatted_sql)

        self.log.info(f"Data successfully loaded into fact table {self.table}")
