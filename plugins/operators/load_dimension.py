from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Optional


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 table: str = "",
                 sql_query: str = "",
                 mode: str = "append",  # Use 'append' or 'truncate-insert'
                 *args, **kwargs) -> None:
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context) -> None:
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(
            f"Starting to load data into dimension table {self.table}")

        if self.mode == "truncate-insert":
            self.log.info(f"Truncating {self.table} before inserting new data")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Inserting data into {self.table}")
        formatted_sql = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(formatted_sql)

        self.log.info(
            f"Success: Data loaded into dimension table {self.table}")
