from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import List


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 tables: List[str] = None,  # List of table names
                 *args, **kwargs) -> None:
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context) -> None:
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Checking data quality for table {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                raise ValueError(
                    f"DATA QUALITY CHECK FAILED. {table} has no records")

            self.log.info(
                f"DATA QUALITY CHECK PASSED. {table} has {records[0][0]} records")
