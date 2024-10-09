from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
from typing import Optional


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 table: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 log_json_file: Optional[str] = "auto",
                 *args, **kwargs) -> None:
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file

    def execute(self, context) -> None:
        # Retrieve AWS credentials
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(
            self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear data from Redshift table
        self.log.info(f"Clearing data from {self.table} table in Redshift")
        redshift.run(f"DELETE FROM {self.table}")

        # Copy data from S3 to Redshift
        self.log.info(f"Copying data from S3 to {self.table} in Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        json_format = self.log_json_file if self.log_json_file else "auto"

        # Formatted SQL COPY command
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            json_format
        )

        # Execute COPY command
        redshift.run(formatted_sql)
        self.log.info(
            f"Successfully copied data from {s3_path} to Redshift table {self.table}")
