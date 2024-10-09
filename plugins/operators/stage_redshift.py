from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


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
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 log_json_file="auto",
                 * args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_connection = metastoreBackend.get_connection(
            self.aws_credentials_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to destination Redshift table")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        json_format = self.log_json_file if self.log_json_file != "" else "auto"

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            json_format
        )

        redshift.run(formatted_sql)
        self.log.info(
            f"Successfully copied data from S3 to destination Redshift table: {self.table}")
