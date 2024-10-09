from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftSQLOperator(BaseOperator):
    template_fields = ('sql',)
    template_fields_renderers = {'sql': 'sql'}
    template_ext = ('.sql',)
    ui_color = '#99e699'

    @apply_defaults
    def __init__(self,
                 *,
                 sql: str = '',
                 redshift_conn_id: str = 'redshift',
                 autocommit: bool = True,
                 **kwargs) -> None:
        super().__init__(*kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.autocommit = autocommit

    def execute(self, context) -> None:
        """
        Executes the provided SQL statements on the Redshift cluster.
        Supports either a single SQL string or a list of SQL queries.
        """
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        try:
            # Check if 'sql' is a string or a list and execute accordingly
            if isinstance(self.sql, str):
                self.log.info(f"Executing SQL query on Redshift: {self.sql}")
                postgres_hook.run(self.sql, self.autocommit)
            elif isinstance(self.sql, list):
                for query in self.sql:
                    self.log.info(f"Executing SQL query on Redshift: {query}")
                    postgres_hook.run(query, self.autocommit)
            else:
                raise ValueError(
                    "SQL parameter should be either a string or a list of strings.")

            self.log.info('SQL queries executed successfully!')
        except Exception as e:
            self.log.error(f"Error executing SQL query: {str(e)}")
            raise
