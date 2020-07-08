from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class TruthToPostgresOperator(BaseOperator):
    """
    DAG operator to populate staging tables from source files.

    :param string  conn_id: reference to a specific redshift database
    :param string  table_name: staging table to load
    :param string  sql: SQL command to perform
    :param string  loadTable: True to load table; False does not load

    """

    ui_color = '#8080ff'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table_name,
                 sql,
                 loadTable,
                 *args,
                 **kwargs):

        super(TruthToPostgresOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.table_name = table_name
        self.sql = sql
        self.loadTable = loadTable

    def execute(self, context):
        self.log.info('TruthToPostgresOperator begin execute')

        # connect to database
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(
            f"Connected with {self.conn_id} -- {self.table_name} -> {self.sql}")

        if self.loadTable:
            pg_hook.run(f"DROP TABLE IF EXISTS {self.table_name} CASCADE")
            pg_hook.run(self.sql)
            self.log.info(
                f"TruthToPostgresOperator copy complete - {self.table_name}")
        else:
            self.log.info(
                f"TruthToPostgresOperator copy ignored - {self.table_name}")
