from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToPostgresOperator(BaseOperator):
    """
    DAG operator to populate staging tables from source files.

    :param string  conn_id: reference to a specific redshift database
    :param string  table_name: staging table to load
    :param string  filename: filename
    :param string  delimiter: delimiter used in file
    :param string  loadTable: True to load table; False does not load

    """

    ui_color = '#c0c0c0'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table_name,
                 filename,
                 delimiter,
                 loadTable,
                 *args,
                 **kwargs):

        super(StageToPostgresOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.table_name = table_name
        self.filename = filename
        self.delimiter = delimiter
        self.loadTable = loadTable

    def execute(self, context):
        self.log.info('StageToPostgresOperator begin execute')

        # connect to database
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f"Connected with {self.conn_id}")

        sql_stmt = f"""
            COPY {self.table_name}
            FROM STDIN
            WITH CSV HEADER 
            DELIMITER AS '{self.delimiter}'
        """

        self.log.info(f"copy sql: {sql_stmt} from {self.filename}")
        if self.loadTable:
            pg_hook.run(f"TRUNCATE TABLE {self.table_name}")
            pg_hook.copy_expert(sql_stmt, self.filename)
            self.log.info(
                f"StageToPostgresOperator copy complete - {self.table_name}")
        else:
            self.log.info("StageToPostgresOperator copy ignored")
