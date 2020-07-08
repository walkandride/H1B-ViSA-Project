import re

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    DAG operator used for data quality checks.

    :param string conn_id: reference to a specific redshift database
    :param list check_data: data quality checks
    :param string type: switch used to determine type of quality checks to perform

    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 check_data,
                 type,
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.check_data = check_data
        self.type = type

    def execute(self, context):
        self.log.info('DataQualityOperator begin execute')
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f"Connected with {self.conn_id}")

        failed_tests = []

        if self.type == 'stage':
            for key in self.check_data:
                sql = f"SELECT COUNT(*) FROM {key}"
                table_count = (pg_hook.get_records(sql)[0])[0]
                filename, hasHeader = re.split('\|', self.check_data[key])
                with open(filename, "r", encoding='utf-8') as f:
                    numLines = len(f.readlines())
                if hasHeader == 'Y':
                    numLines = numLines - 1

                self.log.info(f"...[{key}] {table_count} =? {numLines}")
                if table_count != numLines:
                    failed_tests.append(
                        f"{key}, expected {numLines} got {table_count}\n")
        elif self.type == 'truth':
            for key in self.check_data:
                sql1 = f"SELECT COUNT(*) FROM {key}"
                sql2 = f"SELECT COUNT(*) FROM {self.check_data[key]}"
                result1 = (pg_hook.get_records(sql1)[0])[0]
                result2 = (pg_hook.get_records(sql2)[0])[0]
                if result1 != result2:
                    failed_tests.append(
                        f"{key}-{self.check_data[key]}, {result1} <> {result2}\n")
        elif self.type == 'truth2':
            for key in self.check_data:
                sql1 = key.get('dual_sql1')
                sql2 = key.get('dual_sql2')
                descr = key.get('descr')
                if sql1:
                    self.log.info(f"...[{descr}]\n  {sql1}\n  {sql2}")

                result1 = pg_hook.get_records(sql1)[0]
                result2 = pg_hook.get_records(sql2)[0]

                if result1[0] != result2[0]:
                    failed_tests.append(
                        f"Mismatch: {descr}\n  {sql1}\n  {sql2}")

        if len(failed_tests) > 0:
            self.log.info('Tests failed')
            self.log.info(failed_tests)
            raise ValueError('Data quality check failed')

        self.log.info("DataQualityOperator complete")
