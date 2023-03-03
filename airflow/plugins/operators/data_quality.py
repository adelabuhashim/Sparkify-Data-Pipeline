from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self, redshift_conn_id="", tables=[], schema="", dq_checks=[], *args, **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.schema = schema
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            for check in self.dq_checks:
                self.log.info(f"Data quality check on {self.schema}.{table}")
                records = redshift_hook.get_records(
                    check["check_sql"].format(self.schema, table)
                )
                if records[0][0] <= check["expected_result"]:
                    raise ValueError(
                        f"Data quality check failed. {self.schema}.{table} contained 0 rows"
                    )
                self.log.info(
                    f"Data quality on table {self.schema}.{table} check passed with {records[0][0]} records"
                )
