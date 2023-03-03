from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = "#704684"

    insert_sql = """
        INSERT INTO {}
        {};
    """

    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(
        self, table="", redshift_conn_id="", sql="", truncate=False, *args, **kwargs
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift_hook.run(LoadDimensionOperator.truncate_sql.format(self.table))

        self.log.info(f"Loading dimension table {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, self.sql)

        self.log.info(f"Insertion of dim table:\n {formatted_sql}")
        redshift_hook.run(formatted_sql)
