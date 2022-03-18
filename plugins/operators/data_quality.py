from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info("Establishing Redshift Connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Checking if table is empty")
        count_sql = """
                     Select count(*) from {}
                    """
        
        dq_count_check = count_sql.format(self.table)
    
        count = redshift.get_records(dq_count_check)
        
        if count is None or len(count[0]) < 1:
            raise logging.error(f"No records present in destination table {self.table}")
            raise ValueError(f"No records present in destination table {self.table}")
        else:
            self.log.info(f"Data quality on table {self.table} check passed with {count[0][0]} records")