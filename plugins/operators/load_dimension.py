from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 truncate_table=False,
                 insert_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_table = truncate_table
        self.insert_query = insert_query
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Deleting data from target table     
        if self.truncate_table:
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        # Inserting data into target table
        query = """
                INSERT INTO {}
                {};
                """.format(self.table, self.insert_query, self.execution_date)
        
        # Run insert query
        redshift.run(query)
        
        
