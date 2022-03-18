from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append_only = False,
                 insert_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_only = append_only
        self.insert_query = insert_query
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Deleting data from target table if not append
        if not self.append_only:
            redshift.run("DELETE FROM {}".format(self.table))
        
        # Inserting data into target table
        query = """
                INSERT INTO {}
                {};
                """.format(self.table, self.insert_query, self.execution_date)
        
        # Run insert query
        redshift.run(query)
