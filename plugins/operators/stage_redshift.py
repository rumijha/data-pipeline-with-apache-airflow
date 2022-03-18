from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_conn_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "" , 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.execution_date = kwargs.get('execution_date')
        
    def execute(self, context):
        self.log.info("Fetching connection details!")
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        # Deleting data from the target table
        self.log.info("Deleting records from target table before copying the data")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # Query to copy data into target table
        self.log.info("Fetching data from S3")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info("Started copying data from S3 to Redshift table")
        query = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{{}}'
                SECRET_ACCESS_KEY '{{}}'
                IGNOREHEADER 1
                DELIMITER ','
               """.format(self.table,
                              s3_path,
                              credentials.access_key,
                              credentials.secret_key,
                              self.execution_date)
        
        # Run Copy data Query
        redshift.run(query)
        self.log.info("Finished copying data to Redshift stage tables")

       

