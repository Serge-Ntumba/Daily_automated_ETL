from datetime import datetime, timedelta
import csv 
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile

default_args = {
    "owner" : "askin_owner",
    "retries" : 5,
    "retry_Delay" : timedelta(minutes=10)
}

#Main function
# Meaning of 'ds_nodash' is today's execution
# Meaning of 'next_ds_nodash' is next execution

def postgres_to_csv(ds_nodash , next_ds_nodash):
    #postgres connection
    hook = PostgresHook(postgres_conn_id = "postgres_local")
    conn = hook.get_conn()
    cursor = conn.cursor()
    # get the data between today and next execution
    cursor.execute("select * from orders where date >= %s and date < %s",
                   (ds_nodash, next_ds_nodash) 
                   )
    
    #create temporary file and upload data to s3 bucket.
    with NamedTemporaryFile(mode = "w", suffix = f"{ds_nodash}" ) as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow( [i[0] for i in cursor.description] )
        csv_writer.writerow(cursor)
        f.flush()
        cursor.close()
        cursor.close()
        
        #s3 bucket connection
        s3_hook = S3Hook(aws_conn_id = "connect_s3")
        s3_hook.load_file(
            filename = f.name,
            key = f"orders/{ds_nodash}.text",
            bucket_name = "products-orders",
            replace = True
        )
        logging.info(
            "orders file {ds_nodash} has been uploaded to s3 bucket", f.name
            )
        
#DAG
with DAG(
    dag_id = "data-from-postgres-to-s3", 
    default_args = default_args,
    start_date = datetime(2023, 12, 22),
    schedule_interval = "@daily"
)as dag:
    # task 
    task1 = PythonOperator(
        task_id = "data-from-postgres-to-s3",
        python_callable = postgres_to_csv
    )
    task1
        