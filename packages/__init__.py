from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime as dt

from extract import extract 
from psql_create_tables import create_table
from transform import transform
from load_to_sql import load

# connection = Connection(
#     conn_id="ETL_Jobs_Con_ID",
#     conn_type="postgres",
#     host="127.0.0.1",
#     login="etl_user",
#     password="Dadinos8162022!",
# )

pg_hook = PostgresHook(postgres_conn_id = "ETL_Jobs_Con_ID")

conn = pg_hook.get_conn()

default_args = {
    "owner" : "constantinosPasialis",
    "start_date" : dt.datetime(2024,9,29),
    "retries" : 1,
    "retry_delay" : dt.timedelta(hours = 1)
}

@dag(default_args = default_args, schedule=dt.timedelta(days = 4), catchup=False, tags = ["ETL_Jobs"])
def etl_workflow():

    @task()
    def table_creation():
        create_table(conn)
        
    @task()
    def data_extraction():
        extracted_data = extract()
        return extracted_data
    
    @task()
    def data_transformation(extracted_data):
        transformed_data = transform(conn, extracted_data)
        return transformed_data
    
    @task()
    def data_load(transformed_data):
        load(conn, transformed_data)
    
    table_task = table_creation()
    extracted_data = data_extraction()
    transformed_data = data_transformation(extracted_data)
    data_loading = data_load(transformed_data)
    
    table_task >> extracted_data >> transformed_data >> data_loading
etl_workflow()
