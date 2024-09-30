import pandas as pd
import numpy as np
import time
from log import log_into_file
import extract
from refresh_table import refresh_table

def transform(conn,dataframe) :
    log_into_file("info", "Transforming Data ... \n")
    time.sleep(3)
        
    sql_query = 'SELECT title, company, salary, location, job_type, link FROM "Reed_Jobs";'
        
    database_data = pd.read_sql(
            sql=sql_query,
            con=conn
    )
    
    extracted_data = dataframe
    extracted_data.dropna(ignore_index=True, inplace=True)
    extracted_data.drop_duplicates(ignore_index=True, inplace=True)
    
    if not database_data.empty :
        extracted_data = pd.concat([database_data,extracted_data])
        refresh_table()
        extracted_data.dropna(ignore_index=True, inplace=True)
        extracted_data.drop_duplicates(ignore_index=True, inplace=True)
        
    transformed_data = extracted_data
    
    log_into_file("info", "Data are Transformed ... \n")
    time.sleep(3)
    
    return transformed_data