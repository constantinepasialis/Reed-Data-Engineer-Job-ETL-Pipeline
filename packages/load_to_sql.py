
import pandas as pd
import time 
import psycopg2

from log import log_into_file

def load(conn,dataframe) :
    try :
        log_into_file("info", "Loading data into database ... \n")
        time.sleep(3)

        connection = conn

        cursor = connection.cursor()
        
        connection.autocommit = True
        
        query = 'INSERT INTO "Reed_Jobs"(job_id, title, company, salary, location, job_type, link) \
                            VALUES(%s,%s,%s,%s,%s,%s,%s);'
        for i, row in dataframe.iterrows():
            
            data = (
                i + 1, 
                row['title'],
                row['company'],
                row['salary'],
                row['location'],
                row['job_type'],
                row['link'])
            
            cursor.execute(query, data)
        connection.close()
        
        log_into_file("info", "Data are loaded \n")
    except Exception as e :
        log_into_file("error", e) 
