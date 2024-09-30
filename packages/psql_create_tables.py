import pandas as pd 
import psycopg2
import time

from log import log_into_file



def create_table(conn) : 
    try :
        
        cursor = conn.cursor()
        
        conn.autocommit = True
        
        log_into_file("info", "Creating Table ... \n")
        time.sleep(3)
        
        cursor.execute("SELECT EXISTS ( \
        SELECT 1 \
        FROM information_schema.tables \
        WHERE table_schema = 'public' \
        AND table_name = 'Reed_Jobs' \
        );") 
        
        table1 = cursor.fetchone()[0]
        
        if table1 == 1 :
            log_into_file("info", "Table is already created ...\n")
            time.sleep(3)
        else : 
            log_into_file("info", "Table is created ...\n")
            time.sleep(3)
            
        cursor.execute('CREATE TABLE IF NOT EXISTS "Reed_Jobs" ( \
                                                        job_ID SERIAL PRIMARY KEY NOT NULL,\
                                                        title VARCHAR(200) NOT NULL, \
                                                        company VARCHAR(200) NOT NULL, \
                                                        salary VARCHAR(200) NOT NULL, \
                                                        location VARCHAR(200) NOT NULL, \
                                                        job_type VARCHAR(200) NOT NULL, \
                                                        link VARCHAR(200) NOT NULL);')
        
        check_index = "SELECT  1 FROM  pg_indexes  WHERE  indexname = 'job_title_index';"
        cursor.execute(check_index)
        if not cursor.fetchone() :
            cursor.execute('CREATE INDEX job_title_index ON "Reed_Jobs" (title);')
        
        time.sleep(3)
        cursor.close()
        conn.close()
    except Exception as e :
        log_into_file("error", e)