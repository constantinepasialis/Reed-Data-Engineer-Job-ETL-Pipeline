import psycopg2
import time

from log import log_into_file

def refresh_table() :
    try :
        conn = psycopg2.connect(database = "ETL_Reed_Jobs", user = "etl_user", password = "Dadinos8162022!", host = "localhost", port = 5432)

        cursor = conn.cursor()

        conn.autocommit = True

        cursor.execute('DELETE FROM "Reed_Jobs";')
        cursor.execute('DROP INDEX job_title_index')

        cursor.execute('VACUUM;')

        cursor.close()
        conn.close()
    except Exception as e :
        log_into_file("error", e)