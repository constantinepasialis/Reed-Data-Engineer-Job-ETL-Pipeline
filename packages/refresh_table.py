#IMPORT LIBRARIES
import psycopg2

from log import log_into_file

def refresh_table() :
    try :
        conn = psycopg2.connect(database = "ETL_Reed_Jobs", user = "etl_user", password = "*****", host = "localhost", port = ****)

        cursor = conn.cursor()

        conn.autocommit = True

        cursor.execute('DELETE FROM "Reed_Jobs";')

        cursor.execute('VACUUM;')

        cursor.close()
        conn.close()
    except Exception as e :
        log_into_file("error", e)
