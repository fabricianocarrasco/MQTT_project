import sqlite3
from sqlite3 import Error


def create_connection():
    """ create a database connection to a database that resides
        in the memory
    """
    conn = None;
    try:
        conn = sqlite3.connect(':memory:')
        print(sqlite3.version)
        return conn
    except Error as e:
        print(e)
    # finally:
        # if conn:
            
            # conn.close()

def create_table(conn,create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        print('Table created!')
    except Error as e:
        print(e)

if __name__ == '__main__':
    conn = create_connection()
    sql_create_iot_table = """ CREATE TABLE IF NOT EXISTS iot(
                                        id text PRIMARY KEY,
                                        sensor_id text,
                                        sensor_type text,
                                        value integer,
                                        timestamp integer
                                    ); """
    create_table(conn,sql_create_iot_table)