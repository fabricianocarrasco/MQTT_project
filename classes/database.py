import sqlite3
from sqlite3 import Error


class database:
    def __init__(self, db_route, sql=None):
        """
        Initialize the class. It creates a connection to the db,
        and if a SQL sentence is given execute it.

        @param db_route - route of the database
        @param sql - SQL sentence to execute
        """
        self.db_route = db_route
        self.conn = self.create_connection(db_route)

        if sql is not None:
            self.execute_sql(self.conn, sql)

    def __del__(self):
        """
        Close the connection to the database.
        """
        self.conn.close()

    def create_connection(self, db_route):
        """
        Create a connection to the specified database.

        @param db_route - route of the database

        @return conn - connection to the database or throw error
        """

        conn = None
        try:
            conn = sqlite3.connect(db_route)
            print("Database connected")
            return conn
        except Error as e:
            print(e)

    def execute_sql(self, conn, sql):
        """
        Execute a SQL statement or throw error.

        @param conn - connection to the database
        @param sql - SQL statement to be executed
        """
        try:
            c = conn.cursor()
            c.execute(sql)
            print("SQL executed")
        except Error as e:
            print(e)


# Connect to database and create IOT table if doesn't exist.
if __name__ == "__main__":
    # Parameters to connect and create the table
    db_route = "data/sensor_data.db"
    sql_create_iot_table = """ CREATE TABLE IF NOT EXISTS iot(
                                        id text PRIMARY KEY,
                                        sensor_id text,
                                        sensor_type text,
                                        value integer,
                                        timestamp integer
                                    ); """
    # Creation of the connection and table
    database(db_route, sql_create_iot_table)
