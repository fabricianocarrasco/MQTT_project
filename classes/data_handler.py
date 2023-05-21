import json
import time
from classes.database import database


class data_statistics:
    def __init__(self):
        """
        Initialize the mean and std variables to None.
        """
        self.mean = None
        self.std = None

    def update(self, mean, std):
        """
        Update the mean and standard deviation.

        @param mean - the mean value to update
        @param std - the standard deviation value to update
        """
        self.mean = mean
        self.std = std

    def get_mean(self):
        """
        Get the mean of the data.

        @return The mean of the data
        """
        return self.mean

    def get_std(self):
        """
        Get the standard deviation of the data.

        @return The standard deviation
        """
        return self.std


class data_handler:
    def __init__(self, client, database):
        """
        Initializes the process to obtain the data and save it in the database.

        @param client - the client to connect to obtain the data
        @param database - the database to connect to
        """
        # Set the client, database and statistics as parameters
        self.client = client
        self.database = database
        self.statistics = data_statistics()
        # Set on_message and on_connect callback functions on the MQTT client
        self.client.set_on_message(self.on_message)
        self.client.set_on_connect(self.on_connect)
        # Connects to the broker and subscribe (check on_connect function)
        self.client.connect()

    def update_variables(self, num_registers_to_check, db_route):
        """
        Update the mean and standard deviation from  sensor data.
        """
        sql = """SELECT AVG(value) as mean,SUM((value-(SELECT AVG(value) FROM iot))*
            (value-(SELECT AVG(value) FROM iot)) ) / (COUNT(value)-1) AS var
            FROM iot ORDER BY timestamp DESC LIMIT """ + str(
            num_registers_to_check
        )
        # Creates a database connection
        db = database(db_route)
        cur = db.conn.cursor()
        # Execute the SQL sentence
        data = cur.execute(sql).fetchall()[0]
        # Check if data is obtained from the SQL sentence
        if (data[0] and data[1]) is not None:
            self.statistics.update(data[0], data[1] ** 0.5)
        print(self.statistics.get_mean(), self.statistics.get_std())
        # Commit sentence and delete connection to database
        db.conn.commit()
        del db

    def detect_outlier(self, sensor_data):
        """
        Checks if the sensor data is outlier.
        It returns True if the sensor data is in the range

        @param sensor_data - data from the sensor

        @return True if the sensor data is outlier
        False if there is no data to compare or if the value is in the range
        """

        # If there is not statistics to compare to, return False
        if (self.statistics.mean and self.statistics.std) is None:
            return False

        # Define as outlier those who deviates 3 times from the mean
        low = self.statistics.mean - 3 * self.statistics.std
        high = self.statistics.mean + 3 * self.statistics.std

        # Returns true if data is in range, False otherwise.
        if low <= sensor_data <= high:
            return True
        else:
            return False

    def create_sensor_data(self, sensor_data):
        """
        Create new sensor data in Iot table.

        @param sensor_data - array containing data to insert

        @return 'Not checking outliers' if there is no statistics
        'Outlier' if the data value is out of range
        Row number is succesfully inserted in database
        """

        # Check if there is statistics, if not,
        # insert the sensor data into iot table
        if (self.statistics.mean and self.statistics.std) is None:
            sql = """ INSERT INTO iot(id,sensor_id,sensor_type,value,timestamp)
                VALUES(?,?,?,?,?) """
            cur = self.database.conn.cursor()
            cur.execute(sql, sensor_data)
            self.database.conn.commit()
            return "Not checking outliers"

        # Detect outlier sensor data and exit if detected
        if not self.detect_outlier(sensor_data[3]):
            return "Outlier"

        # Insert data in IOT table
        sql = """ INSERT INTO iot(id,sensor_id,sensor_type,value,timestamp)
                VALUES(?,?,?,?,?) """
        cur = self.database.conn.cursor()
        cur.execute(sql, sensor_data)
        self.database.conn.commit()

        return cur.lastrowid

    def on_message(self, client, userdata, message):
        """
        Callback function to run when a message is received from the broker.

        @param client - the client that sent the message (unused)
        @param userdata - the data from the user that sent the data (unused)
        @param message - the message that was received from the MQTT
        """
        # Create the variables to input to the database
        timestamp = time.time_ns()
        sensor_id = json.loads(message.payload.decode())["sensor_id"]
        sensor_type = json.loads(message.payload.decode())["sensor_type"]
        value = json.loads(message.payload.decode())["value"]
        id = str(timestamp) + sensor_id + sensor_type + str(value)
        # Input the data to the database
        row_id = self.create_sensor_data(
            [id, sensor_id, sensor_type, value, timestamp]
        )
        print(row_id)

    def on_connect(self, client, userdata, flags, rc):
        """
        Called function to run when the connection has been established.
        Subscribes to the topic specified.

        @param client - the MQTT client that is being connected to (unused)
        @param userdata - the user data ( unused )
        @param flags - flags passed to the MQTT handler ( unused )
        @param rc - the return code ( unused )
        """
        print("Connected")
        self.client.client.subscribe(self.client.topic)
