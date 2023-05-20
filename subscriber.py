import paho.mqtt.client as mqtt
import json
import time
from database import create_connection, create_table
from threading import Thread


class mqtt_client:
    def __init__(self, broker_address, broker_port, topic, client_id, client_pwd):
        """
        Initializes the suscriber_test class. This is the constructor for the SuscriberTest class.

        @param broker_address - The address of the MQTT broker.
        @param broker_port - The port of the MQTT broker.
        @param topic - The topic to send messages to. This can be a string or a file - like object.
        @param client_id - The client id to use for authenticating with the broker.
        @param client_pwd - The client password to use for authenticating with the broker
        """
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.topic = topic
        self.client_id = client_id
        self.client_pwd = client_pwd
        # Creating new instance
        self.client = mqtt.Client("suscriber_test")
        self.client.tls_set()
        self.client.username_pw_set(username=client_id, password=client_pwd)

    def set_on_message(self, func):
        """
        Set function to call when messages are received. The function should take two arguments the message and the client

        @param func - function to call when
        """
        self.client.on_message = func

    def set_on_connect(self, func):
        """
        Set function to call when connection is established. The function should take two arguments the connection object and the error object

        @param func - function to call when
        """
        self.client.on_connect = func

    def connect(self):
        """
        Connect to the broker and send a message to the client. This is called by the constructor and should not be called directly
        """
        print(self.broker_address, self.broker_port)
        self.client.connect(self.broker_address, self.broker_port)
        print("Connecting...")


class updater_thread(Thread):
    def __init__(self, func):
        """
        Start the thread in a daemon mode. This is a wrapper around Thread. __init__ to allow the thread to be started in a daemon mode

        @param func - The function to run
        """
        """
        Start the thread in a daemon mode.
        """
        Thread.__init__(self)
        self.daemon = True
        self.func = func
        self.start()

    def run(self):
        """
        Runs the thread. This is the method that will be called by the run method of the Thread class
        """
        """
        Update variables every time we start a new run. This is a blocking call so we don't have to worry about threading
        """
        # Update variables in the current thread.
        # Loop through all the functions in the loop
        while True:
            self.func()


class data_statistics:
    def __init__(self):
        """
        Initialize the mean and std variables to None. This is called by __init__ and should not be called directly
        """
        self.mean = None
        self.std = None

    def update(self, mean, std):
        """
        Update the mean and standard deviation. This is called after a change in the parameter values to ensure that the values are up to date

        @param mean - The mean of the parameter values
        @param std - The standard deviation of the parameter values ( must be in the range 0 to 1
        """
        self.mean = mean
        self.std = std

    def get_mean(self):
        """
        Get the mean of the data. This is used to determine the number of points that have been added to the dataset before this method is called.


        @return The mean of the data in units of : math : ` m^ { - 1 } `. If the dataset is univariate the mean is calculated by taking the mean of each point
        """
        return self.mean

    def get_std(self):
        """
        Get the standard deviation of the Gaussian. This is a convenience method for getting the value of the : attr : ` std ` attribute.


        @return The standard deviation of the Gaussian in Hz ( float between 0 and 1 ). Note that the value will be truncated to the nearest integer
        """
        return self.std


class database_connection:
    def __init__(self, database):
        """
        Initialize the class. This is the method that should be called by the user. It creates the database if it doesn't exist

        @param database - name of the database
        """
        self.database = database
        self.conn = create_connection(database)
        sql_create_iot_table = """ CREATE TABLE IF NOT EXISTS iot(
                                        id text PRIMARY KEY,
                                        sensor_id text,
                                        sensor_type text,
                                        value integer,
                                        timestamp integer
                                    );
                            """
        create_table(self.conn, sql_create_iot_table)

    def __del__(self):
        """
        Close the connection to the MySQL server. This is called by __del__ so we don't have to worry about it
        """
        self.conn.close()


class data_handler:
    def __init__(self, client, database):
        """
        Initializes the connection to the database. This is the method that must be called by the client when it connects

        @param client - The client to connect to the database
        @param database - The database to connect to ( optional default :
        """
        self.client = client
        self.database = database
        self.statistics = data_statistics()
        # self.updater_thread = updater_thread()
        self.client.set_on_message(self.on_message)
        self.client.set_on_connect(self.on_connect)
        self.client.connect()

    def update(self, mean, std):
        """
        Update the mean and standard deviation. This is called after a call to : meth : ` set_mean ` and

        @param mean - The mean to use for the statistics.
        @param std - The standard deviation to use for the statistics. If this is None it will be set to the mean
        """
        self.statistics.mean = mean
        self.statistics.std = std

    def get_mean(self):
        """
        Get the mean of the data. This is an alias for : py : meth : ` stats. mean `.


        @return The mean of the data in the data set as a float or ` None ` if there is no
        """
        return self.statistics.mean

    def get_std(self):
        """
        Get the standard deviation of the data. This is a copy of the : attr : ` stats. std ` attribute.


        @return The standard deviation of the data in the data set as a float or None if there is no data
        """
        return self.statistics.std

    def update_variables(self):
        """
        Update the mean and standard deviation of sensor data. This is called every second to update the variables and the statistics
        """
        """
        Update mean and standard deviation of sensor data. This is called every second to update the variables
        """
        time.sleep(1)
        sql = """SELECT AVG(value) as mean,SUM((value-(SELECT AVG(value) FROM iot))*
            (value-(SELECT AVG(value) FROM iot)) ) / (COUNT(value)-1) AS var
            FROM iot ORDER BY timestamp DESC LIMIT 1000"""
        database = database_connection(self.database.database)
        cur = database.conn.cursor()
        data = cur.execute(sql).fetchall()[0]
        self.statistics.update(data[0], data[1] ** 0.5)
        print(self.get_mean(), self.get_std())
        database.conn.commit()
        del database

    def detect_outlier(self, sensor_data):
        """
        Checks if the sensor data is outlier. It returns True if the sensor data is in the range of the mean and standard deviation

        @param sensor_data - The value of the sensor

        @return True if the sensor data is outlier False if not or if the value is not in the range
        """
        """
        Checks if the sensor data is outlier. It returns True if the sensor data is in the range of the mean and standard deviation and False otherwise

        @param sensor_data - The value of the sensor

        @return True if the sensor data is in the range of the mean and standard deviation and False otherwise ( in this case the value is not in the range
        """

        # Return True if mean and standard deviation are not in mean and std.
        # Returns True if the statistics are not mean and standard deviation.
        if not (self.statistics.mean and self.statistics.std):
            return False

        # Identificar los valores que están fuera de 3 desviaciones estándar del valor medio
        low = self.statistics.mean - 1 * self.statistics.std
        high = self.statistics.mean + 1 * self.statistics.std

        # Filtrar los valores
        # Returns true if sensor data is within the low and high range.
        # Returns true if sensor data is within the low and high range.
        if low <= sensor_data <= high:
            return True
        else:
            return False

    def create_sensor_data(self, sensor_data):
        """
        Create new sensor data in Iot table. This function is called by : func : ` create_sensor ` to create new sensor data in Iot table.

        @param sensor_data - tuple containing data to be inserted.

        @return ' First second'' Outlier'or error message if something went wrong. Empty string otherwise.
        """

        # Insert the sensor data into iot table
        if not (self.statistics.mean and self.statistics.std):
            sql = """ INSERT INTO iot(id,sensor_id,sensor_type,value,timestamp)
                VALUES(?,?,?,?,?) """
            cur = self.database.conn.cursor()
            cur.execute(sql, sensor_data)
            self.database.conn.commit()
            return "First second"

        # Detect outlier sensor data.
        if not self.detect_outlier(sensor_data[3]):
            return "Outlier"

        sql = """ INSERT INTO iot(id,sensor_id,sensor_type,value,timestamp)
                VALUES(?,?,?,?,?) """
        cur = self.database.conn.cursor()
        cur.execute(sql, sensor_data)
        self.database.conn.commit()

        return cur.lastrowid

    def on_message(self, client, userdata, message):
        """
        Callback when a message is received from the MQTT client. This is called by Twisted when a message arrives from the MQTT client

        @param client - The client that sent the message
        @param userdata - The data sent to the client as part of the callback
        @param message - The message that was received from the MQTT
        """
        # print("message received ", str(message.payload.decode("utf-8")))
        # print("message topic=", message.topic)
        # print("message ", json.loads(message.payload.decode())["sensor_id"])
        # print("message ", json.loads(message.payload.decode())["sensor_type"])
        # print("message ", json.loads(message.payload.decode())["value"])
        # print("message ", json.loads(message.payload.decode())["sensor_type"])
        # print("userdata ", userdata)
        timestamp = time.time_ns()
        sensor_id = json.loads(message.payload.decode())["sensor_id"]
        sensor_type = json.loads(message.payload.decode())["sensor_type"]
        value = json.loads(message.payload.decode())["value"]
        id = str(timestamp) + sensor_id + sensor_type + str(value)
        row_id = self.create_sensor_data([id, sensor_id, sensor_type, value, timestamp])
        print(row_id)

    def on_connect(self, client, userdata, flags, rc):
        """
        Called when the connection to MQTT has been established. Subscribes to the topic specified in the topic_config.

        @param client - The MQTT client that is being connected to
        @param userdata - The user data ( unused )
        @param flags - Flags passed to the MQTT handler ( unused )
        @param rc - The return code ( unused ) This is a callback function that is called in response to the QMP CONNECT command
        """
        print("Connected")
        client.subscribe(topic)


# Main function to handle MQTT broker connection information
if __name__ == "__main__":
    # HiveMQ broker connection information
    broker_address = "300de1d0fce741319264fb560810a851.s2.eu.hivemq.cloud"
    broker_port = 8883
    topic = "FabriTopics/sensor_data_ikerlan"
    client_id = "fabriSubscriber"
    client_pwd = "fabriSubscriber96!"

    # Create mqtt_client class to handle connection to MQTT broker
    client = mqtt_client(broker_address, broker_port, topic, client_id, client_pwd)

    # Create database_connection to handle connections to the db
    database = database_connection("data/sensor_data.db")

    handler = data_handler(client, database)

    database2 = database_connection("data/sensor_data.db")
    updater_thread(handler.update_variables)

    # loop_forever loop until the client is running
    while True:
        client.client.loop_forever()
