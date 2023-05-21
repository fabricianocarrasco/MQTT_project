from classes.database import database
from classes.mqtt_client import mqtt_client
from classes.updater_thread import updater_thread
from classes.data_handler import data_handler


# Main function to handle MQTT broker connection subscription
if __name__ == "__main__":
    # HiveMQ broker connection information
    broker_address = "300de1d0fce741319264fb560810a851.s2.eu.hivemq.cloud"
    broker_port = 8883
    topic = "FabriTopics/sensor_data_ikerlan"
    client_id = "fabriSubscriber"
    client_pwd = "fabriSubscriber96!"
    client_name = "subscriberTest"
    # Create mqtt_client class to handle connection to MQTT broker
    client = mqtt_client(
        broker_address, broker_port, topic, client_id, client_pwd, client_name
    )

    # SQLite database route and sentence to create the table
    db_route = "data/sensor_data.db"
    sql_create_table = """ CREATE TABLE IF NOT EXISTS iot(
                                        id text PRIMARY KEY,
                                        sensor_id text,
                                        sensor_type text,
                                        value integer,
                                        timestamp integer
                                    );
                            """
    # Create database_table and table to handle connections to the db
    db = database(db_route, sql_create_table)

    # Data handler to manage the data from the HiveMQ broker
    # to the SQLite database
    handler = data_handler(client, db)

    # Statistics thread updater parameters
    update_interval = 10
    num_registers_statistics = 100
    # Create a thread to update the mean and std variables parameters.
    updater_thread(
        handler.update_variables,
        update_interval,
        num_registers_statistics,
        db_route,
    )

    # start receiveng messages and loop forever
    while True:
        client.client.loop_forever()
