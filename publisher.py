import time
import json
import random


def generate_data():
    """
    Generates data for test.
    It is used to generate a test data that can be sent to HiveMQ broker.

    @return JSON string of sensor data to be sent to HiveMQ server
    """
    sensor_id = "sensor_" + str(random.randint(1, 10))
    sensor_type = "type_" + str(random.randint(1, 3))
    value = random.uniform(0, 100)
    data = {
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "value": value,
    }
    return json.dumps(data)


def publish_data(client, topic):
    """
    Publish data to HiveMQ broker.
    The loop generates data and publishes it to HiveMQ every 0.1 seconds.

    @param client - Client to publish to HiveMQ
    @param topic - Topic to publish data to
    """
    # Publishes data to the topic.
    while True:
        data = generate_data()
        client.publish(topic, data)
        time.sleep(0.1)
        print("Messagge sent")


# Create a connection to the broker with the credentials provided
# and publish data
if __name__ == "__main__":
    from classes.mqtt_client import mqtt_client

    # HiveMQ broker connection information
    broker_address = "300de1d0fce741319264fb560810a851.s2.eu.hivemq.cloud"
    broker_port = 8883  # Port for the SSL connection
    topic = "FabriTopics/sensor_data_ikerlan"
    client_id = "fabriPublisher"
    client_pwd = "fabriPublisher96!"
    client_name = "publisherTest"

    # HiveMQ client configuration
    client = mqtt_client(
        broker_address, broker_port, topic, client_id, client_pwd, client_name
    )
    # Establish the connection
    client.connect()
    # Data publishing
    publish_data(client.client, client.topic)
