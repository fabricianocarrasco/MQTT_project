import sqlite3
import paho.mqtt.client as mqtt
import numpy as np
import json
import time
from database import create_connection, create_table
import threading


broker_address = "localhost"
broker_address = "300de1d0fce741319264fb560810a851.s2.eu.hivemq.cloud"
broker_port = 8883
topic = "FabriTopics/sensor_data_test_14_05"
client_id = "fabriSubscriber"
client_pwd = "fabriSubscriber96!"
print("creating new instance")
client = mqtt.Client("suscriber1_test")  # create new instance
print("connecting to broker")
client.tls_set()  # <--- even without arguments
client.username_pw_set(username=client_id, password=client_pwd)
print("Connecting...")
client.connect(broker_address, broker_port)  # connect to broker
# Connection to DB
conn = sqlite3.connect("data/sensor_data.db")


mean_g = None
std_g = None


def update_variables():
    global mean_g
    global std_g

    time.sleep(1)
    conn = sqlite3.connect("data/sensor_data.db")
    # conn = create_connection()
    sql = """SELECT AVG(value) as mean,SUM((value-(SELECT AVG(value) FROM iot))*
           (value-(SELECT AVG(value) FROM iot)) ) / (COUNT(value)-1) AS var 
           FROM iot ORDER BY timestamp DESC LIMIT 1000"""
    cur = conn.cursor()
    data = cur.execute(sql).fetchall()[0]
    mean_g = data[0]
    std_g = data[1]**0.5
    print(mean_g,std_g)
    # mean_g = mean

    conn.commit()
    conn.close()


def detect_outlier(sensor_data):
    global mean_g
    global std_g

    if not (mean_g and std_g):
        return False
    
    # Identificar los valores que están fuera de 3 desviaciones estándar del valor medio
    low = mean_g - 3 * std_g
    high = mean_g + 3 * std_g

    # Filtrar los valores
    if low <= sensor_data <= high:
        return True
    else: 
        return False

def create_sensor_data(sensor_data):
    global mean_g
    global std_g
    """
    Create a new sensor_data
    :param conn:
    :param sensor_data:
    :return:
    """
    if not (mean_g and std_g):
        sql = """ INSERT INTO iot(id,sensor_id,sensor_type,value,timestamp)
              VALUES(?,?,?,?,?) """
        cur = conn.cursor()
        cur.execute(sql, sensor_data)
        conn.commit() 
        return "First second"
    
    if not detect_outlier(sensor_data[3]):
        return "Outlier"

    sql = """ INSERT INTO iot(id,sensor_id,sensor_type,value,timestamp)
              VALUES(?,?,?,?,?) """
    cur = conn.cursor()
    cur.execute(sql, sensor_data)
    conn.commit()

    return cur.lastrowid


def on_message(client, userdata, message):
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
    row_id = create_sensor_data([id, sensor_id, sensor_type, value, timestamp])
    print(row_id)
    # conn.


def on_connect(client, userdata, flags, rc):
    print("Connected")
    client.subscribe(topic)


dict_thousand = {}
client.on_message = on_message
client.on_connect = on_connect
# conn = create_connection()
sql_create_iot_table = """ CREATE TABLE IF NOT EXISTS iot(
                                    id text PRIMARY KEY,
                                    sensor_id text,
                                    sensor_type text,
                                    value integer,
                                    timestamp integer
                                ); """
create_table(conn, sql_create_iot_table)


class myClassA(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = True
        self.start()

    def run(self):
        while True:
            update_variables()


myClassA()

while True:
    client.loop_forever()


# if __name__ == "__main__":
#     import paho.mqtt.client as mqtt
#     import json
#     import time
#     import random
#     # Configuración del broker MQTT
#     broker_address = "localhost"
#     broker_port = 1883
#     topic = "sensor_data"
#     # Función para generar los datos aleatorios
#     def generate_data():
#         sensor_id = "sensor_" + str(random.randint(1, 10))
#         sensor_type = "type_" + str(random.randint(1, 3))
#         value = random.uniform(0, 100)
#         data = {"sensor_id": sensor_id, "sensor_type": sensor_type, "value":
#         value}
#         return json.dumps(data)
#     # Función para publicar los datos en el broker MQTT
#     def publish_data(client):
#         while True:
#             data = generate_data()
#             client.publish(topic, data)
#             time.sleep(0.1)
#     # Conexión al broker MQTT
#     client = mqtt.Client()
#     client.connect(broker_address, broker_port)
#     # Inicio de la publicación de datos
#     publish_data(client)
