import sqlite3
import paho.mqtt.client as mqtt
import numpy as np
import json
import time 

broker_address="localhost" 
broker_address ='300de1d0fce741319264fb560810a851.s2.eu.hivemq.cloud'
broker_port = 8883
topic = 'FabriTopics/sensor_data_test_14_05'
client_id = 'fabriSubscriber'
client_pwd = 'fabriSubscriber96!'
print("creating new instance")
client = mqtt.Client("suscriber1_test") #create new instance
print("connecting to broker")
client.tls_set()  # <--- even without arguments
client.username_pw_set(username=client_id, password=client_pwd)
print("Connecting...")
client.connect(broker_address,broker_port) #connect to broker
# Connection to DB
conn = sqlite3.connect(':memory:')


def filter_outliers(datos): 
    # Calcular el valor medio y la desviación estándar de la columna de valores 
    mean = 50
    varianza = 100**(2)/12
    std = varianza ** 0.5 
 
    # Identificar los valores que están fuera de 3 desviaciones estándar del valor medio 
    low = mean - 3*std 
    high = mean + 3*std 
 
    # Filtrar los valores 
    valores_filtrados = [v if low <= v <= high else np.nan for v in datos] 
 
    # Eliminar los valores faltantes (NaN) 
    datos_filtrados = [] 
    for i in range(len(datos)): 
        if not np.isnan(valores_filtrados[i]): 
            datos_filtrados.append(datos[i]) 
 
    # Retornar los datos filtrados 
    return datos_filtrados 

def create_sensor_data(conn, sensor_data):
    """
    Create a new sensor_data
    :param conn:
    :param task:
    :return:
    """

    sql = ''' INSERT INTO tasks(name,priority,status_id,project_id,begin_date,end_date)
              VALUES(?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, sensor_data)
    conn.commit()

    return cur.lastrowid

def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message ", json.loads(message.payload.decode())['sensor_id'])
    print("message ",json.loads(message.payload.decode())['sensor_type'])
    print("message ",json.loads(message.payload.decode())['value'])
    print("message ",json.loads(message.payload.decode())['sensor_type'])
    print("userdata ",userdata)
    timestamp = time.time_ns()
    conn.


def on_connect(client,userdata,flags,rc):
    print('Connected')
    client.subscribe(topic)

while True:
    client.on_message=on_message
    client.on_connect=on_connect
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