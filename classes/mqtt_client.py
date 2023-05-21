import paho.mqtt.client as mqtt


class mqtt_client(mqtt.Client):
    def __init__(
        self,
        broker_address,
        broker_port,
        topic,
        client_id,
        client_pwd,
        client_name,
    ):
        """
        Initializes the mqtt_client class.
        Gets the parameters to be able to stablish SSL connection to a broker.

        @param broker_address - The address of the MQTT broker
        @param broker_port - The port of the MQTT broker
        @param topic - The topic to send messages to
        @param client_id - The client id to use for authenticating
        @param client_pwd - The client password to use for authenticating
        """
        # Connection parameters
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.topic = topic
        self.client_id = client_id
        self.client_pwd = client_pwd
        # Creating new mqtt.Client instance
        self.client = mqtt.Client(client_name)
        # Setting SSL connection
        self.client.tls_set()
        self.client.username_pw_set(username=client_id, password=client_pwd)

    def set_on_message(self, func):
        """
        Modify the on_message callback function with the specified function.

        @param func - new callback function
        """
        self.client.on_message = func

    def set_on_connect(self, func):
        """
        Modify the on_connect callback function with the specified function.

        @param func - new callback function
        """
        self.client.on_connect = func

    def connect(self):
        """
        Connect to the broker.
        """
        self.client.connect(self.broker_address, self.broker_port)
        print("Connecting...")
