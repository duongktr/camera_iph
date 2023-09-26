from kafka_custom import KafkaReader
from kafka import KafkaConsumer
import pickle

kafka_opts={
            "value_deserializer": lambda x: pickle.loads(x) ,
            "auto_offset_reset": "latest",
            "enable_auto_commit": False
        }
topic = "testtrack2"
while True:
    reader = KafkaReader(bootstrap_servers=["localhost:9092"], topic="testtrack2", group_id=None, auto_offset_reset="latest")

    # reader = KafkaConsumer(topic,**kafka_opts)
    data = reader.poll(1000)

    print(data)