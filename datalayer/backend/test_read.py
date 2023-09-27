from kafka_custom import KafkaReader
from milvus import MilvusBackend
from kafka import KafkaConsumer
import pickle
import random

kafka_opts={
            "bootstrap_servers":["localhost:9092"],
            "group_id": None,
            "value_deserializer": lambda x: pickle.loads(x) ,
            "auto_offset_reset": "latest",
            "enable_auto_commit": False,
            "topic": "testtrack2"
        }
topic = "testtrack2"
reader = KafkaReader(**kafka_opts)
# while True:
#     reader = KafkaReader(bootstrap_servers=["localhost:9092"], topic="testtrack2", group_id=None, auto_offset_reset="latest")

#     # reader = KafkaConsumer(topic,**kafka_opts)
#     data = reader.poll(1000)

#     print(data)

milvus_opts = {
    "host":"localhost",
    "port": 19530,
    "collection": "test_milvus3"
}

search_params = {
    "anns_field": "embeddings",
    "param": {"metric_type": "L2"},
    "limit": 1,
}

import time
testCollection = MilvusBackend(**milvus_opts)
while True:
    data = [
        [random.random() for i in range(512)]
    ]
    t1 = time.time()
    results = testCollection.search(data, search_params=search_params)
    t2 = time.time()
    print(results)
    print(f"time execution: {t2-t1} seconds")
list = [444227981101275269, 444227981101275113]
query_params = {
    'expr': f'_id in {list}',
    'output_fields': ['metadata']
}
# res = testCollection.query(query_params=query_params)
# res_id = results
print(results[0].i)