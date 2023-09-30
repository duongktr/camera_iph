from kafka_custom import KafkaReader
from milvus import MilvusBackend
from kafka import KafkaConsumer
import pickle
import random

kafka_opts={
            "bootstrap_servers":["localhost:9092"],
            "group_id": None,
            # "value_deserializer": lambda x: pickle.loads(x) ,
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
            "topic": "testtrack3"
        }

reader = KafkaReader(**kafka_opts)
# print("connect")
while True:

    data = reader.poll(1000, max_records=500)
    # reader.commit()
    print(data)


# data = reader.poll(1000)
# x = []

# for d in data:
#     x.append(d.value)
# print(x[0]['camera_id'])
# print(x[0]['timestamp'])
# print(x[0]['object_bbox'])
# print(x[0]['confidence'])
# print(x[0]['feature_embeddings'])


# obj_img = x['object_image']
# print(f' object image: {obj_img}')
# reader.commit()

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

# import time
# testCollection = MilvusBackend(**milvus_opts)
# while True:
#     data = [
#         [random.random() for i in range(512)]
#     ]
#     t1 = time.time()
#     results = testCollection.search(data, search_params=search_params)
#     t2 = time.time()
#     print(results)
#     print(f"time execution: {t2-t1} seconds")
# list = [444227981101275269, 444227981101275113]
# query_params = {
#     'expr': f'_id in {list}',
#     'output_fields': ['metadata']
# }
# # res = testCollection.query(query_params=query_params)
# # res_id = results
# print(results[0].i)