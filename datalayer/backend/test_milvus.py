from kafka_custom import KafkaReader
from milvus import MilvusBackend
import sched

import random

import numpy as np
import pandas as pd

import json

from time import strftime, localtime, time

def convert_second_2_datetime(seconds):
    return strftime('%Y-%m-%d %H:%M:%S', localtime(seconds))

collection='test_milvus2'

schema = {
    "embeddings": 2048,
    "metadata": ""
}
image = b'iVBORw0KGgoAAAANSUhEUgAAA2gAAAG+CAYAAADm7icmAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAAFJ8SURBVHhe7d0J/A31/sfxjyVLZEuWLqFVIkJJezdRqZvbRpuSqP4q0kY'

index_params = {
    "index_type": "FLAT",
    "params": {},
    "metric_type": "IP"
}
testCollection = MilvusBackend(host="localhost", port='19530', collection=collection, schema=schema, index_params=index_params)
metadata = {
    "globalId": 0,
    "cameraId": '0',
    "timeStart": '0000-00-00 00:00:00',
    "objectImage": image.decode("utf-8")
    }
print(type(image))
image2 = image.decode("utf-8")
# print(image2)

data = [
[[random.random() for _ in range(2048)]],
[json.dumps(metadata)]
]
testCollection.insert(data)

# print(testCollection.is_empty())
# testCollection.drop_collection()

# testCollection = MilvusBackend(host="localhost", port='19530', collection=collection, schema=schema, index_params=index_params)

# print(testCollection.num_entities())


# vector = [[random.random() for _ in range(512)],
#           [random.random() for _ in range(512)]]
# res = testCollection.search(vector)
# # s = json.dumps(metadata)
# # s = json.loads(s)
# for r in res:
#     print(r.ids)
# for r in res:
# id = res[0].ids[0]
# query = {
#     "expr": f'_id == {id}',
#     "output_fields": ['metadata']
# }
# output = testCollection.query(query_params=query)
# x = json.loads(output[0]['metadata'])
# print(x['globalId'])
# print(type(output[0]))

# print(type(vector))




