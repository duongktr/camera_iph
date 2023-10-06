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

collection='test_milvus1'

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
    [[0 for _ in range(2048)]],
    [json.dumps(metadata)]
]
testCollection.insert(data) 
# search_params = {
#             "anns_field": "embeddings",
#             "param": {"metric_type": "IP"}, #inner product ~ cosine, cosine = 1 - IP 
#             "limit": 1,
#             "output_fields": ['metadata']
#             }
# res = testCollection.search(data=data, search_params=search_params)
# hit = res[0][0]

# x = hit.entity.get('metadata')
# x = json.loads(x)
# print(x['objectImage'].encode("utf-8"))



