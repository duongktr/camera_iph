from backend.kafka_custom import KafkaReader, KafkaSender
from backend.milvus import MilvusBackend
from backend.es import ElasticsearchBackend

from logstash_async.handler import AsynchronousLogstashHandler
import logging
import sys

import base64
import json
import time
import datetime

import numpy as np
from utils.utility import convert_second_2_datetime, split_datetime, split_prefix_id 

# config 
kafka_servers = "localhost:9092"
kafka_topic = "testDuong"

milvus_host = "localhost"
milvus_port = 19530
milvus_collection = "test_milvus3"

es_host = "localhost"
es_port = 9200

log_host = "localhost"
log_port = 5044

schema = {
    "feature_embeddings": 512,
    "metadata":""
}

testSender = KafkaSender(bootstrap_servers=kafka_servers,
                         topic=kafka_topic)

testReader = KafkaReader(bootstrap_servers=kafka_servers, 
                         topic=kafka_topic)

testCollection = MilvusBackend(host=milvus_host,
                               port=milvus_port,
                               collection=milvus_collection)

test_mapping = {"name":"test"}
test_index = "name"
testElasticsearch = ElasticsearchBackend(host=es_host,
                                   port=es_port,
                                   index_mapping=test_mapping) 

# create log to logstash
# service 1 => log <= logstash => elktic <= kibana
# service 2 => log <= logstash => elktic <= kibana
# service 3 => log <= logstash => elktic <= kibana
# logger = logging.getLogger("python-logstash-logger")
# logger.setLevel(logging.INFO)
# logger.addHandler(AsynchronousLogstashHandler(log_host, log_port))

# 

global count
count = 1

def reset_count():
    global count
    # Reset the count to 0
    count = 0

# get data from jetson
def get_data(reader: KafkaReader):
    """
        reader: KafkaReader object
        
        this function read data receive from jetson and commit offset at that time to avoid re-processing the last message read

        return a list result received from kafka

        result: {
            'camera_id',
            'timestamp'
            'object_bbox'
            'confidence'
            'object_id'
            'feature_embeddings'
            'object_image'
        }
    """

    records = reader.poll(timeout_ms=1000)
    reader.commit()
    results = []
    for record in records:
        record = json.loads(record.value)
        results.append(record)
    return results

# match feature embeddings received from jetson
def insert_data(records, collection: MilvusBackend):
    """
        records: a list include metadata
        
        record:     'camera_id',
                    'timestamp',
                    'object_bbox',
                    'confidence',
                    'object_id',
                    'feature_embeddings',
                    'object_image'

        insert new document to db
        document: {
            "embeddings":,
            "metadata": {
                "global_id",
                "object_image",
                "time_start",
                "cam_id"
            }
        }
    """
    for record in records:
        vector = record['feature_embeddings']
        time_start = convert_second_2_datetime(record['timestamp'])
        obj_image = record['object_image'] #base64 encode
        cam_id = record['cam_id']        
        # check if data exist 

        search_result = collection.search(vector)

        if search_result[0].distance > 5: #condition if match same person
            global_id = split_datetime(time_start) + str(count)  #prefix: yyyyddmm, id example: 20230919 + count
            #adding new person => count + 1
            global count
            count += 1

            metadata = {
                "globalId": global_id,
                "startTime": time_start,  
                "objImage": obj_image,
                "cameraId": cam_id
            }

            data = [
                vector,
                metadata
            ]
            collection.insert(data)
        
        search_id = search_result[0].id

        user_id = testCollection.query(expr=f'_id == {search_id}', output_fields=['globalId'])

        metadata = {
            "globalId": user_id,
            "startTime": time_start,
            "objImage": obj_image,
            "cameraId": cam_id
        }

        data = [
            vector,
            metadata
        ]
        collection.insert(data) 

# send result to elasticsearch
def send_result(result, index, elk: ElasticsearchBackend):
    """
        elk: Elasticsearch

        create result return to elasticsearch
        document format:
            {
            "globalId": string,
            "startTime": string,
            "camId": int,
            "image": string,
            }
    """ 
    elk.insert(index=index, body=result)

# count people 
def count_people(list_user_id):
    """
        logic inside ???
    """
    ids = []
    for user_id in list_user_id:
        user_id = split_prefix_id(user_id)
        ids.append(user_id)
    
    unique_ids = set(ids)
    return len(unique_ids)

def run():
    while True:

        # reset count people at 12.00 AM
        current = datetime.datetime.now().time()
        if current.hour == 0 and current.minute == 0 and current.second == 0:
            reset_count()
        
        time.sleep(1)

        results = get_data(testReader)

        insert_data(results, testCollection)




