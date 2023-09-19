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

from utils.utility import convert_second_2_datetime, split_datetime 

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

metadata: {
    "global_id",
    "time_start",
    ""
}
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

testElasticsearch = ElasticsearchBackend(host=es_host,
                                   port=es_port) 

# create log to logstash
# service 1 => log <= logstash => elktic <= kibana
# service 2 => log <= logstash => elktic <= kibana
# service 3 => log <= logstash => elktic <= kibana
logger = logging.getLogger("python-logstash-logger")
logger.setLevel(logging.INFO)
logger.addHandler(AsynchronousLogstashHandler(log_host, log_port))

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
    """

    records = reader.poll(timeout_ms=10000)
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
        # check if data exist 
        search_result = collection.search(vector)

        if search_result[0].distance > 10:
            time_start = convert_second_2_datetime(record['timestamp'])
            obj_image = record['object_image'] #base64
            cam_id = record['cam_id']
            global_id = split_datetime(time_start) + str(count)  #prefix: yyyyddmm, id example: 20230919 + count
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
        # search_params = {
        #     [],
        #     output = 'globalId'
        # }
        # user = testCollection.query(search_params=search_params)

# send result to logstash
def send_result(results, elk: ElasticsearchBackend):
    """
        elk: Elasticsearch
        sender: KafkaSender

        create log return to elasticsearch
    """
    for result in results:
        log = {
            ""
        }
    
# count people int/out
def count_people():
    """
        logic inside ???
    """
    return

def run():
    while True:

        # reset count people at 12.00 AM
        current = datetime.datetime.now().time()
        if current.hour == 0 and current.minute == 0 and current.second == 0:
            reset_count()
        
        time.sleep(0.5)

        results = get_data(testReader)

