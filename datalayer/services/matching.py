from datalayer.backend.kafka_custom import KafkaReader, KafkaSender
from datalayer.backend.milvus import MilvusBackend
from datalayer.backend.es import ElasticsearchBackend

import pickle

from loguru import logger

import json
import time
import datetime

import numpy as np
from datalayer.utils.utility import convert_second_2_datetime, split_datetime, split_prefix_id 

# config 
kafka_opts={
    "bootstrap_servers": ["localhost:9092"],
    # "value_deserializer": lambda x: pickle.loads(x) ,
    "group_id": None,
    "session_timeout_ms": 300000,
    "request_timeout_ms": 300000,
    "enable_auto_commit": False,
    "auto_offset_reset": "earliest",
    "topic": "track"
}

milvus_opts = {
    "host":"localhost",
    "port": 19530,
    "collection": "test_milvus3"
}

es_opts={
    "host": "localhost",
    "port": 9200,
    "index_mapping": {
        "name": "test"
    }
}

test_mapping = {"name":"test"}
test_index = "name"

testReader = KafkaReader(**kafka_opts)

testCollection = MilvusBackend(**milvus_opts)

testElasticsearch = ElasticsearchBackend(**es_opts) 

global count
count = 1

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

    #commit offset
    reader.commit()

    results = []
    for record in records: #ConsumerRecord 
        results.append(json.loads(record.value))
    return results

# match feature embeddings received from jetson
def matching(records: list, indexElastic, dist_threshold: float, collection: MilvusBackend):
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

        insert log to elasticsearch: metadata
    """

    for record in records:
        vector = record['feature_embeddings']
        date, time = convert_second_2_datetime(record['timestamp']).split(" ")
        obj_image = record['object_image'] #base64 encode
        cam_id = record['camera_id']        
        # check if data exist 

        search_result = collection.search([vector])

        if search_result[0].distance > dist_threshold:

            #prefix: yyyyddmm
            # global_id = count
            # adding new person => count + 1
            metadata = {
                "globalId": count,
                "date": date,
                "startTime": time,  
                "objImage": obj_image,
                "cameraId": cam_id
            }

            global count
            count += 1

            data = [
                [vector],
                [metadata]
            ]
            
            # save data to collection
            collection.insert(data)

            # send es
            testElasticsearch.insert(index=indexElastic, body=metadata) 
        
        search_id = search_result[0].id

        meta = testCollection.query(expr=f'_id == {search_id}', output_fields=['metadata'])

        metadata = {
            "globalId": meta['globalId'],
            "date": date,
            "startTime": time,
            "objImage": obj_image,
            "cameraId": cam_id
        }

        data = [
            [vector],
            [metadata]
        ]

        #save data to collection
        collection.insert(data)

        #send es
        testElasticsearch.insert(index=indexElastic, body=metadata)

def run(**kwgars):
    distance_threshold = 0.5

    while True:
        try:
            # reset count people at 12.00 AM
            current = datetime.datetime.now().time()
            if current.hour == 0 and current.minute == 0 and current.second == 0:
                global count
                count = 1
            
            time.sleep(0.5)

            results = get_data(testReader)

            testReader.commit()
            if not len(results):
                continue

            matching(results, test_index, distance_threshold, testCollection)
        except Exception as e:
            logger.info(f'Error: {str(e)}')