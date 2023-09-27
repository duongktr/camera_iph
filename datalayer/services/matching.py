from backend.kafka_custom import KafkaReader, KafkaSender
from backend.milvus import MilvusBackend
from backend.es import ElasticsearchBackend

import pickle

from loguru import logger

import json
import time
import datetime

import numpy as np
from utils.utility import convert_second_2_datetime, split_datetime, split_prefix_id 

# config 
kafka_opts={
    "bootstrap_servers": ["localhost:9092"],
    "value_deserializer": lambda x: pickle.loads(x) ,
    "group_id": None,
    "session_timeout_ms": 300000,
    "request_timeout_ms": 300000,
    "enable_auto_commit": False,
    "auto_offset_reset": "latest",
    "topic": "track"
}

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

    records = reader.poll(timeout_ms=1000, max_records=500)

    #commit offset
    # reader.commit()

    results = []
    for record in records: #ConsumerRecord 
        results.append(record.value)
    return results

# match feature embeddings received from jetson
# search by batch, clustering ...
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
    # vectors = records['feature_embeddings']
    for record in records:
        vector = record['feature_embeddings']
        date, time_start = convert_second_2_datetime(record['timestamp']).split(" ")
        obj_image = record['object_image'] #base64 encode
        cam_id = record['camera_id']        
        # check if data exist 

        # t1 = time.time()
        search_result = collection.search([vector])

        if search_result[0].distance > dist_threshold:

            #prefix: yyyyddmm
            # global_id = count
            # adding new person => count + 1
            global count

            metadata = {
                "globalId": count,
                "date": date,
                "startTime": time_start,  
                "objImage": obj_image,
                "cameraId": cam_id
            }

            count += 1

            data = [
                [vector],
                [metadata]
            ]
            
            # save data to collection
            collection.insert(data)

            # send es
            testElasticsearch.insert(index=indexElastic, body=metadata) 
        
        logger.info("Updating person")

        search_id = search_result[0].id

        meta = testCollection.query(expr=f'_id == {search_id}', output_fields=['metadata'])

        metadata = {
            "globalId": meta[0]['metadata']['globalId'],
            "date": date,
            "startTime": time_start,
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

def matching(records, dist_threshold, collection: MilvusBackend):
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
    vectors = [record['feature_embeddings'] for record in records]

    results = collection.search(vectors, search_params=search_params)

    ids, distances = [result.ids for result in results], [result.distances for result in results]

    for record, id, distance in zip(records, ids, distances):
        neighbor = [i for i, dist in zip(id,distance) if dist <= dist_threshold]        

def run(**kwgars):
    distance_threshold = 0.4

    while True:
        try:
            # reset count people at 12.00 AM
            current = datetime.datetime.now().time()
            if current.hour == 0 and current.minute == 0 and current.second == 0:
                global count
                count = 1
            
            time.sleep(0.5)

            results = get_data(testReader)

            print(len(results))

            # testReader.commit()
            if not len(results):
                continue

            matching(results, test_index, distance_threshold, testCollection)
        except Exception as e:
            logger.info(f'Error: {str(e)}')