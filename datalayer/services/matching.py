from backend.kafka_custom import KafkaReader
from backend.milvus import MilvusBackend
from backend.es import ElasticsearchBackend
from utils.utility import convert_second_2_datetime
from loguru import logger

import numpy as np
import datetime
import pickle
import json
import time
import sys
from uuid import uuid4

from sklearn.cluster import AgglomerativeClustering

test_index = "name"

#insert default data
image = b'iVBORw0KGgoAAAANSUhEUgAAA2gAAAG+CAYAAADm7icmAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAAFJ8SURBVHhe7d0J/A31/sfxjyVLZEuWLqFVIkJJezdRqZvbRpuSqP4q0kY'
meta = {
    "globalId": 0,
    "cameraId": '0',
    "timeStart": '0000-00-00 00:00:00',
    "objectImage": image.decode("utf-8")
    }

default_data = [
    [[0 for _ in range(2048)]],
    [json.dumps(meta)]
]
# match feature embeddings received from jetson
# search by batch, clustering ...
class Matching():
    def __init__(
            self,
            kafka_opts={
                "bootstrap_servers": ["localhost:9092"],
                "value_deserializer": lambda x: pickle.loads(x) ,
                "group_id": None,
                "session_timeout_ms": 300000,
                "request_timeout_ms": 300000,
                "enable_auto_commit": False,
                "auto_offset_reset": "earliest",
                "topic": "testtrack13"
            },
            es_opts={
            "host": "localhost",
            "port": 9200,
            # "index_prefix": "kotora"
            "index_mapping": {
                "name": "ktrtest"
                }
            },
            milvus_opts = {
                "host":"localhost",
                "port": 19530,
                "collection": "test_milvus1", #change schema,
                "schema":{
                    "embeddings": 2048,
                    "metadata": ""
                }
            },
            index_params = {
                "index_type": "FLAT",
                "params": {},
                "metric_type": "IP"
            },
            search_params = {
            "anns_field": "embeddings",
            "param": {"metric_type": "IP"}, #inner product ~ cosine
            "limit": 1,
            "output_fields": ['metadata']
            }):
        
        self.search_params = search_params
        self.index_params = index_params

        self.reader = KafkaReader(**kafka_opts)
        self.milvus_opt = milvus_opts
        self.milvusdb = MilvusBackend(**milvus_opts, index_params=index_params)

        #add default data
        if self.milvusdb.is_empty():
            self.milvusdb.insert(default_data)
        
        self.es = ElasticsearchBackend(**es_opts)

    def run(self, **kwargs):
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
                    "object_image", need decode utf 8 to string for saving
                    "time_start",
                    "cam_id"
                }
            }
        """
         #cosine threshold
         threshold =  kwargs.get('distance_threshold', 0.8)
    
         count = 1
         frame = 1
    
         while True:
            try:
                #check new day, reset data in 1 minute
                current = datetime.datetime.now().time()
                if current.hour == 0 and current.minute == 0:
                    #reset count
                    # count = 1

                    # reset database and insert default record to database:
                    self.milvusdb.drop_collection()

                    self.milvusdb = MilvusBackend(**self.milvus_opt)

                    #add default
                    self.milvusdb.insert(default_data)

                    break

                #sleep 
                time.sleep(0.5)
                
                records = []

                #read message from kafka
                consumerRecords = self.reader.poll(1000)
                
                print(len(consumerRecords))
                if not len(consumerRecords):
                    continue
                
                for record in consumerRecords:
                    records.append(record.value)

                old_vectors = [record['feature_embeddings'] for record in records]
                
                new_vectors = []
                new_records = []

                labels = AgglomerativeClustering(
                    distance_threshold= 1-threshold,
                    metric='cosine',
                    n_clusters=None,
                    linkage='complete',
                ).fit_predict(np.array(old_vectors))

                u_labels = np.unique(labels)

                for label in u_labels:
                    #index cluster
                    in_cluster = np.where(labels==label)[0]

                    #new vectors
                    new_vectors.append([old_vectors[index] for index in in_cluster])
                    new_records.append([records[index] for index in in_cluster])
                
                logger.info('pass clustering')
                
                # search with each cluster
                for vectors, records in zip(new_vectors, new_records):
                    # search first vector
                    result = self.milvusdb.search([vectors[0]], search_params=self.search_params)
                    
                    distance = result[0].distances[0]
                    
                    #add new person if small than threshold
                    if distance < threshold:
                        logger.info("pass first condition")
                        metadatas = [
                            {
                                "globalId": count,
                                "cameraId": record['camera_id'],
                                "timeStart": convert_second_2_datetime(record['timestamp']),
                                "objectImage": record['object_image'].decode("utf-8")
                            } for record in records
                        ]

                        # f = open("log.txt", mode='a')
                        
                        # f.write(f'{count}, {record["object_image"].decode("utf-8")}' for record in records)

                        data = [
                            vectors,
                            [json.dumps(metadata) for metadata in metadatas]
                        ]
                         
                        logger.info("insert new id " + str(count) )
                        count += 1

                        #insert new user
                        self.milvusdb.insert(data)
                        #send es
                        for metadata in metadatas:
                            self.es.insert(index=test_index, body=metadata)
                    elif distance > threshold:
                        logger.info("pass second condition")                        
                        hit = result[0][0].entity.get('metadata')
                        hit = json.loads(hit)

                        id = hit['globalId']
                        metadatas = [
                            {
                                "globalId": id,
                                "cameraId": record['camera_id'],
                                "timeStart": convert_second_2_datetime(record['timestamp']),
                                "objectImage": record['object_image'].decode("utf-8")
                            } for record in records
                        ]

                        data = [
                            vectors,
                            [json.dumps(metadata) for metadata in metadatas]
                        ]

                        self.milvusdb.insert(data)
                        #send es
                        for metadata in metadatas:
                            self.es.insert(index=test_index, body=metadata) 
            except Exception as e:
                logger.error("Error: " + str(e))

            
