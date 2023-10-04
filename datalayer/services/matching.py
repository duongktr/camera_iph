from backend.kafka_custom import KafkaReader
from backend.milvus import MilvusBackend
from backend.es import ElasticsearchBackend
from utils.utility import convert_second_2_datetime, normalize_vector

from loguru import logger

import datetime
import pickle
import json
import time

from sklearn.cluster import AgglomerativeClustering

test_mapping = {"name":"kotoratest"}
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
    [[1 for _ in range(2048)]],
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
                "topic": "testtrack6"
            },
            es_opts={
            "host": "localhost",
            "port": 9200,
            # "index_prefix": "kotora"
            "index_mapping": {
                "name": "ktrtest1"
                }
            },
            milvus_opts = {
                "host":"192.168.1.236",
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
            "param": {"metric_type": "IP"}, #inner product with normalized data ~ cosine 
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
        
    # def run(self, **kwargs):
    #     """
    #         records: a list include metadata
    #         record:     'camera_id',
    #                     'timestamp',
    #                     'object_bbox',
    #                     'confidence',
    #                     'object_id',
    #                     'feature_embeddings',
    #                     'object_image'

    #         insert new document to db
    #         document: {
    #             "embeddings":,
    #             "metadata": {
    #                 "global_id",
    #                 "object_image", need decode utf 8 to string for saving
    #                 "time_start",
    #                 "cam_id"
    #             }
    #         }
    #     """
    #     threshold = kwargs.get('distance_threshold', 0.3)
        
    #     count = 1
        
    #     while True:
    #         #check new day
    #         current = datetime.datetime.now().time()
    #         if current.hour == 0 and current.minute == 0 and current.second == 0:
    #             #reset count
    #             count = 1

    #             # reset database and insert first record to database
    #             # self.milvusdb.drop_collection()

    #         #sleep 
    #         time.sleep(0.5)
            
    #         records = []

    #         #read message from kafka
    #         consumerRecords = self.reader.poll(1000)
            
    #         print(len(consumerRecords))

    #         if not len(consumerRecords):
    #             continue
            
    #         for record in consumerRecords:
    #             records.append(record.value)

    #         vectors = [record['feature_embeddings'] for record in records]
            
    #         # !!search tung vector
    #         searchResults = self.milvusdb.search(vectors, search_params=self.search_params)

    #         if not len(searchResults):
    #             continue

    #         ids, distances = [result.ids[0] for result in searchResults], [result.distances[0] for result in searchResults]

    #         for record, id, distance, embedding in zip(records, ids, distances, vectors):
    #             # new ID
    #             if distance > threshold:
    #                 # global ID
    #                 print('pass first condition')
    #                 metadata = {
    #                     "globalId": count,
    #                     "cameraId": record['camera_id'],                    
    #                     "timeStart": convert_second_2_datetime(record['timestamp']),
    #                     "objectImage": record['object_image'].decode("utf-8") #convert byte encode to string
    #                 }

    #                 data = [
    #                     [embedding],
    #                     [json.dumps(metadata)] #convert json object to string 
    #                 ]
    #                 count += 1
    #                 print(f'Insert new user id {count}')
    #                 #insert new user
    #                 self.milvusdb.insert(data)
    #                 #send es
    #                 self.es.insert(index=test_index, body= metadata)
    #             else:
    #                 print('pass second condition')

    #                 #already exist user
    #                 query_params = {
    #                     "expr": f'_id == {id}',
    #                     "output_fields": ['metadata']
    #                 }
                    
    #                 query_results = self.milvusdb.query(query_params)

    #                 result = json.loads(query_results[0]['metadata'])

    #                 metadata = {
    #                     "globalId": result['globalId'],
    #                     "cameraId": record['camera_id'],
    #                     "timeStart": convert_second_2_datetime(record['timestamp']),
    #                     "objectImage": record['object_image'].decode("utf-8")
    #                 }

    #                 data = [
    #                     [embedding],
    #                     [json.dumps(metadata)]
    #                 ]
    #                 #insert old user
    #                 self.milvusdb.insert(data)
    #                 #send es
    #                 self.es.insert(index=test_index, body=metadata)


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
         
         threshold =  kwargs.get('distance_threshold', 0.3)
        
         count = 1
        
         while True:
            #check new day
            current = datetime.datetime.now().time()
            if current.hour == 0 and current.minute == 0 and current.second == 0:
                #reset count
                count = 1

                # reset database and insert default record to database:
                self.milvusdb.drop_collection()

                self.milvusdb = MilvusBackend(**self.milvus_opt)

                #add default
                self.milvusdb.insert(default_data)

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

            vectors = [normalize_vector(record['feature_embeddings']) for record in records]

            # for vector, record in zip(vectors, records):
            #     searchresult = self.milvusdb.search(vector) 

            labels = AgglomerativeClustering(
                distance_threshold=threshold,
                metric='cosine',
                n_clusters=None,
                linkage='complete',
            ).fit_predict(vectors)

            
