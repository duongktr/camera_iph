from backend.kafka_custom import KafkaReader
from backend.milvus import MilvusBackend
from backend.es import ElasticsearchBackend
from utils.utility import convert_second_2_datetime

from loguru import logger

import datetime
import pickle
import json
import sys
import random


test_mapping = {"name":"kotoratest"}
test_index = "name"

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
                "name": "ktrtest"
                }
            },
            milvus_opts = {
                "host":"localhost",
                "port": 19530,
                "collection": "test_milvus9", #change schema,
                "schema":{
                    "embeddings": 2048,
                    "metadata": ""
                }
            },
            search_params = {
            "anns_field": "embeddings",
            "param": {"metric_type": "L2"},
            "limit": 1,
            }):
        
        self.search_params = search_params
        
        self.reader = KafkaReader(**kafka_opts)
        self.milvusdb = MilvusBackend(**milvus_opts)
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
        threshold = kwargs.get('distance_threshold', 0.3)
        
        count = kwargs.get('count', 1)
        
        while True:
            #check new day
            current = datetime.datetime.now().time()
            if current.hour == 0 and current.minute == 0 and current.second == 0:
                #reset count
                count = 1

                #reset database
                # self.milvusdb.drop_collection()

            records = []

            consumerRecords = self.reader.poll(1000)
            
            print(len(consumerRecords))

            if not len(consumerRecords):
                continue
            
            for record in consumerRecords:
                records.append(record.value)

            vectors = [record['feature_embeddings'] for record in records]
            
            searchResults = self.milvusdb.search(vectors, search_params=self.search_params)

            if not len(searchResults):
                continue

            ids, distances = [result.ids[0] for result in searchResults], [result.distances[0] for result in searchResults]

            for record, id, distance, embedding in zip(records, ids, distances, vectors):
                # new ID
                if distance > threshold:
                    # global ID
                    print('pass first condition')
                    metadata = {
                        "globalId": count,
                        "cameraId": record['camera_id'],                    
                        "timeStart": convert_second_2_datetime(record['timestamp']),
                        "objectImage": record['object_image'].decode("utf-8") #convert byte encode to string
                    }

                    data = [
                        [embedding],
                        [json.dumps(metadata)] #convert json object to string 
                    ]
                    count += 1
                    print(f'Insert new user id {count}')
                    #insert new user
                    self.milvusdb.insert(data)
                    #send es
                    self.es.insert(index=test_index, body= metadata)
                else:
                    print('pass second condition')

                    #already exist user
                    query_params = {
                        "expr": f'_id == {id}',
                        "output_fields": ['metadata']
                    }
                    
                    query_results = self.milvusdb.query(query_params)

                    result = json.loads(query_results[0]['metadata'])

                    metadata = {
                        "globalId": result['globalId'],
                        "cameraId": record['camera_id'],
                        "timeStart": convert_second_2_datetime(record['timestamp']),
                        "objectImage": record['object_image'].decode("utf-8")
                    }

                    data = [
                        [embedding],
                        [json.dumps(metadata)]
                    ]
                    #insert old user
                    self.milvusdb.insert(data)
                    #send es
                    self.es.insert(index=test_index, body=metadata)


# def get_data(reader: KafkaReader):
#     """
#         reader: KafkaReader object
        
#         this function read data receive from jetson and commit offset at that time to avoid re-processing the last message read

#         return a list result received from kafka

#         result: {
#             'camera_id',
#             'timestamp'
#             'object_bbox'
#             'confidence'
#             'object_id'
#             'feature_embeddings'
#             'object_image'
#         }
#     """

#     records = reader.poll(timeout_ms=1000, max_records=500)

#     #commit offset
#     # reader.commit()

#     results = []
#     for record in records: #ConsumerRecord 
#         results.append(record.value)
#     return results

# def matching(records: list, indexElastic, dist_threshold: float, collection: MilvusBackend):
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
#                 "object_image",
#                 "time_start",
#                 "cam_id"
#             }
#         }
#         insert log to elasticsearch: metadata
#     """
#     # vectors = records['feature_embeddings']
#     for record in records:
#         vector = record['feature_embeddings']
#         date, time_start = convert_second_2_datetime(record['timestamp']).split(" ")
#         obj_image = record['object_image'] #base64 encode
#         cam_id = record['camera_id']        
#         # check if data exist 

#         # t1 = time.time()
#         search_result = collection.search([vector])

#         if search_result[0].distance > dist_threshold:

#             #prefix: yyyyddmm
#             # global_id = count
#             # adding new person => count + 1
#             global count

#             metadata = {
#                 "globalId": count,
#                 "date": date,
#                 "startTime": time_start,  
#                 "objImage": obj_image,
#                 "cameraId": cam_id
#             }

#             count += 1

#             data = [
#                 [vector],
#                 [metadata]
#             ]
            
#             # save data to collection
#             collection.insert(data)

#             # send es
#             testElasticsearch.insert(index=indexElastic, body=metadata) 
        
#         logger.info("Updating person")

#         search_id = search_result[0].id

#         meta = testCollection.query(expr=f'_id == {search_id}', output_fields=['metadata'])

#         metadata = {
#             "globalId": meta[0]['metadata']['globalId'],
#             "date": date,
#             "startTime": time_start,
#             "objImage": obj_image,
#             "cameraId": cam_id
#         }

#         data = [
#             [vector],
#             [metadata]
#         ]

#         #save data to collection
#         collection.insert(data)

#         #send es
#         testElasticsearch.insert(index=indexElastic, body=metadata)

