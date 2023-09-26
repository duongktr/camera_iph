# from backend.es import ElasticsearchBackend
# from elasticsearch import Elasticsearch

# from utils.utility import convert_second_2_datetime
# import time

# # hosts = {
# #     "host": "localhost",
# #     "port": 9200
# # }

# host = "localhost"
# port = 9200

# # index name
# index_mapping = {
#     "name": "test"
# }

# index = "name"

# elk = ElasticsearchBackend(host=host, port=port, index_mapping=index_mapping)

# document = {
#     "globalId": str(2023092025),
#     "startTime": convert_second_2_datetime(time.time()),
#     "image": "abcdefghijk",
#     "camId": 5
# }

# # # print(type(document))
# # elk.insert(index=index, body=document)

# # res = elk._search(index=index)

# # print(res['hits']['hits'])

# string = b'asfasfas'
# print(type(string))

# import datetime

