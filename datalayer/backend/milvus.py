from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)
import pandas as pd


DEFAULT_INDEX = {
    "index_type": "FLAT",
    "params": {},
    "metric_type": "L2"
}

DEFAULT_SEARCH = {
    "anns_field": "embeddings",
    "param": {"metric_type": "L2"},
    "limit": 5,
}

DEFAULT_SCHEMA = {
    "embeddings": 512,
    "metadata": ""
}

class MilvusBackend:
    def __init__(
        self,
        host=None,
        port=None,
        collection=None,
        schema=None,
        index_params=None,
        **kwargs
    ):
        # Open connection
        connections.connect(host=host, port=port)

        # Schema
        schema = schema or DEFAULT_SCHEMA
        index_params = index_params or DEFAULT_INDEX

        # Load collection
        self.__create_collection(collection, schema, index_params)

    def __create_collection(self, collection, schema, index_params):
        if utility.has_collection(collection):
            self.collection = Collection(collection)
        else:
            fields, index_field = self.__create_schema(**schema)
            init_schema = CollectionSchema(fields)
            self.collection = Collection(collection, init_schema, consistency_level="Strong")

            if index_field is not None:
                self.collection.release()
                self.collection.create_index(index_field, index_params)
                self.collection.load()

    def __create_schema(self, **kwargs):
        index_field = None
        fields = [
            FieldSchema(name="_id", dtype=DataType.INT64, is_primary=True, auto_id=True)
        ]
        for key, value in kwargs.items():
            if isinstance(value, int):
                fields.append(
                    FieldSchema(name=key, dtype=DataType.FLOAT_VECTOR, dim=value)
                )
                index_field = key
            else:
                fields.append(
                    FieldSchema(name=key, dtype=DataType.VARCHAR, max_length=10000)
                )
        return fields, index_field

    def insert(self, data):
        # data=pd.DataFrame(data)
        self.collection.insert(data)
        self.collection.flush()

    def _insert(self, data, **kwargs):
        pass

    def delete(self, expr):
        self.collection.delete(expr=expr)
        self.collection.flush()

    def _delete(self, expr, **kwargs):
        pass

    def search(self, data, search_params=None):
        search_params = search_params or DEFAULT_SEARCH 
        search_params['data'] = data
        hits = self.collection.search(**search_params)
        return hits

    def _search(self, data, search_params, **kwargs):
        pass

    def query(self, query_params):
        hits = self.collection.query(**query_params)
        return hits

    def _query(self, search_params, **kwargs):
        pass
    
    def drop_collection(self):
        self

    def schema(self):
        return self.collection.schema

    def indexes(self):
        return self.collection.indexes
    
    def is_empty(self):
        return self.collection.is_empty
    
    def num_entities(self):
        return self.collection.num_entities
    
    def primary_field(self):
        return self.collection.primary_field