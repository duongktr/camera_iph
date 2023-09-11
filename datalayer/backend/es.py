from elasticsearch import Elasticsearch
from datalayer.query_parser.ast_to_json import parse_query_string_to_json
from datalayer.query_parser.jsonToES import Json2Es
from datalayer.query_parser.query_parser_exception import QueryParserException


DEFAULT_AST_URI = "http://testserver:9082"
DEFAULT_OFFSET = 0
DEFAULT_LIMIT = 10


def make_query_body(identity):
    search_term = []
    for key, val in identity.items():
        if isinstance(val, list):
            term = {"bool": {"should": [{"term": {key: v}} for v in val]}}
        else:
            term = {"term": {key: val}}
        search_term.append(term)
    query_body = {
        "query": {"bool": {"filter": [{"bool": {"must": search_term}}]}}
    }
    return query_body


class ElasticsearchBackend:
    allow_sort = "ALL"

    def __init__(
        self,
        host=None,
        port=None,
        index_mapping=None,
        index_prefix=None,
        index_suffix=None,
        config=None,
    ):
        if not (
            bool(index_mapping) ^ (bool(index_prefix) or bool(index_suffix))
        ):
            raise Exception(
                "You should choose one out of "
                "index_mapping and index_prefix"
            )
        if host and port:
            self.es = Elasticsearch([{"host": host, "port": port}])
        self.index_mapping = index_mapping
        self.index_prefix = index_prefix or ""
        self.index_suffix = index_suffix or ""
        self.ast_uri = DEFAULT_AST_URI
        if config is not None:
            self.ast_uri = config.get("AST_URI", self.ast_uri)

    def _index_name(self, names):
        _names = names.replace(" ", "").split(",")
        _indexes = []
        if self.index_prefix:
            _indexes = [
                self.index_prefix + name + self.index_suffix for name in _names
            ]
        elif self.index_mapping:
            _indexes = [self.index_mapping.get(name) for name in _names]
        return ",".join(_indexes)

    def _bin_sort(self, sort):
        if not sort:
            return None
        sort = (
            sort.replace(" ", "")
            .replace("\t", "")
            .replace("\r", "")
            .replace("\n", "")
            .split(",")
        )

        qr_sort = []
        for item in sort:
            type_sort = "asc"
            key_name = item
            if item.find("-") == 0:
                type_sort = "desc"
                key_name = item[1:]
            if self.allow_sort != "ALL" and (
                key_name not in self.allow_sort
                or not self.allow_sort[key_name]
            ):
                continue
            qr_sort.append(
                {key_name: {"order": type_sort, "unmapped_type": "keyword"}}
            )
        if not qr_sort:
            return None
        return qr_sort

    def insert(self, index, body, refresh="true"):
        _index = self._index_name(index)
        res = self.es.index(index=_index, body=body, refresh=refresh)
        res["doc"] = body
        return res

    def update(self, index, body, refresh="true", _id=None):
        _index = self._index_name(index)
        res = self.es.index(index=_index, body=body, refresh=refresh, id=_id)
        res["doc"] = body
        return res

    def search(self, index, identity=None, es_query=None):
        _index = self._index_name(index)

        if es_query:
            results = self.es.search(index=_index, body=es_query)["hits"][
                "hits"
            ]
        else:
            results = self.es.search(
                index=_index, body=make_query_body(identity)
            )["hits"]["hits"]

        return results

    def _search(self, index, identity=None, es_query=None, **kwargs):
        _index = self._index_name(index)

        if es_query:
            return self.es.search(index=_index, body=es_query, **kwargs)
        elif identity:
            return self.es.search(
                index=_index, body=make_query_body(identity), **kwargs
            )
        return self.es.search(index=_index, **kwargs)

    def _parse_query_string_to_es_query(self, query_string):
        try:
            json2Es = Json2Es()
            if query_string in ("", '""') or not query_string:
                return json2Es.converToEs(None)
            query_as_json = parse_query_string_to_json(
                self.ast_uri, query_string
            )
            return json2Es.converToEs(query_as_json)
        except Exception:
            raise QueryParserException()

    def filter_by_query_string(self, index, query_string, **kwargs):
        es_query = self._parse_query_string_to_es_query(query_string)
        offset = kwargs.get("offset", DEFAULT_OFFSET)
        limit = kwargs.get("limit", DEFAULT_LIMIT)
        order_by = self._bin_sort(kwargs.get("order_by", None))

        es_query["from"] = offset
        es_query["size"] = limit
        if order_by:
            es_query["sort"] = order_by

        return self._search(index=index, es_query=es_query)["hits"]["hits"]
