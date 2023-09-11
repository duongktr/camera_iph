import logging
import json
from netaddr import IPNetwork, IPAddress
from copy import deepcopy

logger = logging.getLogger("query_parser")


class Json2Es:
    def __init__(self, ip_fields=None):
        if ip_fields:
            self.ip_fields_list = ip_fields
        else:
            self.ip_fields_list = []

    def is_ip_string(self, value):
        if value.count(".") != 3 and value.count(":") != 5:
            return False
        try:
            IPAddress(value)
        except Exception:
            try:
                IPNetwork(value)
            except Exception:
                return False
        return True

    def toSubES(self, jSon):
        res = {}
        if "compare" in jSon.keys():
            boolES = "'"
            if jSon["compare"]["function"] is None:
                if jSon["compare"]["operator"] == "=":
                    if jSon["compare"]["value"] is not None:
                        boolES = "must"
                        field = jSon["compare"]["field"]
                        res = {
                            "bool": {
                                boolES: [
                                    {"term": {field: jSon["compare"]["value"]}}
                                ]
                            }
                        }
                    else:
                        boolES = "must_not"
                        field = jSon["compare"]["field"]
                        res = {
                            "bool": {boolES: [{"exists": {"field": field}}]}
                        }
                elif jSon["compare"]["operator"] == "!=":
                    if jSon["compare"]["value"] is not None:
                        boolES = "must_not"
                        field = jSon["compare"]["field"]
                        res = {
                            "bool": {
                                boolES: [
                                    {"term": {field: jSon["compare"]["value"]}}
                                ]
                            }
                        }
                    else:
                        boolES = "must"
                        field = jSon["compare"]["field"]
                        res = {
                            "bool": {boolES: [{"exists": {"field": field}}]}
                        }
                elif jSon["compare"]["operator"] == ">":
                    field = jSon["compare"]["field"]
                    value = jSon["compare"]["value"]
                    res = {"range": {field: {"gt": value}}}
                elif jSon["compare"]["operator"] == ">=":
                    field = jSon["compare"]["field"]
                    value = jSon["compare"]["value"]
                    res = {"range": {field: {"gte": value}}}
                elif jSon["compare"]["operator"] == "<":
                    field = jSon["compare"]["field"]
                    value = jSon["compare"]["value"]
                    res = {"range": {field: {"lt": value}}}
                elif jSon["compare"]["operator"] == "<=":
                    field = jSon["compare"]["field"]
                    value = jSon["compare"]["value"]
                    res = {"range": {field: {"lte": value}}}
                elif jSon["compare"]["operator"] == "~":
                    field = jSon["compare"]["field"]
                    if isinstance(jSon["compare"]["value"], str):
                        jSon["compare"]["value"] = jSon["compare"][
                            "value"
                        ].replace("\\", "\\\\")
                    value = "*" + jSon["compare"]["value"] + "*"
                    res = {"bool": {"must": [{"wildcard": {field: value}}]}}
                elif jSon["compare"]["operator"] == "!~":
                    field = jSon["compare"]["field"]
                    if isinstance(jSon["compare"]["value"], str):
                        jSon["compare"]["value"] = jSon["compare"][
                            "value"
                        ].replace("\\", "\\\\")
                    value = "*" + jSon["compare"]["value"] + "*"
                    res = {
                        "bool": {"must_not": [{"wildcard": {field: value}}]}
                    }
                elif jSon["compare"]["operator"] == "allField":
                    field = "_all_fields"
                    if isinstance(jSon["compare"]["value"], str):
                        jSon["compare"]["value"] = jSon["compare"][
                            "value"
                        ].replace("\\", "\\\\")
                    base_value = jSon["compare"]["value"]
                    value = "*{}*".format(base_value)
                    res = {"bool": {"should": [{"wildcard": {field: value}}]}}
                    if self.is_ip_string(base_value):
                        for ip_field in self.ip_fields_list:
                            res["bool"]["should"].append(
                                {"term": {ip_field: base_value}}
                            )

            elif jSon["compare"]["function"] == "in":
                if jSon["compare"]["operator"] in ["=", "~"]:
                    boolES = "should"
                else:
                    boolES = "must_not"
                field = jSon["compare"]["field"]
                listES = []
                for i in jSon["compare"]["value"]:
                    if i is not None:
                        if jSon["compare"]["operator"] in ["~", "!~"]:
                            if isinstance(i, str):
                                i = i.replace("\\", "\\\\")
                            value = "*{}*".format(i)
                            listES.append({"wildcard": {field: value}})
                        else:
                            listES.append({"term": {field: i}})
                    else:
                        listES.append(
                            {
                                "bool": {
                                    "must_not": [{"exists": {"field": field}}]
                                }
                            }
                        )
                res = {"bool": {boolES: listES}}
            elif jSon["compare"]["function"] == "notin":
                if jSon["compare"]["operator"] in ["=", "~"]:
                    boolES = "must_not"
                else:
                    boolES = "should"
                field = jSon["compare"]["field"]
                listES = []
                for i in jSon["compare"]["value"]:
                    if i is not None:
                        if jSon["compare"]["operator"] in ["~", "!~"]:
                            if isinstance(i, str):
                                i = i.replace("\\", "\\\\")
                            value = "*{}*".format(i)
                            listES.append({"wildcard": {field: value}})
                        else:
                            listES.append({"term": {field: i}})
                    else:
                        listES.append(
                            {
                                "bool": {
                                    "must_not": [{"exists": {"field": field}}]
                                }
                            }
                        )
                res = {"bool": {boolES: listES}}
            else:
                if jSon["compare"]["operator"] == "=":
                    boolES = "must"
                else:
                    boolES = "must_not"
                res = {
                    "bool": {
                        boolES: [
                            {
                                "match": {
                                    jSon["compare"]["field"]: jSon["compare"][
                                        "value"
                                    ]
                                }
                            }
                        ]
                    }
                }
        else:
            if jSon["logic"]["symbol"] == "AND":
                left = self.toSubES(jSon["logic"]["left"])
                right = self.toSubES(jSon["logic"]["right"])
                res = {"bool": {"must": [left, right]}}
            elif jSon["logic"]["symbol"] == "OR":
                left = self.toSubES(jSon["logic"]["left"])
                right = self.toSubES(jSon["logic"]["right"])
                res = {"bool": {"should": [left, right]}}

        return res

    def toES(self, jSon):
        if not jSon:
            return {"query": {"bool": {"filter": []}}}

        res = {}
        if "compare" in jSon:
            boolES = "'"
            if jSon["compare"]["function"] is None:
                if jSon["compare"]["operator"] == "=":
                    if jSon["compare"]["value"] is not None:
                        boolES = "must"
                        field = jSon["compare"]["field"]
                        res = {
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "bool": {
                                                boolES: [
                                                    {
                                                        "term": {
                                                            field: jSon[
                                                                "compare"
                                                            ]["value"]
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    else:
                        boolES = "must_not"
                        field = jSon["compare"]["field"]
                        res = {
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "bool": {
                                                boolES: [
                                                    {
                                                        "exists": {
                                                            "field": field
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        }

                elif jSon["compare"]["operator"] == "!=":
                    if jSon["compare"]["value"] is not None:
                        boolES = "must_not"
                        field = jSon["compare"]["field"]
                        res = {
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "bool": {
                                                boolES: [
                                                    {
                                                        "term": {
                                                            field: jSon[
                                                                "compare"
                                                            ]["value"]
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    else:
                        boolES = "must"
                        field = jSon["compare"]["field"]
                        res = {
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "bool": {
                                                boolES: [
                                                    {
                                                        "exists": {
                                                            "field": field
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                elif jSon["compare"]["operator"] == ">":
                    field = jSon["compare"]["field"]
                    value = jSon["compare"]["value"]
                    res = {
                        "query": {
                            "bool": {
                                "filter": [{"range": {field: {"gt": value}}}]
                            }
                        }
                    }
                elif jSon["compare"]["operator"] == ">=":
                    field = jSon["compare"]["field"]
                    value = jSon["compare"]["value"]
                    res = {
                        "query": {
                            "bool": {
                                "filter": [{"range": {field: {"gte": value}}}]
                            }
                        }
                    }
                elif jSon["compare"]["operator"] == "<":
                    field = jSon["compare"]["field"]
                    value = jSon["compare"]["value"]
                    res = {
                        "query": {
                            "bool": {
                                "filter": [{"range": {field: {"lt": value}}}]
                            }
                        }
                    }
                elif jSon["compare"]["operator"] == "<=":
                    field = jSon["compare"]["field"]
                    value = jSon["compare"]["value"]
                    res = {
                        "query": {
                            "bool": {
                                "filter": [{"range": {field: {"lte": value}}}]
                            }
                        }
                    }
                elif jSon["compare"]["operator"] == "~":
                    field = jSon["compare"]["field"]
                    if isinstance(jSon["compare"]["value"], str):
                        jSon["compare"]["value"] = jSon["compare"][
                            "value"
                        ].replace("\\", "\\\\")
                    value = "*" + jSon["compare"]["value"] + "*"
                    res = {
                        "query": {
                            "bool": {
                                "filter": [
                                    {
                                        "bool": {
                                            "must": [
                                                {"wildcard": {field: value}}
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                elif jSon["compare"]["operator"] == "!~":
                    field = jSon["compare"]["field"]
                    if isinstance(jSon["compare"]["value"], str):
                        jSon["compare"]["value"] = jSon["compare"][
                            "value"
                        ].replace("\\", "\\\\")
                    value = "*" + jSon["compare"]["value"] + "*"
                    res = {
                        "query": {
                            "bool": {
                                "filter": [
                                    {
                                        "bool": {
                                            "must_not": [
                                                {"wildcard": {field: value}}
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                elif jSon["compare"]["operator"] == "allField":
                    field = "_all_fields"
                    if isinstance(jSon["compare"]["value"], str):
                        jSon["compare"]["value"] = jSon["compare"][
                            "value"
                        ].replace("\\", "\\\\")
                    base_value = jSon["compare"]["value"]
                    value = "*{}*".format(base_value)
                    res = {
                        "query": {
                            "bool": {
                                "filter": [
                                    {
                                        "bool": {
                                            "should": [
                                                {"wildcard": {field: value}}
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                    if self.is_ip_string(base_value):
                        for ip_field in self.ip_fields_list:
                            res["query"]["bool"]["filter"][0]["bool"][
                                "should"
                            ].append({"term": {ip_field: base_value}})

            elif jSon["compare"]["function"] == "in":
                if jSon["compare"]["operator"] in ["=", "~"]:
                    boolES = "should"
                else:
                    boolES = "must_not"
                field = jSon["compare"]["field"]
                listES = []
                for i in jSon["compare"]["value"]:
                    if i is not None:
                        if jSon["compare"]["operator"] in ["~", "!~"]:
                            if isinstance(i, str):
                                i = i.replace("\\", "\\\\")
                            value = "*{}*".format(i)
                            listES.append({"wildcard": {field: value}})
                        else:
                            listES.append({"term": {field: i}})
                    else:
                        listES.append(
                            {
                                "bool": {
                                    "must_not": [{"exists": {"field": field}}]
                                }
                            }
                        )
                res = {
                    "query": {"bool": {"filter": [{"bool": {boolES: listES}}]}}
                }
            elif jSon["compare"]["function"] == "notin":
                if jSon["compare"]["operator"] in ["=", "~"]:
                    boolES = "must_not"
                else:
                    boolES = "should"
                field = jSon["compare"]["field"]
                listES = []
                for i in jSon["compare"]["value"]:
                    if i is not None:
                        if jSon["compare"]["operator"] in ["~", "!~"]:
                            if isinstance(i, str):
                                i = i.replace("\\", "\\\\")
                            value = "*{}*".format(i)
                            listES.append({"wildcard": {field: value}})
                        else:
                            listES.append({"term": {field: i}})
                    else:
                        listES.append(
                            {
                                "bool": {
                                    "must_not": [{"exists": {"field": field}}]
                                }
                            }
                        )
                res = {
                    "query": {"bool": {"filter": [{"bool": {boolES: listES}}]}}
                }
            else:
                if jSon["compare"]["operator"] == "=":
                    boolES = "must"
                else:
                    boolES = "must_not"
                res = {
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "bool": {
                                        boolES: [
                                            {
                                                "match": {
                                                    jSon["compare"][
                                                        "field"
                                                    ]: jSon["compare"]["value"]
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }

            return res
        elif "logic" in jSon:
            if jSon["logic"]["symbol"] == "AND":
                left = self.toSubES(jSon["logic"]["left"])
                right = self.toSubES(jSon["logic"]["right"])
                res = {
                    "query": {
                        "bool": {"filter": [{"bool": {"must": [left, right]}}]}
                    }
                }
            elif jSon["logic"]["symbol"] == "OR":
                left = self.toSubES(jSon["logic"]["left"])
                right = self.toSubES(jSon["logic"]["right"])
                res = {
                    "query": {
                        "bool": {
                            "filter": [{"bool": {"should": [left, right]}}]
                        }
                    }
                }
        return res

    def converToEs(self, jSon):
        result = self.toES(jSon)
        self.path_must = []
        return self.toRangeMust(result)

    path_must = []

    def find_must_range(self, es_query, parent_path):
        if isinstance(es_query, dict):
            if "must" in es_query:
                temp = deepcopy(parent_path)
                temp.append("must")
                self.path_must.append(temp)
            for key in es_query:
                temp = deepcopy(parent_path)
                temp.append(key)
                self.find_must_range(es_query[key], temp)
        if isinstance(es_query, list):
            i = 0
            for item in es_query:
                temp = deepcopy(parent_path)
                temp.append(i)
                self.find_must_range(item, temp)
                i += 1

    def toOneRangeMust(self, es_query, path_must, field="ips"):
        temp_value = es_query
        for path in path_must:
            temp_value = temp_value[path]

        logger.debug(temp_value)
        must_range = []
        other_query = []
        for temp in temp_value:
            if not isinstance(temp, dict):
                other_query.append(temp)
                continue

            if (
                "range" in temp
                and isinstance(temp["range"], dict)
                and field in temp["range"]
            ):
                must_range.append(temp)
            else:
                other_query.append(temp)

        if len(must_range) != 2:
            return es_query

        result = {"range": {field: {}}}

        key_list = []
        for temp in must_range:
            for key in temp["range"][field]:
                if key not in ["gte", "lte", "lt", "gt"] or key in key_list:
                    return es_query

                key_list.append(key)
                result["range"][field][key] = temp["range"][field][key]

        #  replace to es_query
        must_query = deepcopy(other_query)
        must_query.append(result)
        temp_value = es_query
        i = 0
        for path in path_must:
            i += 1
            if i < len(path_must):
                temp_value = temp_value[path]
            else:
                temp_value[path] = must_query

        return es_query

    def toRangeMust(self, es_query):
        parent_path = []
        self.find_must_range(es_query, parent_path)
        logger.debug(self.path_must)

        for path_must in self.path_must:
            es_query = self.toOneRangeMust(es_query, path_must)
        return es_query


if __name__ == "__main__":
    logic_json = {
        "compare": {
            "operator": "=",
            "field": "c",
            "function": None,
            "value": True,
        }
    }

    logic_json = {
        "compare": {
            "field": "",
            "operator": "allField",
            "value": "datpm",
            "function": None,
        }
    }

    # ast = {"compare": {"field": "server_id", "operator": "!~", "value": ["ubuntu", "centos"], "function": "in"}}
    logic_json = {
        "logic": {
            "left": {
                "compare": {
                    "field": "",
                    "operator": "allField",
                    "value": "vtpost",
                    "function": None,
                }
            },
            "symbol": "OR",
            "right": {
                "compare": {
                    "field": "group",
                    "operator": "=",
                    "value": "vtpost",
                    "function": None,
                }
            },
        }
    }
    logic_json = {
        "compare": {
            "field": "ips",
            "operator": "=",
            "value": ["datpm", "123"],
            "function": "in",
        }
    }

    logic_json = {
        "logic": {
            "left": {
                "compare": {
                    "field": "ips",
                    "operator": ">=",
                    "value": "10.52.51.14",
                    "function": None,
                }
            },
            "symbol": "AND",
            "right": {
                "compare": {
                    "field": "ips",
                    "operator": "<=",
                    "value": "10.52.51.34",
                    "function": None,
                }
            },
        }
    }

    logic_json = {
        "logic": {
            "left": {
                "logic": {
                    "left": {
                        "compare": {
                            "field": "asset_type",
                            "operator": "=",
                            "value": "computing_device",
                            "function": None,
                        }
                    },
                    "symbol": "AND",
                    "right": {
                        "compare": {
                            "field": "_id",
                            "operator": "!=",
                            "value": "pZkZYGgB3o8EgXVLCLrW",
                            "function": None,
                        }
                    },
                }
            },
            "symbol": "AND",
            "right": {
                "logic": {
                    "left": {
                        "compare": {
                            "field": "ips",
                            "operator": ">=",
                            "value": "10.230.144.243",
                            "function": None,
                        }
                    },
                    "symbol": "AND",
                    "right": {
                        "compare": {
                            "field": "ips",
                            "operator": "<=",
                            "value": "10.230.144.255",
                            "function": None,
                        }
                    },
                }
            },
        }
    }

    json2Es = Json2Es()
    result = json2Es.converToEs(logic_json)
    logger.debug(json.dumps(result, indent=2))

    # print(json2Es.is_ip_string("192.168.2.1/22"))
