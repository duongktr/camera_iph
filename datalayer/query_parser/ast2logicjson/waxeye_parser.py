# coding=utf-8
# thangld_anm
# Waxeye to JSON
class DslJSON:
    def __init__(
        self, logic=None, compare=None, left=None, symbol=None, right=None
    ):
        self.logic = logic
        self.compare = compare
        self.left = left
        self.symbol = symbol
        self.right = right


class CompareJSON:
    def __init__(
        self, field=None, operator=None, value=None, function=None, symbol=None
    ):
        self.field = field
        self.operator = operator
        self.value = value
        self.function = function
        self.symbol = symbol


class ValueJSON:
    def __init__(self, value=None, function=None):
        self.value = value
        self.function = function


# class LogicJSON:
#     def __init__(self, left=None, symbol=None, right=None):
#         self.left = left
#         self.symbol = symbol
#         self.right = right


def fieldToNumber(child):
    res = 0
    for x in child["children"]:
        if x != "-" and x != ".":
            res = res * 10 + int(x)
    if child["children"][0] == "-":
        res = -res
    childout = 1.0
    breakout = False
    for x in child["children"]:
        if x == ".":
            breakout = True
        elif breakout:
            childout = childout * 10
    if childout != 1.0:
        res = res / childout
    return res


def fieldToString(child):
    res = ""
    for x in child["children"]:
        if "type" in x:
            res += str(x["children"][0])
        else:
            res += str(x)
    # print("Real value = ", res)
    return res


def valueToString(child):
    res = ""
    for x in child["children"]:
        if "type" in x:
            res += str(x["children"][0])
        else:
            res += str(x)
    # print("Real value = " , res)
    return res


def valueToLiteral(child):
    # print('print(child['children']) = ',child['children'][0]['children'][0])
    if child["children"][0] == "T":
        return True
    elif child["children"][0] == "F":
        return False
    else:
        return None


def valueToAllField(child):
    res = ""
    for x in child["children"]:
        if "type" in x:
            res += str(x["children"][0])
        else:
            res += str(x)
    # print("Real value = " , res)
    return res


def operatorToString(operator):
    res = "".join(str(x) for x in operator)
    return res


def getArray(mother):
    children = mother["children"][0]["children"]
    res = []
    for i in children:
        if i["children"][0]["type"] in ["string", "ipValue"]:
            y = fieldToString(i["children"][0])
        elif i["children"][0]["type"] == "number":
            y = fieldToNumber(i["children"][0])
        else:
            y = valueToLiteral(i["children"][0])
        res.append(y)
    return res


def getChildOfFunction(children):
    type = children["children"][0]["type"]
    if type == "term":
        return fieldToString(children["children"][0]["children"][0])
    else:
        return getArray(children["children"][0])


def getTypeOfFunction(children):
    return children["children"][0]["type"]


def toJsonValue(children):
    res = ValueJSON()
    type = children["children"][0]["type"]
    if type == "number":
        child = fieldToNumber(children["children"][0])
    elif type in ["string", "ipValue"]:
        child = valueToString(children["children"][0])
    elif type == "literal":
        child = valueToLiteral(children["children"][0])
    else:
        type = children["children"][0]["type"]
        if type == "function":
            child = getChildOfFunction(children["children"][0])
            function = getTypeOfFunction(children["children"][0])
            res.function = function
        else:
            child = children["children"][0]
    res.value = child
    return res


def toJsonNotValue(children):
    res = ValueJSON()
    type = children["type"]
    if type == "number":
        child = fieldToNumber(children)
    elif type in ["string", "ipValue"]:
        child = valueToString(children)
    elif type == "literal":
        child = valueToLiteral(children)
    elif type == "allField":
        child = valueToAllField(children)
    else:
        type = children["type"]
        if type == "function":
            child = getChildOfFunction(children)
            function = getTypeOfFunction(children)
            res.function = function
        else:
            child = children
    res.value = child
    return res


def toJsonLogic(children):
    return children


def toJsonCompare(children):
    res = CompareJSON()
    child = children[0]["children"]
    if children[0]["type"] == "allField":
        res.field = ""
    else:
        res.field = fieldToString(child[0])
    value = child[len(child) - 1]
    if value["type"] == "value":
        tojsonvalue = toJsonValue(value)
    else:
        tojsonvalue = toJsonNotValue(value)
    res.value = tojsonvalue.value
    res.function = tojsonvalue.function
    if children[0]["type"] == "allField":
        res.operator = "allField"
    else:
        operator = child[1:-1]
        res.operator = operatorToString(operator)
    return res


def toJsonDsl(children):
    res = DslJSON()

    if len(children) == 1:
        child = children[0]
        if child["type"] == "dsl":
            res.logic = toJsonDsl(child["children"])
        elif child["type"] == "compare":
            res.compare = toJsonCompare(child["children"])
    else:
        child = children[len(children) - 1]
        if child["type"] == "dsl":
            res.right = toJsonDsl(child["children"])
        elif child["type"] == "compare":
            res.right = toJsonCompare(child["children"])
        res.symbol = fieldToString(children[len(children) - 2])
        if len(children[: (len(children) - 3)]) > 1:
            res.left = toJsonDsl(children[: (len(children) - 2)])
        else:
            if children[0]["type"] == "dsl":
                res.left = toJsonDsl(children[0]["children"])
            else:
                res.left = toJsonCompare(children[0]["children"])
    return res


def instanceToDict(res):
    if res.symbol != None:
        result = {
            "logic": {
                "left": instanceToDict(res.left),
                "symbol": res.symbol,
                "right": instanceToDict(res.right),
            }
        }
    else:
        if hasattr(res, "compare"):
            # if "compare" in res:
            if res.compare != None:
                result = {
                    "compare": {
                        "field": res.compare.field,
                        "operator": res.compare.operator,
                        "value": res.compare.value,
                        "function": res.compare.function,
                    }
                }
            else:
                result = instanceToDict(res.logic)
        else:
            result = {
                "compare": {
                    "field": res.field,
                    "operator": res.operator,
                    "value": res.value,
                    "function": res.function,
                }
            }
    return result


def waxeye_parse(body):
    if not body:
        return {}
    if "children" not in body:
        return {}

    ast = body
    # if ast.__class__.__name__ == waxeye.ParseError.__name__:
    #     return False
    # print(ast)

    # Parse our input
    res = toJsonDsl(ast["children"])
    result = instanceToDict(res)
    return result


if __name__ == "__main__":
    import json

    # ast_tree={"pos":[0, 3], "children":[{"pos":[0, 3], "children":[{"pos":[0, 3], "children":[{"pos":[0, 1], "children":["a"], "type": "field"}, "=", {"pos":[2, 3], "children":[{"pos":[2, 3], "children":["1"], "type": "number"}], "type": "value"}], "type": "equals"}], "type": "compare"}], "type": "dsl"}
    ast_tree = {
        "pos": [0, 7],
        "children": [
            {
                "pos": [0, 7],
                "children": [
                    {
                        "pos": [0, 7],
                        "children": [
                            {
                                "pos": [0, 7],
                                "children": ["d", "a", "t", "p", "m"],
                                "type": "string",
                            }
                        ],
                        "type": "allField",
                    }
                ],
                "type": "compare",
            }
        ],
        "type": "dsl",
    }
    # ast_tree={"pos":[0,32],"children":[{"pos":[1,17],"children":[{"pos":[1,6],"children":[{"pos":[1,6],"children":[{"pos":[1,2],"children":["a"],"type":"field"},"=",{"pos":[4,6],"children":[{"pos":[4,5],"children":["1"],"type":"number"}],"type":"value"}],"type":"equals"}],"type":"compare"},{"pos":[6,9],"children":["A","N","D"],"type":"logic"},{"pos":[9,17],"children":[{"pos":[9,17],"children":[{"pos":[10,17],"children":["d","a","t","p","m"],"type":"string"}],"type":"allField"}],"type":"compare"}],"type":"dsl"},{"pos":[19,22],"children":["A","N","D"],"type":"logic"},{"pos":[22,32],"children":[{"pos":[22,32],"children":[{"pos":[23,31],"children":["d","a","t","p","m","2"],"type":"string"}],"type":"allField"}],"type":"compare"}],"type":"dsl"}
    #  {"query" : "a ~ IN([\"datpm\"])"}
    ast_tree = {
        "pos": [0, 17],
        "children": [
            {
                "pos": [0, 17],
                "children": [
                    {
                        "pos": [0, 17],
                        "children": [
                            {
                                "pos": [0, 1],
                                "children": ["a"],
                                "type": "field",
                            },
                            "~",
                            {
                                "pos": [4, 17],
                                "children": [
                                    {
                                        "pos": [4, 17],
                                        "children": [
                                            {
                                                "pos": [4, 17],
                                                "children": [
                                                    {
                                                        "pos": [7, 16],
                                                        "children": [
                                                            {
                                                                "pos": [8, 15],
                                                                "children": [
                                                                    {
                                                                        "pos": [
                                                                            8,
                                                                            15,
                                                                        ],
                                                                        "children": [
                                                                            "d",
                                                                            "a",
                                                                            "t",
                                                                            "p",
                                                                            "m",
                                                                        ],
                                                                        "type": "string",
                                                                    }
                                                                ],
                                                                "type": "value",
                                                            }
                                                        ],
                                                        "type": "array",
                                                    }
                                                ],
                                                "type": "in",
                                            }
                                        ],
                                        "type": "function",
                                    }
                                ],
                                "type": "value",
                            },
                        ],
                        "type": "equals",
                    }
                ],
                "type": "compare",
            }
        ],
        "type": "dsl",
    }
    # ast_tree={"pos":[0,17],"children":[{"pos":[0,17],"children":[{"pos":[0,17],"children":[{"pos":[0,1],"children":["a"],"type":"field"},"=",{"pos":[4,17],"children":[{"pos":[4,17],"children":[{"pos":[4,17],"children":[{"pos":[7,16],"children":[{"pos":[8,15],"children":[{"pos":[8,15],"children":["d","a","t","p","m"],"type":"string"}],"type":"value"}],"type":"array"}],"type":"in"}],"type":"function"}],"type":"value"}],"type":"equals"}],"type":"compare"}],"type":"dsl"}
    ast_tree = {
        "pos": [0, 37],
        "children": [
            {
                "pos": [0, 37],
                "children": [
                    {
                        "pos": [0, 37],
                        "children": [
                            {
                                "pos": [0, 9],
                                "children": [
                                    "s",
                                    "e",
                                    "r",
                                    "v",
                                    "e",
                                    "r",
                                    "_",
                                    "i",
                                    "d",
                                ],
                                "type": "field",
                            },
                            "!",
                            "~",
                            {
                                "pos": [13, 37],
                                "children": [
                                    {
                                        "pos": [13, 37],
                                        "children": [
                                            {
                                                "pos": [13, 37],
                                                "children": [
                                                    {
                                                        "pos": [16, 36],
                                                        "children": [
                                                            {
                                                                "pos": [
                                                                    17,
                                                                    25,
                                                                ],
                                                                "children": [
                                                                    {
                                                                        "pos": [
                                                                            17,
                                                                            25,
                                                                        ],
                                                                        "children": [
                                                                            "u",
                                                                            "b",
                                                                            "u",
                                                                            "n",
                                                                            "t",
                                                                            "u",
                                                                        ],
                                                                        "type": "string",
                                                                    }
                                                                ],
                                                                "type": "value",
                                                            },
                                                            {
                                                                "pos": [
                                                                    27,
                                                                    35,
                                                                ],
                                                                "children": [
                                                                    {
                                                                        "pos": [
                                                                            27,
                                                                            35,
                                                                        ],
                                                                        "children": [
                                                                            "c",
                                                                            "e",
                                                                            "n",
                                                                            "t",
                                                                            "o",
                                                                            "s",
                                                                        ],
                                                                        "type": "string",
                                                                    }
                                                                ],
                                                                "type": "value",
                                                            },
                                                        ],
                                                        "type": "array",
                                                    }
                                                ],
                                                "type": "in",
                                            }
                                        ],
                                        "type": "function",
                                    }
                                ],
                                "type": "value",
                            },
                        ],
                        "type": "equals",
                    }
                ],
                "type": "compare",
            }
        ],
        "type": "dsl",
    }
    print(json.dumps(waxeye_parse(ast_tree)))
