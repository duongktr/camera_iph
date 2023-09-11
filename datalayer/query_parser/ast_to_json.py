from . import DslMatcher


def parse_query_string_to_json(ast_uri=None, query=None):
    if query is None:
        query = ""
    matcher = DslMatcher(query)
    return matcher.logic_json
