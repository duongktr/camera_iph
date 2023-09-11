class DslQueryMatcherError(Exception):
    """There was an error occurred when matching dsl with data dictionary"""


class DslToAstError(DslQueryMatcherError):
    """There was an error occurred when parsing dsl query to abstract syntax tree"""


class DslSyntaxError(DslToAstError):
    """Dsl query has wrong syntax"""


class AstToLogicJsonError(DslQueryMatcherError):
    """There was an error occurred when parsing abstract syntax tree to logic json"""


class FieldIsNotExist(DslQueryMatcherError):
    """Field name in dsl query is not exist"""
