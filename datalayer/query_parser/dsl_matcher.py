from .ast2logicjson.waxeye_parser import waxeye_parse
from .dsl2ast.parser import Parser
from .dsl2ast.waxeye import AST, ParseError
from .exceptions import (
    DslSyntaxError,
    AstToLogicJsonError,
    DslQueryMatcherError,
)
import logging

logger = logging.getLogger("root")


class DslMatcher:
    COMPARE_OPERATION_KEY = "compare"
    LOGIC_OPERATION_KEY = "logic"
    LOGIC_AND_OPERATOR = "AND"
    LOGIC_OR_OPERATOR = "OR"

    def __init__(self, dsl_query):
        _dsl_query = dsl_query.strip()
        self.match_all = _dsl_query == ""
        if self.match_all:
            return
        self.dsl_query = _dsl_query
        self.ast = self._parse_dsl_to_ast(self.dsl_query)
        self.logic_json = self._parse_ast_to_logic_json(self.ast)

    def is_match(self, obj):
        if self.match_all:
            return True
        self.dict_data = obj
        # matching logic json with input data dictionary
        try:
            return self._evaluate_logic_expression(self.logic_json)
        except Exception as e:
            self._process_error(e)

    @staticmethod
    def _process_error(e):
        if isinstance(e, DslQueryMatcherError):
            raise
        raise DslQueryMatcherError(
            f"There was an error occurred when matching "
            f"dsl with data dictionary"
        ) from e

    @staticmethod
    def _parse_dsl_to_ast(dsl_query: str) -> AST:
        result = Parser().parse(dsl_query)
        if isinstance(result, ParseError):
            raise DslSyntaxError(f'"{dsl_query}" has wrong syntax')
        return result

    @staticmethod
    def _parse_ast_to_logic_json(ast: AST) -> dict:
        try:
            return waxeye_parse(ast.to_dict())
        except Exception as e:
            raise AstToLogicJsonError(
                "There was an error occurred when paring "
                "abstract syntax tree to logic json"
            ) from e

    def _evaluate_logic_expression(self, logic_json):
        if self._is_compare_operation(logic_json):
            return self._make_compare_operation(logic_json)
        left_operand = self._evaluate_logic_expression(
            logic_json["logic"]["left"]
        )
        right_operand = self._evaluate_logic_expression(
            logic_json["logic"]["right"]
        )
        logic_operator = logic_json["logic"]["symbol"]
        return self._make_logic_operation(
            logic_operator, left_operand, right_operand
        )

    def _is_compare_operation(self, logic_json):
        return self.COMPARE_OPERATION_KEY in logic_json

    def _make_compare_operation(self, compare_as_dict):
        if self._is_function_included(compare_as_dict):
            return self._make_compare_operation_include_function(
                compare_as_dict
            )
        return self._make_compare_operation_without_function(compare_as_dict)

    def _is_function_included(self, compare_as_dict):
        if compare_as_dict.get("compare").get("function"):
            return True
        return False

    def _make_logic_operation(self, operator, left_operand, right_operand):
        if operator == self.LOGIC_AND_OPERATOR:
            return left_operand and right_operand
        elif operator == self.LOGIC_OR_OPERATOR:
            return left_operand or right_operand

    def _make_compare_operation_include_function(self, compare_as_dict):
        function_name = compare_as_dict.get("compare").get("function")
        field_name = compare_as_dict.get("compare").get("field")
        value = compare_as_dict.get("compare").get("value")
        operator = compare_as_dict.get("compare").get("operator")

        field_value = self._get_field_value(field_name)

        if function_name == "in":
            return self._make_in_function(operator, field_value, value)
        if function_name == "notin":
            return not self._make_in_function(operator, field_value, value)

    def _make_in_function(self, operator, field_value, values):
        if operator == "=":
            return field_value in values
        elif operator == "!=":
            return field_value not in values

    def _make_compare_operation_without_function(self, compare_as_dict):
        field_name = compare_as_dict.get("compare").get("field")
        value = compare_as_dict.get("compare").get("value")
        operator = compare_as_dict.get("compare").get("operator")

        if operator == "allField":
            return self._make_all_field_operation(value)

        field_value = self._get_field_value(field_name)
        if operator == "=":
            return field_value == value
        elif operator == "!=":
            return field_value != value
        elif operator == "<":
            return field_value < value if field_value is not None else False
        elif operator == "<=":
            return field_value <= value if field_value is not None else False
        elif operator == ">":
            return field_value > value if field_value is not None else False
        elif operator == ">=":
            return field_value >= value if field_value is not None else False
        elif operator == "~":
            return (
                value.lower() in str(field_value).lower()
                if field_value is not None
                else False
            )
        elif operator == "!~":
            return (
                value.lower() not in str(field_value).lower()
                if field_value is not None
                else False
            )

    def _get_field_value(self, field_name: str):
        if self._is_single_key(field_name):
            if field_name not in self.dict_data:
                logger.info(f'Field "{field_name}" is not exist in dictionary {self.dict_data}')
                return None

            return self.dict_data[field_name]

        key_list = field_name.split(".")
        value = dict(self.dict_data)
        for key in key_list:
            if value is None or key not in value:
                logger.info(f'Field "{field_name}" is not exist in dictionary {value}')
                return None
            value = value[key]
        return value

    @staticmethod
    def _is_single_key(field_name):
        return "." not in field_name

    def _make_all_field_operation(self, value):
        return value.lower() in str(self.dict_data).lower()
