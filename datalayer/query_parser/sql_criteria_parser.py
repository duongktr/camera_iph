from sqlalchemy.ext.associationproxy import (
    AssociationProxyInstance,
    ASSOCIATION_PROXY,
)
from sqlalchemy.sql.expression import cast
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import String, inspect
from sqlalchemy.sql import not_, and_, or_


class SqlCriteriaParser:
    COMPARE_OPERATION_KEY = "compare"
    LOGIC_OPERATION_KEY = "logic"
    LOGIC_AND_OPERATOR = "AND"
    LOGIC_OR_OPERATOR = "OR"
    REGEX_CHARACTERS = ["\\", "_", "%"]

    def __init__(self, model=None, jsonb_field="_data"):
        self.model = model
        self.jsonb_field = jsonb_field

    def _process_field(self, field_name, is_jsonb):
        if field_name is None:
            return None
        if is_jsonb:
            return getattr(self.model, self.jsonb_field)[field_name]
        if self._is_association_proxy_attr(field_name):
            return self._get_remote_attr_of_association_proxy(field_name)
        return getattr(self.model, field_name)

    def _process_value(self, value, is_jsonb):
        if value is not None and is_jsonb:
            return cast(value, JSONB)
        return value

    def _is_jsonb(self, field_name):
        if not field_name:
            return False
        field = getattr(self.model, field_name, None)
        if field:
            return False
        return True

    def to_sql_criteria(self, ast_as_dict):
        if self._is_compare_operation(ast_as_dict):
            return self._make_compare_operation(ast_as_dict)
        left_operand = self.to_sql_criteria(ast_as_dict["logic"]["left"])
        right_operand = self.to_sql_criteria(ast_as_dict["logic"]["right"])
        logic_operator = ast_as_dict.get("logic").get("symbol")
        return self._make_logic_operation(
            logic_operator, left_operand, right_operand
        )

    def _is_compare_operation(self, ast_as_dict):
        if ast_as_dict.get(self.COMPARE_OPERATION_KEY, None):
            return True
        return False

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
        operand_list = list()
        operand_list.append(left_operand)
        operand_list.append(right_operand)
        operator_to_operation_dict = {
            self.LOGIC_AND_OPERATOR: and_(*operand_list),
            self.LOGIC_OR_OPERATOR: or_(*operand_list),
        }
        return operator_to_operation_dict[operator]

    def _make_compare_operation_include_function(self, compare_as_dict):
        function_name = compare_as_dict.get("compare").get("function")
        field_name = compare_as_dict.get("compare").get("field")
        value = compare_as_dict.get("compare").get("value")
        # value = self._escape_regex_characters(value)
        operator = compare_as_dict.get("compare").get("operator")
        if function_name == "in":
            return self._make_in_function(operator, field_name, value)
        if function_name == "notin":
            return not_(self._make_in_function(operator, field_name, value))
        if function_name == "term":
            # return self._make_term_function(operator, field_name, value)
            pass

    def _make_in_function(self, operator, field_name, values):
        is_jsonb = self._is_jsonb(field_name)
        operation_list = list()
        _field = self._process_field(field_name, is_jsonb)
        for value in values:
            _value = self._process_value(value, is_jsonb)
            operation_list.append(_field == _value)
        operator_to_operation_dict = {
            "=": or_(*operation_list),
            "!=": not_(or_(*operation_list)),
        }
        return operator_to_operation_dict.get(operator)

    def _make_compare_operation_without_function(self, compare_as_dict):
        field_name = compare_as_dict.get("compare").get("field")
        value = compare_as_dict.get("compare").get("value")
        operator = compare_as_dict.get("compare").get("operator")

        if operator == "allField":
            value = self._escape_regex_characters(value)
            return self._make_all_field_operation(value)

        is_jsonb = self._is_jsonb(field_name)
        _field = self._process_field(field_name, is_jsonb)
        _value = self._process_value(value, is_jsonb)

        if operator == "=":
            return _field == _value
        elif operator == "!=":
            return _field != _value
        elif operator == "<":
            return _field < _value
        elif operator == "<=":
            return _field <= _value
        elif operator == ">":
            return _field > _value
        elif operator == ">=":
            return _field >= _value
        elif operator == "~":
            value = self._escape_regex_characters(value)
            return cast(_field, String).ilike(f"%{value}%")
        elif operator == "!~":
            value = self._escape_regex_characters(value)
            return not_(cast(_field, String).ilike(f"%{value}%"))

    def _make_all_field_operation(self, value):
        field_name_list = self._get_column_names()
        operation_list = list()
        for field_name in field_name_list:
            is_jsonb = self._is_jsonb(field_name)
            _field = self._process_field(field_name, is_jsonb)
            operation_list.append(cast(_field, String).ilike(f"%{value}%"))

        remote_attr_list = self._get_all_remote_attrs_of_association_proxies()
        for attr in remote_attr_list:
            _field = attr
            operation_list.append(cast(_field, String).ilike(f"%{value}%"))

        return or_(*operation_list)

    def _get_column_names(self):
        column_list = self.model.__table__.columns
        field_name_list = list()
        for column in column_list:
            field_name_list.append(column.name)
        return field_name_list

    def _escape_regex_characters(self, value):
        if isinstance(value, str):
            for regex_character in self.REGEX_CHARACTERS:
                value = value.replace(regex_character, "\\" + regex_character)
        return value

    def _is_association_proxy_attr(self, field_name):
        """
        Determine whether a attribute (with field_name) of model is
        association_proxy or not
        :param field_name:
        :return: True if the attribute (with input field_name) of model is
        association_proxy
        """
        return isinstance(
            getattr(self.model, field_name), AssociationProxyInstance
        )

    def _get_remote_attr_of_association_proxy(self, field_name):
        """
        Get remote attribute of a association proxy attribute (with field_name)
        from model. Remote attribute is a attribute of another model referenced.
        E.g: With the Ticket model, below:
            class Ticket(Base):
                __tablename__ = 'ticket'
                _id = Column(Integer, primary_key=True, autoincrement=True)
                ticket_severity_id = Column(Integer(), ForeignKey('ticket_severity._id'))
                ticket_severity = relationship('TicketSeverity')
                severity = association_proxy('ticket_severity', 'name')

            class TicketSeverity(Base):
                __tablename__ = 'ticket_severity'
                _id = Column(Integer, primary_key=True, autoincrement=True)
                name = Column(String(100), unique=True)

        when calling self._get_remote_attr_of_association_proxy('severity'),
        the returned value is TicketSeverity.name
        :return: remote attribute of a association proxy attribute
        """
        return getattr(self.model, field_name).remote_attr

    def _get_all_remote_attrs_of_association_proxies(self):
        """
        Get all association proxies of model using "inspect API", then get
        remote attribute of them using "descriptor API"
        For detail:
        https://docs.sqlalchemy.org/en/13/core/inspection.html
        https://docs.python.org/3/howto/descriptor.html#invoking-descriptors
        https://stackoverflow.com/questions/41671854/finding-all-association-proxies-in-a-sqlalchemy-class
        :return: remote attribute list
        """
        attr_list = list()
        descriptors = inspect(self.model).all_orm_descriptors
        for desc in descriptors:
            if desc.extension_type is ASSOCIATION_PROXY:
                _field = desc.__get__(None, self.model).remote_attr
                attr_list.append(_field)
        return attr_list
