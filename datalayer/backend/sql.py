import os
from sqlalchemy import create_engine, func, and_
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.expression import cast
from datalayer.query_parser.ast_to_json import parse_query_string_to_json
from datalayer.query_parser.sql_criteria_parser import SqlCriteriaParser
from datalayer.query_parser.query_parser_exception import QueryParserException


DEFAULT_POSTGRES_URI = "postgres://localhost:5432"
DEFAULT_AST_URI = "http://localhost:9082"
DEFAULT_OFFSET = 0
DEFAULT_LIMIT = 100


class ObjectNotFoundError(Exception):
    def __init__(self, table, _id):
        self.table = table
        self._id = _id

    def __str__(self):
        return (
            f"Object with id {self._id} cannot be found "
            f"in the table {self.table}"
        )


class JsonbColumnNotAvaiable(Exception):
    def __init__(self, table, column):
        self.table = table
        self.column = column

    def __str__(self):
        return (
            f'Table "{self.table}" cannot accept field "{self.column}" '
            f" because jsonb column is not avaiable"
        )


def alchemize(mapping, fields=None, jsonb_field="_data"):
    if fields is None:
        return None

    if isinstance(fields, str):
        fields = [fields]

    if hasattr(mapping, fields[0]):
        # field[0] is not contained in _data
        root_field = fields[0]
        childs = fields[1:]
    else:
        root_field = jsonb_field
        childs = fields

    res = getattr(mapping, root_field)
    for c in childs:
        res = res[c]
    return res


class PostgreSQLBackend:
    def __init__(self, config=None, **kwargs):
        self.connect(**kwargs)
        self.ast_uri = os.environ.get("AST_URI", "http://0.0.0.0:9082")
        if config is not None:
            self.ast_uri = config.get("AST_URI", self.ast_uri)

    def connect(self, jsonb_field="_data", **kwargs):
        engine = kwargs.get("engine", None)
        base = kwargs.get("base_model", None)
        uri = kwargs.get("uri", DEFAULT_POSTGRES_URI)
        if engine is None:
            self.engine = create_engine(uri)
        else:
            self.engine = engine

        if base is None:
            self.base = automap_base()
            self.base.prepare(self.engine, reflect=True)
        else:
            self.base = base

        self.session = Session(self.engine, expire_on_commit=False)
        self.jsonb_field = jsonb_field

    def close_session(self):
        self.session.close()

    def execute(self, sql):
        return self.engine.execute(sql)

    def _get_mapping(self, table):
        """
        Get SQL mapping

        """
        return getattr(self.base.classes, table)

    def _prepare_data(self, model, data):
        """Convert from flattened data to model-like data in which the
        extensible fields are put in a jsonb field.

        """
        has_jsonb = getattr(model, self.jsonb_field, None) is not None

        if has_jsonb:
            _data = {self.jsonb_field: {}}
        else:
            _data = {}

        for k, v in data.items():
            if getattr(model, k, None):
                _data[k] = v
            else:
                if has_jsonb:
                    _data[self.jsonb_field][k] = v
                else:
                    raise JsonbColumnNotAvaiable(
                        table=model.__name__, column=k
                    )
        return _data

    def _serialize_data(self, obj, mapping):
        """Convert python object containing _data attribute to a flattened
        dict in which keys and values inside _data are moved to root
        level.

        """
        res = {}
        columns = [c.key for c in mapping.__table__.columns]

        if obj is None:
            return None

        for c in columns:
            v = getattr(obj, c)
            if c == self.jsonb_field:
                if isinstance(v, dict):
                    for k1, v1 in v.items():
                        res[k1] = v1
            else:
                res[c] = v
        return res

    def make_model(self, model, data):
        """Create a sql model from given data knowing what its model is.

        """

        return model(**self._prepare_data(model, data))

    def _get(self, table, _id, tenant_token=None, query_string=""):
        """Low level function to retrieve record from Postgres. The return
        value is an object whose properties are corresponding to the
        table's columns.

        """
        model = self._get_mapping(table)
        result = self.session.query(model).filter(model._id == _id)
        if tenant_token and getattr(model, "tenant", None):
            result = result.filter(model.tenant == tenant_token)
        if query_string:
            criteria = self._parse_query_string_to_sql_criteria(model, query_string)
            result = result.filter(criteria)
        result = result.first()

        if result is None:
            raise ObjectNotFoundError(table, _id)
        return result

    def get(self, table, _id, tenant_token=None, query_string=""):
        mapping = self._get_mapping(table)
        try:
            obj = self._get(table, _id, tenant_token, query_string)
        except ObjectNotFoundError as err:
            obj = None
        return self._serialize_data(obj, mapping)

    def _insert(self, table, data):
        model = self._get_mapping(table)
        obj = self.make_model(model, data)
        self.session.add(obj)
        return obj

    def create(self, table, data, commit=True):
        model = self._get_mapping(table)
        obj = self._insert(table, data)
        self.session.flush()
        if commit:
            self.session.commit()
        return self._serialize_data(obj, model)

    def _update(self, table, _id, data, **kwargs):
        model = self._get_mapping(table)
        obj = self._get(table, _id, tenant_token=kwargs.get("_tenant"), query_string=kwargs.get("query_string"))
        _data = self._prepare_data(model, data)
        jsonb_columns = []
        for k, v in _data.items():
            if isinstance(v, dict):
                jsonb_columns.append(k)
                if k == self.jsonb_field:
                    # colum k is used to contain dynamic columns
                    jsonb_data = getattr(obj, k, {})
                    if jsonb_data is None:
                        jsonb_data = {}
                    jsonb_data.update(v)
                    setattr(obj, k, jsonb_data)
                else:
                    # k is static, outer column
                    setattr(obj, k, v)
                # obj.data = jsonb
            else:
                setattr(obj, k, v)
        for c in jsonb_columns:
            flag_modified(obj, c)
        self.session.add(obj)
        self.session.flush()
        return obj

    def update(self, table, _id, data, commit=True, **kwargs):
        model = self._get_mapping(table)
        obj = self._update(table, _id, data, **kwargs)
        if commit:
            self.session.commit()
        return self._serialize_data(obj, model)

    def _delete(self, table, _id, tenant_token=None):
        """Delete an object with the given _id but not commit the change to
        DB.

        """

        obj = self._get(table, _id, tenant_token)
        self.session.delete(obj)
        return None

    def delete(self, table, _id, commit=True, tenant_token=None):
        """Delete an object with the given _id and commit the change to DB"""

        self._delete(table, _id, tenant_token)
        self.session.flush()
        if commit:
            self.session.commit()
        return None

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def __get_field(self, model, field_name):

        if field_name is None:
            return None

        if field_name.startswith("-"):
            _field_name = field_name[1:]
            descending = True
        else:
            _field_name = field_name
            descending = False

        _field = getattr(model, _field_name, None)
        if _field is None:
            _field = getattr(model, self.jsonb_field)[_field_name]
        if descending:
            return _field.desc()
        return _field

    def _filter_dict_to_sql_criteria(self, model, _filter):
        """Convert a dict of criteria to sqlalchemy criteria."""

        criteria = self._prepare_data(model, _filter)
        __filter = []
        for k, v in criteria.items():
            if k == self.jsonb_field:
                for k1, v1 in v.items():
                    __filter.append(getattr(model, k)[k1] == cast(v1, JSONB))
            else:
                __filter.append(getattr(model, k) == v)
        return and_(*__filter)

    def _parse_query_string_to_sql_criteria(self, model, query_string):
        try:
            query_as_json = parse_query_string_to_json(
                self.ast_uri, query_string
            )
            criteria_parser = SqlCriteriaParser(model)
            return criteria_parser.to_sql_criteria(query_as_json)
        except Exception:
            raise QueryParserException()

    def _filter(self, table, criteria, **kwargs):
        """Query a table using the condition specified with _filter.

        Keyword parameters are:
        - offset
        - limit
        - order_by
        """

        model = self._get_mapping(table)

        offset = kwargs.get("offset", DEFAULT_OFFSET)
        limit = kwargs.get("limit", DEFAULT_LIMIT)
        order_by = self.__get_field(model, kwargs.get("order_by", None))
        return (
            self.session.query(model)
            .filter(criteria)
            .order_by(order_by)
            .offset(offset)
            .limit(limit)
        )

    def _count(self, table, criteria, **kwargs):
        """Count
        """

        model = self._get_mapping(table)
        return self.session.query(func.count(model._id)).filter(criteria).scalar()

    def count(self, table, _filter, **kwargs):
        model = self._get_mapping(table)
        return self._count(
            table, self._filter_dict_to_sql_criteria(model, _filter), **kwargs
        )

    def filter(self, table, _filter, **kwargs):
        model = self._get_mapping(table)
        objs = self._filter(
            table, self._filter_dict_to_sql_criteria(model, _filter), **kwargs
        ).all()
        return [self._serialize_data(o, model) for o in objs]

    def filter_by_query_string(self, table, query_string, **kwargs):
        model = self._get_mapping(table)
        criteria = True
        if query_string not in ("", '""'):
            criteria = self._parse_query_string_to_sql_criteria(
                model, query_string
            )
        objs = self._filter(table, criteria, **kwargs).all()
        return [self._serialize_data(o, model) for o in objs]

    def count_by_query_string(self, table, query_string, **kwargs):
        model = self._get_mapping(table)
        criteria = True
        if query_string not in ("", '""'):
            criteria = self._parse_query_string_to_sql_criteria(
                model, query_string
            )
        return self._count(table, criteria, **kwargs)

    def count_and_group_by_with_query_string(
        self, table, query_string, count_field, group_by_field, **kwargs
    ):
        model = self._get_mapping(table)

        offset = kwargs.get("offset", 0)
        limit = kwargs.get("limit", None)
        order_by = self.__get_field(model, kwargs.get("order_by"))

        _count_field = self.__get_field(model, count_field)
        _group_by_field = self.__get_field(model, group_by_field)
        criteria = self._parse_query_string_to_sql_criteria(
            model, query_string
        )

        res = (
            self.session.query(_group_by_field, func.count(_count_field))
            .filter(criteria)
            .group_by(_group_by_field)
            .order_by(order_by)
            .offset(offset)
            .limit(limit)
            .all()
        )
        return res

    def __make_filter_params(self, model, query_string):
        if query_string not in ("", '""', None):
            return self._parse_query_string_to_sql_criteria(model, query_string)
        return True

    def qrs(self, model, query_string):
        return self.__make_filter_params(model, query_string)

    def __get_table_and_column_from_string(self, s):
        _s = s.split(".")
        table_obj = self._get_mapping(_s[0])
        column_obj = self.__get_field(table_obj, _s[1])
        # column_obj = getattr(table_obj, _s[1])
        return (table_obj, column_obj)

    def __parse_join_stm(self, join_stm):
        # Clean up and split in 3 parts
        s = " ".join(join_stm.split(" ")).split(" ")
        table1_obj, column1_obj = self.__get_table_and_column_from_string(s[0])
        table2_obj, column2_obj = self.__get_table_and_column_from_string(s[2])
        join_condition = column1_obj == column2_obj
        return (table1_obj, table2_obj, join_condition)

    def __make_query_params(self, tables, selected_columns=None):
        if selected_columns is None:
            return tables
        params = []
        for c in selected_columns:
            _, column_obj = self.__get_table_and_column_from_string(c)
            params.append(column_obj)
        return params

    def __remove_duplicate_tables(self, tables):
        # Get list of tables
        _tables = []
        _names = set()
        for t in tables:
            if t.__name__ not in _names:
                _names.add(t.__name__)
                _tables.append(t)
        return _tables

    def __serialize_join(self, join_response):
        ret = []
        for row in join_response:
            _row = {}
            for table in row:
                name = table.__table__.fullname
                _row[name] = self._serialize_data(
                    table, self._get_mapping(name)
                )
            ret.append(_row)
        return ret

    def join(self, *join_stms, **kwargs):
        """Execute a join statement implicitly by the join criterial
        respresented in the join_stms argument.

        Possible keyword arguments are:
        - columns
        - query_string
        - offset
        - limit
        - order_by

        The arugment columns speficies which columns to return in the
        returned value.

        Other arguments have the same meaning with other functions.

        The return value is a list of row (dict object) which has the
        below format:

        {
           "table1": {
              "column": value
               ...
           },
           "table2": {
              "column": value
               ...
           }
        }

        """
        _join_info = [self.__parse_join_stm(s) for s in join_stms]
        selected_columns = kwargs.get("columns", None)

        _tables = []
        for t1, t2, _ in _join_info:
            _tables.append(t1)
            _tables.append(t2)
        tables = self.__remove_duplicate_tables(_tables)

        query_params = self.__make_query_params(tables, selected_columns)
        offset = kwargs.get("offset", 0)
        limit = kwargs.get("limit", None)
        query_string = kwargs.get("query_string", None)
        # query_dict = kwargs.get('query_dict', None)
        filter_params = self.__make_filter_params(_tables[0], query_string)
        _order_by = kwargs.get("order_by", None)
        if _order_by:
            _, order_by = self.__get_table_and_column_from_string(_order_by)
        else:
            order_by = None

        _query = self.session.query(*query_params)
        for _, t2, join_condition in _join_info:
            _query = _query.join(t2, join_condition)

        res = (
            _query.filter(filter_params)
            .order_by(order_by)
            .offset(offset)
            .limit(limit)
            .all()
        )

        if selected_columns is not None:
            return res
        return self.__serialize_join(res)
