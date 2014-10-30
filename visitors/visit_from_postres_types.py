from sqlalchemy import schema
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql.base import OID
from sqlalchemy.dialects.postgresql.base import TEXT
from sqlalchemy.dialects.postgresql.base import BOOLEAN
from sqlalchemy.dialects.postgresql.base import BIGINT

@compiles(schema.CreateColumn)
def visit_from_postgres_types(element, compiler, **kw):
    column = element.element
    if isinstance(column.type, OID):
        return '%s BLOB' % column.name
    elif isinstance(column.type, TEXT):
        return '%s CLOB' % column.name
    elif isinstance(column.type, BOOLEAN):
        return '%s INTEGER' % column.name
    elif isinstance(column.type, BIGINT):
        return '%s INTEGER' % column.name
    else:
        return compiler.visit_create_column(element, **kw)
