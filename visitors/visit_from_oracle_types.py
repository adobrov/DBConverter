from sqlalchemy import schema
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.oracle.base import BLOB
from sqlalchemy.dialects.oracle.base import NUMBER
from sqlalchemy.dialects.oracle.base import CLOB

@compiles(schema.CreateColumn)
def visit_from_oracle_types(element, compiler, **kw):
    column = element.element
    if isinstance(column.type, BLOB):
        return '%s OID' % column.name
    elif isinstance(column.type, NUMBER):
        return '%s INTEGER' % column.name
    elif isinstance(column.type, CLOB):
        return '%s TEXT' % column.name
    else:
        return compiler.visit_create_column(element, **kw)
