from sqlalchemy import schema
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql.base import OID
from sqlalchemy.dialects.postgresql.base import TEXT
from sqlalchemy.dialects.postgresql.base import BOOLEAN
from sqlalchemy.dialects.postgresql.base import BIGINT
from sqlalchemy.dialects.postgresql.base import SMALLINT
from sqlalchemy.dialects.postgresql.base import VARCHAR
from sqlalchemy.dialects.postgresql.base import BYTEA
from sqlalchemy.dialects.postgresql.base import DOUBLE_PRECISION
from sqlalchemy.dialects.postgresql.base import TIME


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
    elif isinstance(column.type, BYTEA):
        return '%s RAW(255)' % column.name
    elif isinstance(column.type, DOUBLE_PRECISION):
        return '%s FLOAT(126)' % column.name
    elif isinstance(column.type, SMALLINT):
        return '%s INTEGER' % column.name
    elif isinstance(column.type, TIME):
        return '%s NUMBER(*,0)' % column.name
    elif isinstance(column.type, VARCHAR):
        clause = ''
        if not column.type.length:
            clause = '%s VARCHAR(64)' % column.name
        else:
            clause = '%s %s' % (column.name, str(column.type),)
        if column.server_default:
            if '::' in column.server_default.arg.text:
                clause = '%s DEFAULT %s' % (clause, column.server_default.arg.text.replace('::character varying', ''), )
        return clause
    else:
        return compiler.visit_create_column(element, **kw)
