from sqlalchemy import schema
from sqlalchemy.ext.compiler import compiles


@compiles(schema.ForeignKeyConstraint)
def visit_prevent_auto_create_fk(element, compiler, **kw):
    return None

