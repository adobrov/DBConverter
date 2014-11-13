import string
import random

from sqlalchemy import schema
from sqlalchemy.ext.compiler import compiles


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


@compiles(schema.PrimaryKeyConstraint)
def visit_control_name_length(element, compiler, **kw):
    if len(element.name) > 30:
        element.name = element.name[0:24] + id_generator()
    return compiler.visit_primary_key_constraint(element, **kw)
