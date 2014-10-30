from postgres_blob_manager import PostgresBlobManager
from sqlalchemy.dialects.postgresql.base import OID


def get_function_to(migration_type, connection):

    func_type_mapper_to_postgres = {
        OID: PostgresBlobManager.get_manager(connection).write
    }

    func_type_mapper_to_oracle = {

    }

    if migration_type.endswith('postgresql'):
        return func_type_mapper_to_postgres
    elif migration_type.endswith('oracle'):
        return func_type_mapper_to_oracle
    else:
        return {}


def get_function_from(migration_type, connection):

    func_type_mapper_from_postgres = {
        OID: PostgresBlobManager.get_manager(connection).read
    }

    func_type_mapper_from_oracle = {

    }

    if migration_type.startswith('postgresql'):
        return func_type_mapper_from_postgres
    elif migration_type.startswith('oracle'):
        return func_type_mapper_from_oracle
    else:
        return {}



