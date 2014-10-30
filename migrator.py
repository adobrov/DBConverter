from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine.reflection import Inspector

from utils.processors import PostSelectDataProcessor, PreInsertDataProcessor
from utils.mapper_type_functions import *

ALLOWED_MIGRATIONS = ['postgresql_to_oracle', 'postgresql_to_postgresql', 'oracle_to_oracle', 'oracle_to_postgresql']


class DBAdapter:
    def __init__(self, src_engine, dst_engine):
        self.src_engine = src_engine
        self.dst_engine = dst_engine
        self.dst_inspector = Inspector.from_engine(dst_engine)
        self.src_metadata = MetaData(bind=self.src_engine, reflect=True)
        self.dst_metadata = MetaData(bind=self.dst_engine)  #, reflect=True)
        self.tables_for_copy = self.src_metadata.tables

    def make_create(self, tbl):
        #self.tables_for_copy[tbl[0]].tometadata(self.dst_metadata)
        #print self.db_adapter.dst_metadata.tables
        return self.tables_for_copy[tbl[0]].tometadata(self.dst_metadata)

    def make_select(self, tbl):
        return select([self.tables_for_copy[tbl[0]]])

    @staticmethod
    def make_insert(tbl, row):
        return tbl.insert().values(row)


class BaseMigrator:
    def __init__(self, db_adapter):
        self.db_adapter = db_adapter
        self.scr_connection = db_adapter.src_engine.connect()
        self.dst_connection = db_adapter.dst_engine.connect()
        self.func_type_mapper_from = {}
        self.func_type_mapper_to = {}

    def _finish(self):
        self.scr_connection.close()
        self.dst_connection.close()

    def start(self, batch_size):
        try:
            self.process_create_tables()
            self.process_insert_data(batch_size)
        finally:
            self._finish()

    def process_create_tables(self):
        from visitors import visit_prevent_auto_create_fk
        for table in self.db_adapter.tables_for_copy.items():
            if table[0] in self.db_adapter.dst_inspector.get_table_names():
                print 'Table %s already exists' % table[0]
            else:
                print 'Create table %s' % table[0]
                tbl = self.db_adapter.make_create(table)
                tbl.create()
        self.db_adapter.dst_metadata.reflect()

    def process_insert_data(self, batch_size):
        for src_table in self.db_adapter.tables_for_copy.items():
            count = 0
            select_statement = self.db_adapter.make_select(src_table)
            data = select_statement.execute()
            dst_table = self.db_adapter.dst_metadata.tables[src_table[0]]
            print 'Start insert into %s' % dst_table.name
            result_proxy = PostSelectDataProcessor(src_table[1], self.func_type_mapper_from, data, batch_size)
            for batch in result_proxy:
                if len(batch) == 0:
                    break
                transaction = self.dst_connection.begin()
                for row in batch:
                    row_for_insert = PreInsertDataProcessor(dst_table, self.func_type_mapper_to, row)
                    insert_statement = self.db_adapter.make_insert(dst_table, row_for_insert.process_row())
                    try:
                        self.dst_connection.execute(insert_statement)
                        count += 1
                    except IntegrityError:
                        continue
                print 'Batch commited (%s rows processed)' % count
                transaction.commit()
            print 'Insert into %s finished, %s rows was processing' % (dst_table.name, count)


class Migrator:
    def __init__(self, db_adapter):
        self.migrator = BaseMigrator(db_adapter)
        self.migration_type = self._get_migration_type()
        self.migrator.func_type_mapper_from = get_function_from(self.migration_type, self.migrator.scr_connection)
        self.migrator.func_type_mapper_to = get_function_to(self.migration_type, self.migrator.dst_connection)

    def _get_migration_type(self):
        from_ = self.migrator.db_adapter.src_engine.dialect.name
        to_ = self.migrator.db_adapter.dst_engine.dialect.name
        mt = '%s_to_%s' % (from_, to_)
        if mt not in ALLOWED_MIGRATIONS:
            raise TypeError('Unsupported migration type %s' % mt)
        return mt

    def migrate(self, batch_size):
        if self.migration_type == 'postgresql_to_oracle':
            from visitors import visit_from_postres_types
        elif self.migration_type == 'oracle_to_postgresql':
            from visitors import visit_from_oracle_types
        else:
            pass
        self.migrator.start(batch_size)

if __name__ == '__main__':
    dst = create_engine('engine://login:password@address:port/dbname')
    src = create_engine('engine://login:password@address:port/dbname')
    adapter = DBAdapter(src, dst)
    migrator = Migrator(adapter)
    print migrator.migration_type
    migrator.migrate(1)




