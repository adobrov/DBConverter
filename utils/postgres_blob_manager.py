class PostgresBlobManager:

    instances = {}

    def __new__(cls, connection):  # doubleton^^
        if cls.instances.get(connection) is None:
            cls.instances[connection] = object.__new__(cls, connection)
        return cls.instances[connection]

    def __init__(self, conn):
        self.conn = conn
        self.pg_conn = self.conn.connection

    def write(self, binary_data):
        lo = self.pg_conn.lobject(mode='w')
        lo.write(str(binary_data))  # maybe NoneType but must be string
        lo.close()
        return lo.oid

    def read(self, oid):
        lo = self.pg_conn.lobject(oid, mode='r')
        res = lo.read()
        lo.close()
        return res

    @classmethod
    def get_manager(cls, connection):
        return cls(connection)
