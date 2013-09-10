import psycopg2, psycopg2.extras, psycopg2.extensions
from . import dbwrap

# Receive strings from the database in unicode
# http://initd.org/psycopg/docs/usage.html#unicode-handling
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

class ExpressionValue(str):
    pass

class ExpressionValueAdapter(object):
    def __init__(self, value):
        self.value = value
    
    def getquoted(self):
        return str(self.value)

psycopg2.extensions.register_adapter(ExpressionValue, ExpressionValueAdapter)

# Quoting of table/schema names in psycopg2:
# http://osdir.com/ml/python.db.psycopg.devel/2004-10/msg00012.html
class SchemaName(str):
    pass

class SchemaNameAdapter(object):
    def __init__(self, value):
        self.value = value
    
    def getquoted(self):
        return '"' + self.value.replace('"', '""') + '"'

psycopg2.extensions.register_adapter(SchemaName, SchemaNameAdapter)

# Use this for obtaining original psycopg2 tuple/list escaping behavior
class SqlArray(list):
    pass

#psycopg2.extensions.register_adapter(list, psycopg2.extensions.SQL_IN)
psycopg2.extensions.register_adapter(SqlArray, psycopg2._psycopg.List)

class CursorWrapper(dbwrap.CursorWrapper):
    def do_execute(self, sql, args):
        try:
            self.cursor.execute(sql, args)
        except psycopg2.OperationalError, e:
            if str(e).startswith('server closed the connection unexpectedly'):
                if self.conn._transaction_depth == 0:
                    self.conn.reconnect()
                    self.cursor = self.conn.cursor()
                    self.cursor.execute(sql, args)
                else:
                    self.conn.want_reconnect = True
                    raise dbwrap.DatabaseConnectionClosed
            else:
                raise
