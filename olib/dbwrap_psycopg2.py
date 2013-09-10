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
    
    # DDL statements
    
    def add_fkey(self, table, column, target_table=None, target_column=None):
        target_table = column[:-3] + 's'
        target_column = 'id'
        name = '%s_%s_fk' % (table, column)
        vars = {
            'table': table,
            'column': column,
            'name': name,
            'target_table': target_table,
            'target_column': target_column,
        }
        self.execute('''
            alter table %(table)s add constraint %(name)s
                foreign key (%(column)s)
                references %(target_table)s (%(target_column)s)
        ''' % vars)
    
    def list_tables(self):
        return self.all_values('''
            select relname from pg_class
            where relkind='r' and
                relname not like %s
                and relname not like %s
        ''', 'pg_%', 'sql_%')
    
    def list_sequences(self):
        return self.all_values('''
            select relname from pg_class
            where relkind='S' and
                relname not like %s
                and relname not like %s
        ''', 'pg_%', 'sql_%')
    
    def list_functions(self):
        public_namespace = self.one_value_check('''
            select oid from pg_namespace where nspname=?
        ''', 'public')
        plpgsql_language = self.one_value_check('''
            select oid from pg_language where lanname=?
        ''', 'plpgsql')
        return self.all_values('''
            select proname from pg_proc
            where pronamespace=? and prolang=?
        ''', public_namespace, plpgsql_language)
