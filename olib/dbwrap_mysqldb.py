from . import dbwrap

class CursorWrapper(dbwrap.CursorWrapper):
    def list_tables(self):
        return self.all_values('''
            show tables
        ''')
