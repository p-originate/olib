def pivot(rows, column):
    '''Transforms a list of rows into a dictionary. The specified column
    is used for keys. Each row becomes a value. The key should be unique
    within the row list, later rows will overwrite earlier rows with the same
    key.'''
    
    map = {}
    for row in rows:
        map[row[column]] = row
    return map

def pivot_value(rows, column):
    '''Transforms a list of rows into a dictionary. The specified column
    is used for keys. pivot_value assumes that there is a total of two
    columns. The value of the other column becomes the value in the
    dictionary. Later values will overwrite earlier values with the same key.'''
    
    map = {}
    if len(rows) == 0:
        return map
    keys = rows[0].keys()
    value_column = [key for key in keys if key != column][0]
    for row in rows:
        map[row[column]] = row[value_column]
    return map

def pivot_multi(rows, *columns):
    map = {}
    for row in rows:
        this_map = map
        for column in columns[:-1]:
            value = row[column]
            if not this_map.has_key(value):
                this_map[value] = {}
            this_map = this_map[value]
        this_map[row[columns[-1]]] = row
    return map

def pivot_lists(rows, column):
    map = {}
    for row in rows:
        key = row[column]
        mapped_row = map.get(key)
        if mapped_row is None:
            mapped_row = []
            map[key] = mapped_row
        mapped_row.append(row)
    return map

def split_row_map(row, map):
    if len(map) == 0:
        return (row, {})
    base = {}
    parts = {}
    for prefix in map:
        if map[prefix] is not None:
            parts[map[prefix]] = {}
    for key in row:
        found = False
        try:
            value = row[key]
        except IndexError, e:
            raise IndexError, '%s: %s' % (e.message, key)
        for prefix in map:
            if key.startswith(prefix):
                adjusted_key = key[len(prefix):]
                if map[prefix] is None:
                    base[adjusted_key] = value
                    found = True
                else:
                    parts[map[prefix]][adjusted_key] = value
                    found = True
                    break
        if not found:
            base[key] = value
    return (base, parts)

def split_row(row, *prefixes):
    map = {}
    for prefix in prefixes[1:]:
        map[prefix + '_'] = prefix
    if len(prefixes) > 0:
        map[prefixes[0] + '_'] = None
    return split_row_map(row, map)

class PropertyDict(object):
    def __init__(self, attrs):
        self.attrs = attrs
    
    def __getattr__(self, attr):
        try:
            return self.attrs[attr]
        except KeyError:
            raise AttributeError, 'No such attribute: %s' % attr
    
    def __repr__(self):
        return 'PropertyDict(%s)' % repr(self.attrs)
    
    def dict(self):
        return self.attrs

def munge_row(row, *prefixes):
    map = {}
    for prefix in prefixes:
        map[prefix + '_'] = prefix
    return munge_row_map(row, map)

def munge_row_dot(row, *prefixes):
    map = {}
    for prefix in prefixes:
        map[prefix + 's.'] = prefix
    return munge_row_map(row, map)

def munge_row_map(row, map):
    base, parts = split_row_map(row, map)
    for key in map:
        mapped_value = map[key]
        if mapped_value is not None:
            value = parts[map[key]]
            value = PropertyDict(value)
            base[map[key]] = value
    return PropertyDict(base)

import re

def _munge_sql(sql):
    regexp = re.compile(r'^(\s*select\s+)(.+?)(\sfrom\s+(\w+)(?:.+)?)$', re.S + re.I)
    match = regexp.match(sql)
    if not match:
        raise ValueError, 'Sql did not match regexp'
    
    preamble = match.group(1)
    selects = match.group(2)
    postamble = match.group(3)
    first_table = match.group(4)
    
    tables = {}
    def replacer(match):
        table = match.group(2)
        if table + 's' == first_table:
            mapped_value = None
        else:
            mapped_value = table
        tables[table + '_'] = mapped_value
        return match.group(1) + ' as ' + match.group(2) + '_' + match.group(3) + match.group(4)
    
    selects = re.sub(r'\b((\w+)s\.(\w+))(,|\s*$)', replacer, selects)
    
    return (preamble + selects + postamble, tables)

_munge_row = munge_row_map
