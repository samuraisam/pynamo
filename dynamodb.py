from pyramid import threadlocal
from boto import connect_dynamodb

connection = None

def get_connection():
    s = threadlocal.get_current_registry().settings
    if connection is None:
        global connection # these connections are thread safe
        connection = connect_dynamodb(
            aws_access_key_id=s['aws_access_key_id'],
            aws_secret_access_key=s['aws_secret_access_key'])
    return connection

def get_table_prefix():
    return threadlocal.get_current_registry().settings['dynamo_table_prefix']

def get_table(name):
    return get_connection().get_table(get_table_prefix() + name)

def create_table(name, schema, read_units=10, write_units=10):
    conn = get_connection()
    schema = conn.create_schema(**schema)
    return conn.create_table(name=get_table_prefix() + name, schema=schema, 
                             read_units=read_units, write_units=write_units)

def get_item(table_name, key):
    return get_table().get_item(hash_key=key)

def create_item(table_name, hash_key, attributes, range_key=None):
    table = get_table(table_name)
    args = {'hash_key': attributes[hash_key], 'attrs': attributes}
    if range_key:
        args['range_key'] = attributes[range_key]
    item = table.new_item(**args)
    item.put()
    return item

def update_item(table_name, key, attributes):
    item = get_item(table_name, key)
    item.update(attributes)
    item.put()
    return item


class NotFoundError(Exception):
    pass


class PersistedObject(object):
    table_name = None
    hash_key = (None, None) # (keyname, proto)
    range_key = (None, None) # (keyname, proto)
    read_units = 10
    write_units = 10
        
    @classmethod
    def get(cls, k):
        r = get_item(cls.table_name, cls.hash_key[1](k))
        if not r:
            raise NotFoundError()
        return cls(r)
    
    @classmethod
    def create(cls, k, d=None):
        table = get_table(cls.table_name)
        if d is None:
            d = {}
        args = {'hash_key': cls.hash_key[0], 'attrs': d}
        if cls.range_key[0]:
            args['range_key'] = cls.range_key[0]
        return cls(table.new_item(**args), is_new=True)

    def __init__(self, item, is_new=False):
        self._dirty = is_new
        self._item = item
        self._exists = not is_new
    
    def __getattribute__(self, name):
        if name in self._item:
            return self._item[name]
        return super(PersistedObject, self).__getattribute__(name)
    
    def __setattribute__(self, name, value):
        if name in self._item:
            self._item[name] = value
            self._dirty = True
        else:
            super(PersistedObject, self).__getattribute__(name)
    
    def save(self):
        if self._dirty:
            self._item.put()
    
    def update(self, d, save=False):
        for k, v in d.iteritems():
            setattr(self, k, v)
        if save:
            self.save()
    
    def delete(self):
        raise NotImplementedError

