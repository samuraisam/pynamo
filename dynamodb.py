from pyramid import threadlocal
from boto import connect_dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from boto.dynamodb.schema import Schema

connection = None

def get_connection():
    s = threadlocal.get_current_registry().settings
    global connection # these connections are thread safe
    if connection is None:
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
    return conn.create_table(name=get_table_prefix() + name, schema=schema, 
                             read_units=read_units, write_units=write_units)

def get_item(table_name, key):
    return get_table(table_name).get_item(hash_key=key)

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

class MutationError(Exception):
    pass

class ValidationError(Exception):
    pass


class Field(object):
    def __init__(self, **options):
        self.options = options
        self.name = None
        self._cached_value = None
    
    def __get__(self, obj, type=None):
        if self._cached_value is None:
            self._cached_value = self.to_python(obj._item.get(self.name, None))
        return self._cached_value

    def __set__(self, obj, value):
        self._cached_value = value
        cleaner = getattr(obj, 'clean_' + self.name, lambda val: True)

        # validate it
        value, error = cleaner(value)
        if error is not None:
            raise ValidationError(error)

        # convert it
        value = self.from_python(value)

        # see if it's actually any different and set it
        old_value = obj._item.get(self.name, None)
        if old_value != value:
            obj._item[self.name] = value
            obj._dirty = True 

    def __delete__(self, obj):
        have_value = obj._item.get(self.name, None) != None
        if have_value:
            del obj._item[self.name]
            obj._dirty = True
        self._cached_value = None
    
    def to_python(self, value):
        return value
    
    def from_python(self, value):
        return value


class StringField(Field):
    proto_val = ''
    proto = str


class NumberField(Field):
    proto_val = 1
    proto = int


class PersistedObjectMeta(type):
    def __init__(self, name, bases, classdict):
        new_values = {}
        try:
            # hax, if building PersistedObject this will throw a NameError
            in_base = (PersistedObject,)
        except NameError:
            pass
        else:
            found_hash_key = False
            for k, v in classdict.iteritems():
                if isinstance(v, Field):
                    # set hash key
                    if v.options.get('hash_key', False) == True:
                        new_values['_hash_key_name'] = k
                        new_values['_hash_key_proto'] = v.proto
                        new_values['_hash_key_proto_val'] = v.proto_val
                        found_hash_key = True
                    # set range key
                    if v.options.get('range_key', False) == True:
                        new_values['_range_key_name'] = k
                        new_values['_range_key_proto'] = v.proto
                        new_values['_range_key_proto_val'] = v.proto_val
                    v.name = k
            if not found_hash_key:
                raise TypeError('At least one field must be marked hash_key=True'
                                ' for class "%s"' % (name,))
        type.__init__(self, name, bases, classdict)
        for k, v in new_values.iteritems():
            setattr(self, k, v)


class PersistedObject(object):
    __table_name__ = None
    __read_units__ = 10
    __write_units__ = 10

    _hash_key_name = None
    _hash_key_proto = None
    _hash_key_proto_val = None
    _range_key_name = None
    _range_key_proto = None
    _range_key_proto_val = None
    _schema = None
    _table = None

    __metaclass__ = PersistedObjectMeta

    @classmethod
    def _load_meta(cls):
        connection = get_connection()
        if cls._table is not None:
            return
        # get the full table name
        cls._full_table_name = get_table_prefix() + cls.__table_name__
        # get the table
        cls._table = connection.get_table(cls._full_table_name)
    
    @classmethod
    def _create_table(cls):
        # create the schema
        connection = get_connection()
        cls._schema = connection.create_schema(
            hash_key_name = cls._hash_key_name,
            hash_key_proto_value = cls._hash_key_proto_val,
            range_key_name = cls._range_key_name,
            range_key_proto_value = cls._range_key_proto_val)
        # get the table 
        cls._table = connection.create_table(
            name = cls._full_table_name,
            schema = cls._schema,
            read_units = cls.__read_units__,
            write_units = cls.__write_units__)

    @classmethod
    def get(cls, k):
        cls._load_meta()
        try:
            r = cls._table.get_item(cls._hash_key_proto(k))
            if not r:
                raise NotFoundError()
        except DynamoDBKeyNotFoundError:
            raise NotFoundError()
        return cls(r)
    
    @classmethod
    def create(cls, d=None, **other):
        cls._load_meta()
        if d is None:
            d = {}
        d.update(other)
        if cls._hash_key_name not in d:
            raise TypeError('creation attributes must at least contain the '
                            'chosen hash_key: ' + cls._hash_key_name)
        
        # create the underlying boto.dynamodb.item.Item
        args = {'hash_key': cls._hash_key_name, 
                'attrs': {cls._hash_key_name: cls._hash_key_proto(
                                d[cls._hash_key_name])}}
        if cls._range_key_name:
            args['range_key'] = cls._range_key_name
            args['attrs'][cls._range_key_name] = \
                cls._range_key_proto(d[cls._range_key_name])
        
        # build the object
        ret = cls(cls._table.new_item(**args), is_new=True)
        ignore = (cls._hash_key_name, cls._range_key_name)
        for k, v in d.iteritems():
            if k in ignore:
                continue
            setattr(ret, k, v)
        
        return ret
    
    def __new__(cls, *args, **kwargs):
        cls._load_meta()
        return object.__new__(cls, *args, **kwargs)
        # return super(PersistedObject, cls).__new__(*args, **kwargs)

    def __init__(self, item, is_new=False):
        self._dirty = is_new
        self._item = item
        self._exists = not is_new
    
    def to_dict(self):
        return dict(self._item)
    
    def save(self):
        if self._dirty:
            self._item.put()
        return self
    
    def update(self, d, save=False):
        for k, v in d.iteritems():
            setattr(self, k, v)
        if save:
            self.save()
        return self
    
    def delete(self):
        raise NotImplementedError

