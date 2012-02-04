import json, logging, time
from pyramid import threadlocal
from boto import connect_dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from boto.exception import DynamoDBResponseError
from boto.dynamodb.schema import Schema
from boto.dynamodb.batch import BatchList
from boto.dynamodb.item import Item

connection = None
logger = logging.getLogger(__name__)

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


class ValidationError(Exception):
    pass


class Field(object):
    def __init__(self, **options):
        self.options = options
        self.name = None
    
    def __get__(self, obj, type=None):
        c = obj._property_cache
        if self.name not in c:
            c[self.name] = self.to_python(obj._item.get(self.name, None))
        return c[self.name]

    def __set__(self, obj, value):
        c = obj._property_cache
        c[self.name] = value
        cleaner = getattr(obj, 'clean_' + self.name, lambda val: (val, None))
        # clean it
        value, error = cleaner(value)
        if error is not None:
            raise ValidationError(error)
        # validate it
        self.validate(value)
        # convert it
        value = self.from_python(value)
        # see if it's actually any different and set it
        old_value = obj._item.get(self.name, None)
        self.do_set(obj, old_value, value)
    
    def do_set(self, obj, old_value, value, set_dirty=True):
        if old_value != value:
            if obj._exists:
                if old is None:
                    obj._item.add_attribute(self.name, value)
                else:
                    obj._item.put_attribute(self.name, value)
            obj._item[self.name] = value
            obj._property_cache[self.name] = value
            if set_dirty:
                obj._dirty = True 

    def __delete__(self, obj):
        have_value = obj._item.get(self.name, None) != None
        if have_value:
            if obj._exists:
                obj._item.delete_attribute(self.name)
            else:
                del obj._item[self.name]
            obj._dirty = True
        if self.name in obj._property_cache:
            del obj._property_cache[self.name]
    
    def contribute_to_class(self, klass):
        pass
    
    def to_python(self, value):
        return value
    
    def from_python(self, value):
        return value
    
    def validate(self, value):
        return None


class StringField(Field):
    proto_val = ''
    proto = str

    def validate(self, value):
        if not isinstance(value, basestring):
            raise ValidationError("An instance of str is required.")


class NumberField(Field):
    proto_val = 1
    proto = int

    def validate(self, value):
        if not isinstance(value, int):
            raise ValidationError("An instance of int is required")


class ObjectField(StringField):
    def to_python(self, value):
        if value is None:
            return value
        return json.loads(value)
    
    def from_python(self, value):
        return json.dumps(value)
    
    def validate(self, value):
        pass # pretty much anything JSON-able is allowed here


class DefaultObjectField(ObjectField):
    object_proto = None
    object_types = None

    def __get__(self, obj, type=None):
        r = super(DefaultObjectField, self).__get__(obj, type=type)
        if r is not None:
            return r
        d = obj._property_cache
        d[self.name] = self.object_proto()
        return d[self.name]
    
    def validate(self, value):
        if not isinstance(value, self.object_types):
            raise ValidationError("An instance of one of %r is required" % (
                                  self.object_types,))


class SetField(Field):
    """
    A `set` field. Only works with mutable sets.
    """
    object_proto = set
    object_types = (set,)

    # __get__ = DefaultObjectField.__get__
    # validate = DefaultObjectField.validate

    def to_python(self, value):
        v = super(SetField, self).to_python(value)
        if v is None:
            return v
        return set(v)
    
    def from_python(self, value):
        # convert it to a list, because `set`s don't serialize to JSON
        sup = super(SetField, self).from_python
        if value is None:
            return sup(value)
        return sup(list(value))
    
    def __get__(self, obj, type=None):
        r = super(SetField, self).__get__(obj, type=type)
        if r is None:
            r = obj._property_cache[self.name] = set()
        return r
    
    def validate(self, value):
        if not isinstance(value, set):
            raise ValidationError("An instance of set is required.")
    
    def contribute_to_class(self, klass):
        super(SetField, self).contribute_to_class(klass)

        def _check(instance, save, force):
            if force:
                return
            if not instance._exists:
                raise ValueError('Can only add/remove from a set on a '
                                 'PersistentObject if it already exists in the '
                                 'data store. Assign a set to the attribute like '
                                 'you would normally unless it has been created.')
            if save is True and instance._dirty:
                raise ValueError('Can not add/remove from a set on a '
                                 'PersistentObject while other attributes have '
                                 'pending changes. Changing set contents this '
                                 'way performs a write to DynamoDB thus flushing '
                                 'all modified attributes. Pasee force=True '
                                 'to acknowledge that you know this operation '
                                 'performs a write.')
        
        def do_update(instance, val):
            instance._item[self.name] = val
            instance._property_cache[self.name] = val
        
        def add_to_set(instance, items, save=True, force=False):
            """
            Adds the provided items to the SetItem attribute.

            NOTE:
                THIS PERFORMS A WRITE
            
            :type items: set|list
            :param items: The items to add to this set.
            """
            _check(instance, save, force)
            old_value = getattr(instance, self.name)
            items = set(items)
            instance._item.add_attribute(self.name, items)
            instance._dirty = True
            try:
                if save:
                    instance.save()
            except:
                raise
            else:
                old_value.update(items)
                do_update(instance, old_value)
            finally:
                instance._dirty = False
        
        def remove_from_set(instance, items, save=True, force=False):
            """
            Remove the provided items from the SetItem attribute.

            NOTE:
                THIS PERFORMS A WRITE. You can only run this operation if there
                are no pending attribute changes for this object.
            
            :type items: set|list
            :param items: The items to remove from this set.
            """
            _check(instance, save, force)
            old_value = getattr(instance, self.name)
            items = set(items)
            instance._item.delete_attribute(self.name, items)
            instance._dirty = True
            try:
                if save:
                    instance.save()
            except:
                raise
            else:
                old_value.difference_update(items)
                do_update(instance, old_value)
            finally:
                instance._dirty = False
        
        setattr(klass, 'add_to_%s_set' % self.name, add_to_set)
        setattr(klass, 'remove_from_%s_set' % self.name, remove_from_set)


class ListField(DefaultObjectField):
    object_proto = list
    object_types = (list,)


class DictField(DefaultObjectField):
    object_proto = dict
    object_types = (dict,)


class PersistedObjectMeta(type):
    def __init__(cls, name, bases, classdict):
        new_values = {}
        _props = []
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
                    _props.append(k)
            if not found_hash_key:
                raise TypeError('At least one field must be marked hash_key=True'
                                ' for class "%s"' % (name,))
        
        type.__init__(cls, name, bases, classdict)

        new_values['_properties'] = _props
        for k, v in new_values.iteritems():
            setattr(cls, k, v)
        
        for prop in _props:
            classdict[prop].contribute_to_class(cls)


class PersistedObject(object):
    __table_name__ = None
    __read_units__ = 8
    __write_units__ = 8

    _hash_key_name = None
    _hash_key_proto = None
    _hash_key_proto_val = None
    _range_key_name = None
    _range_key_proto = None
    _range_key_proto_val = None
    _schema = None
    _table = None
    _properties = None

    __metaclass__ = PersistedObjectMeta

    # TABLE MANIPULATION

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
    def create_table(cls):
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
    def drop_table(cls):
        cls._load_meta()
        cls._table.delete()
    
    @classmethod
    def reset_table(cls):
        cls._load_meta()
        cls.drop_table()
        conn = get_connection()
        for i in xrange(30): # try/wait up to 30 sec for the table to be deleted
            try:
                cls._table.update_from_response(conn.describe_table(cls._table.name))
            except DynamoDBResponseError, e:
                if e.data['__type'].endswith('ResourceNotFoundException'):
                    break
            sleep(1)
        else:
            raise Exception('Tried to delete the table but DynamoDB took too '
                            'long to respond.')
        cls.create_table()
    
    # ITEM MANIPULATION

    @classmethod
    def get(cls, k):
        cls._load_meta()
        try:
            r = None
            t1 = time.time()
            try:
                r = cls._table.get_item(cls._hash_key_proto(k))
            finally:
                logger.info('Got %d %s in %s' % (0 if r is None else 1, 
                                                 cls.__name__, time.time() - t1))
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
    
    @classmethod
    def get_or_create(cls, d=None, **other):
        if d is None:
            d = {}
        d.update(other)
        k = cls._hash_key_proto(d[cls._hash_key_name])
        try:
            ret = cls.get(k)
        except NotFoundError:
            ret = cls.create(d)
        return ret
    
    @classmethod
    def get_many(cls, keys):
        """
        Returns a list of :class:`PersistedObject` identical in length to the
        list of keys provided. If a key could not be found, it's slot will be 
        `None`

        :type keys: list
        :param keys: A list of keys
        """
        cls._load_meta()
        t1 = time.time()
        # get the items
        items, unprocessed = cls._fetch_batch_queue(cls._get_batch_queue(keys))
        # if there are unprocessed items, create a batch from them
        if len(unprocessed):
            unprocessed_queue = cls._get_batch_queue(unprocessed)
        else:
            unprocessed_queue = []
        # and continue fetching unprocessed items until there are no more
        while len(unprocessed_queue):
            new_items, new_unprocessed = cls._fetch_batch_queue(unprocessed_queue)
            items.extend(new_items)
            if len(new_unprocessed):
                unprocessed_queue = cls._get_batch_queue(new_unprocessed)
            else:
                unprocessed_queue = []
        # create a hash out of the values' keys for quick reordering
        h = dict((item[cls._hash_key_name], idx) for idx, item in enumerate(items))
        ret = []
        for key in keys:
            if key in h:
                ret.append(cls(Item(cls._table, key, None, items[h[key]])))
            else:
                ret.append(None)
        logger.info('Got %i of %s in %s' % (len(items), cls.__name__, 
                                            time.time() - t1))
        return ret
    
    @classmethod
    def _fetch_batch_queue(cls, batch_queue):
        results = []
        unprocessed = []
        while len(batch_queue):
            batch_keys = batch_queue.pop()
            batch = BatchList(get_connection())
            batch.add_batch(cls._table, [cls._hash_key_proto(k) 
                                         for k in batch_keys])
            try:
                batch_ret = batch.submit()
            except DynamoDBKeyNotFoundError:
                continue
            # import pprint
            # pprint.pprint(batch_ret)
            if ('UnprocessedKeys' in batch_ret and cls._full_table_name 
                    in batch_ret['UnprocessedKeys']):
                u = batch_ret['UnprocessedKeys'][cls._full_table_name]
                u = [k['HashKeyElement'] for k in u['Keys']]
                unprocessed.extend(u)
            if ('Responses' in batch_ret and cls._full_table_name 
                    in batch_ret['Responses']):
                results.extend(
                    batch_ret['Responses'][cls._full_table_name]['Items'])
        return results, unprocessed
    
    @classmethod
    def _get_batch_queue(cls, keys):
        num_batches, last_batch_size = divmod(len(keys), 100)
        batches = []
        for i in xrange(num_batches+1):
            batches.append(keys[i*100:(i+1)*100])
        return batches
    
    @classmethod
    def get_or_create_many(cls, dicts):
        """
        Does the same as get_or_create but on a collection of dictionaries
        instead. Returns a list of :class:`PersistedObject` the same as 
        :meth:`get_many` except that the None slots (non-existing entries) are
        filled in.

        Note that the :class:`PersistedObject`s, whether they are created or not
        are not automatically persisted to DynamoDB. You must call :meth:`save`
        on the instances.

        :type dicts: list
        :param dicts: A list of dictionaries same as provided to `get_or_create`
        """
        keys = [item[cls._hash_key_name] for item in dicts]
        ret = cls.get_many(keys)
        create = []
        for i, item in enumerate(ret):
            if item is None:
                create.append((i, cls.create(dicts[i])))
        for idx, item in create:
            ret[idx] = item
        return ret
    
    def __new__(cls, *args, **kwargs):
        cls._load_meta()
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, item, is_new=False):
        self._dirty = is_new
        self._item = item
        self._exists = not is_new
        self._property_cache = {}
    
    def __unicode__(self):
        cls = self.__class__
        return u'<%s %s=%r>' % (cls.__name__, cls._hash_key_name, 
                                getattr(self, cls._hash_key_name))
    
    def __str__(self):
        return str(self.__unicode__())
    
    def __repr__(self):
        return self.__str__()
    
    def to_dict(self):
        return {n: getattr(self, n) for n in self._properties}
    
    def verbose_string(self):
        return '<%s %s>' % (self.__class__.__name__, 
                            ' '.join(['='.join(list(map(str, p))) 
                                      for p in self.to_dict().iteritems()]))

    def save(self):
        if self._dirty:
            t1 = time.time()
            ret = {'ConsumedCapacityUnits': 0}
            try:
                if self._exists:
                    ret = self._item.save()
                else:
                    ret = self._item.put()
                self._dirty = False
            finally:
                logger.info('Saved 1 %s in %s ConsumedCapacityUnits=%f' % (
                                self.__class__.__name__, time.time() - t1,
                                ret['ConsumedCapacityUnits']))
        return self
    
    def update(self, d):
        for k, v in d.iteritems():
            setattr(self, k, v)
        return self
    
    def delete(self):
        raise NotImplementedError

