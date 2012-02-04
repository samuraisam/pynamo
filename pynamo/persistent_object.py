import json, logging, time, string
from boto import connect_dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from boto.exception import DynamoDBResponseError
from boto.dynamodb.schema import Schema
from boto.dynamodb.batch import BatchList
from boto.dynamodb.item import Item

# connection = None
logger = logging.getLogger(__name__)

# def get_connection():
#     s = threadlocal.get_current_registry().settings
#     global connection # these connections are thread safe
#     if connection is None:
#         connection = connect_dynamodb(
#             aws_access_key_id=s['aws_access_key_id'],
#             aws_secret_access_key=s['aws_secret_access_key'])
#     return connection

# def get_table_prefix():
#     return threadlocal.get_current_registry().settings['dynamo_table_prefix']

# def get_table(name):
#     return get_connection().get_table(get_table_prefix() + name)

# def create_table(name, schema, read_units=10, write_units=10):
#     conn = get_connection()
#     return conn.create_table(name=get_table_prefix() + name, schema=schema, 
#                              read_units=read_units, write_units=write_units)

# def get_item(table_name, key):
#     return get_table(table_name).get_item(hash_key=key)

# def create_item(table_name, hash_key, attributes, range_key=None):
#     table = get_table(table_name)
#     args = {'hash_key': attributes[hash_key], 'attrs': attributes}
#     if range_key:
#         args['range_key'] = attributes[range_key]
#     item = table.new_item(**args)
#     item.put()
#     return item

# def update_item(table_name, key, attributes):
#     item = get_item(table_name, key)
#     item.update(attributes)
#     item.put()
#     return item


class NotFoundError(Exception):
    """
    Thrown during :meth:`get` when the requested object could not be found.
    """
    pass


class ValidationError(Exception):
    """
    Thrown by :class:`Field` subclasses when setting attributes that are 
    invalid.
    """
    pass


class Meta(object):
    """
    A piece of metadata attached to :class:`PersistentObject` subclasses. All
    it really does is define an attribute in the same name with two underscores
    at the beginning and end of the attribute.

    e.g.::
        class User(PersistentObject):
            table_name = Meta('test_users')
    
    becomes::
        class User(PersistentObject):
            __table_name__ = 'test_users'
    
    However it does provide some convenience to the metaclass which reads
    metadata for the individual classes.

    Currently supported property names are:

      * `table_name` - which table will be used to back this 
        :class:`PersistentObject`
      * `read_units` - how many read units does this table have provisioned
      * `write_units` - how many write units are provisioned for this table
      * `key_format` - if a compound key is used, what is it's format? The 
        attribute names are parsed from the format.
    """
    def __init__(self, *a, **kw):
        if len(a) == 1:
            self.value = a[0]
        elif len(kw):
            self.value = kw
        else:
            raise ValueError('Meta must either be a single value or keyword '
                             'arguments')


class Field(object):
    def __init__(self, **options):
        if self.__class__ == Field:
            raise TypeError('Field is a baseclass. Instantiate one of the '
                            'Field subclasses instead.')
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


# NATIVE TYPES
# These are fields that DynamoDB understands directly


class StringField(Field):
    """
    A string field. For strings, like `unicode` and `str`
    """
    proto_val = ''
    proto = str

    def validate(self, value):
        if not isinstance(value, basestring):
            raise ValidationError("An instance of str is required.")


class IntegerField(Field):
    """
    Numbers. `long`s and `int`s only please.
    """
    proto_val = 1
    proto = int

    def validate(self, value):
        if not isinstance(value, (int, long)):
            raise ValidationError("An instance of int is required")


class FloatField(self, value):
    proto_val = 1.0
    proto = float

    def validate(self, value):
        if not isinstance(value, float):
            raise ValidationError("An instance of int is required")

    def to_python(self, value):
        return float(value)


class BoolField(Field):
    proto_val = False
    proto = bool

    def validate(self, value):
        if not isinstance(value, bool):
            raise ValidationError("An instance of int is required")

    def to_python(self, value):
        return bool(value)


class SetField(Field):
    """
    A `set` field. Sets are native data types in DynamoDB as long as their
    values contain exclusively either numbers or strings. This type
    allows us to work with sets in python and have the functionality mirrored
    in DynamoDB.

    Adds two methods to the class:
        `add_to_{FIELDNAME}_set` which adds objects to the set and saves. For 
        more detailed information call `help(obj.add_to_{FIELDNAME}_set)`

        `remove_from_{FIELDNAME}_set` which removes objects from the set and 
        saves. For more detailed information call 
        `help(obj.remove_from_{FIELDNAME}_set)`
    """
    proto = set
    proto_val = None
    object_proto = set
    object_types = (set,)

    def __init__(self, *a, **kw):
        if self.__class__ == SetField:
            raise TypeError('SetField is a base class. Use StringSetField or '
                            'NumberSetField')
        super(SetField, self).__init__(*a, **kw)

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
                raise ValueError(
                    'Can only add/remove from a set on a PersistentObject if '
                    'already exists in the data store. Assign a set to the '
                    'attribute like you would normally unless it has been '
                    'created.')
            if save is True and instance._dirty:
                raise ValueError(
                    'Can not add/remove from a set on a PersistentObject while '
                    'other attributes have pending changes. Changing set '
                    'contents this way performs a write to DynamoDB thus '
                    'flushing all modified attributes. Pasee force=True to '
                    'acknowledge that you know this operation performs a '
                    'write.')
        
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


class NumberSetField(SetField):
    """
    A :class:`SetField` which contains only numbers: one of `(bool, int, float, 
    long)` Upon setting or modifying this attribute this class will validate 
    that it's contents contain only those types.
    """
    proto_val = set([1])

    def validate(self, value):
        super(NumberSetField, self).validate()
        valid_values = map(lambda v: isinstance(v, (long, int, bool, float)), 
                           value)
        if False in valid_values:
            raise ValidationError("Invalid value inside NumberSetField - all "
                                  "values must be numbers (int, bool, float)")

class StringSetField(SetField):
    """
    A :class:`SetField` which contains only strings (anything descending from
    `basestring`). Upon setting or modifying this attribute this class will 
    validate that it's contents contain only strings.
    """
    proto_val = set([''])

    def validate(self, value):
        super(StringSetField, self).validate(value)
        if False in map(lambda v: isinstance(v, basestring), value):
            raise ValidationError("Invalid value inside StringSetField - all "
                                  "values must be strings")


# SYNTHESIZED TYPES
# These types are build on top of other native DynamoDB types. Primarily they
# are built by using JSON-serialization.


class ObjectField(StringField):
    """
    A :class:`Field` which will serialize it's contents to and from JSON. This
    allows for [somewhat] arbitrary objects to be saved in an attribute. JSON
    is chosen because it's a safe serialization format and does not pose any
    security risk to Python.
    """
    def to_python(self, value):
        if value is None:
            return value
        return json.loads(value)
    
    def from_python(self, value):
        return json.dumps(value)
    
    def validate(self, value):
        pass # pretty much anything JSON-able is allowed here


class DefaultObjectField(ObjectField):
    """
    A container-like superclass for storing objects of a certain type. Don't
    use this class directly, use either :class:`ListField` or :class:`DictField`
    """
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


class ListField(DefaultObjectField):
    """
    A :class:`Field` which defaults to an empty `list`. All contents must be
    serializable in JSON.
    """
    object_proto = list
    object_types = (list,)


class DictField(DefaultObjectField):
    """
    A :class:`Field` which defaults to an empty `dict`. All contents must be
    serializable in JSON.
    """
    object_proto = dict
    object_types = (dict,)


class PersistedObjectMeta(type):
    def __init__(cls, name, bases, classdict):
        new_values = {}
        _props = []
        _remove_props = []
        _meta = []
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
                if isinstance(v, Meta):
                    _meta.append((k, v))
            if not found_hash_key:
                raise TypeError('At least one field must be marked a hash_key'
                                ' for class "%s"' % (name,))
        
        type.__init__(cls, name, bases, classdict)

        new_values['_properties'] = _props
        for k, v in new_values.iteritems():
            setattr(cls, k, v)
        for prop in _remove_props:
            delattr(cls, prop)
        for prop in _props:
            classdict[prop].contribute_to_class(cls)
        for k, v in _meta:
            setattr(cls, '__%s__' % k, v.value)
        # this shouldn't be here. but it's easier for now
        fmt = getattr(cls, '__hash_key_format__', None)
        if fmt is not None:
            print fmt
            attr_list = []
            pieces = string.Formatter().parse(fmt)
            for literal_text, field_name, format_spec, conversion in pieces:
                attr_list.append(field_name)
            setattr(cls, '__hash_key_attributes__', tuple(attr_list))


class PersistedObject(object):
    __table_name__ = None
    __read_units__ = 8
    __write_units__ = 8
    __key_format__ = None
    __key_attributes__ = None

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
        """
        Create this table in DynamoDB by reading the declaritive configuration
        of this class. The class must have at least one attribute that is 
        configured as `hash_key` and must at least have a single :class:`Meta`
        configured as `table_name`.

        Sends a `CreateTable` operation to DynamoDB and does not wait for it to
        complete fully. The table will be in the `CREATING` state for a 
        (usually) short period afterwards. 
        """
        # create the schema
        connection = get_connection()
        cls._full_table_name = get_table_prefix() + cls.__table_name__
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
        """
        Removes the table and does not wait for DynamoDB to compeltely remove
        it. The table will be in the `DELETING` state for some time afterwards.
        Sends a `DeleteTable` to DynamoDB.
        """
        cls._load_meta()
        cls._table.delete()
    
    @classmethod
    def reset_table(cls):
        """
        Drops the table (it must previously exist) then waits until it's totally
        gone to recreate it. Doesn't wait for recreation to complete. Sends 
        `DeleteTable` followed by `CreateTable` to DynamoDB.
        """
        cls._load_meta()
        cls.drop_table()
        conn = get_connection()
        for i in xrange(30): # try/wait up to 30 sec for the table to be deleted
            try:
                cls._table.update_from_response(conn.describe_table(
                        cls._table.name))
            except DynamoDBResponseError, e:
                if e.data['__type'].endswith('ResourceNotFoundException'):
                    break
            time.sleep(1)
        else:
            raise Exception('Tried to delete the table but DynamoDB took too '
                            'long to respond.')
        cls.create_table()
    
    # ITEM MANIPULATION

    @classmethod
    def prepare_key(cls, key_or_dict):
        """
        Creates a key out of a dictionary. Used internally when using compound
        keys.

        :type key_or_dict: str|dict
        :param key_or_dict: Either an already-configured key, or a dictionary
            from which the key will be computed
        """
        # provided a dict and key_attribute and key_format are filled out
        if (isinstance(key_or_dict, dict) and cls.__hash_key_attributes__ is 
                not None and cls.__hash_key_format__ is not None):
            for k in cls.__hash_key_attributes__:
                if k not in key_or_dict:
                    raise ValueError('Tried to build a key but not all the '
                                     'required attributes were present: ' 
                                    + repr(cls.__hash_key_attributes__))
            ret = cls.__hash_key_format__.format(**key_or_dict)
        # provided a dict, just extract the key from there
        elif isinstance(key_or_dict, dict):
            ret = key_or_dict[cls._hash_key_name]
        # otherwise the key is assumed to be already valid 
        else:
            ret = key_or_dict
        return cls._hash_key_proto(ret)

    @classmethod
    def get(cls, *a, **kw):
        """
        Retrieve an item from DynamoDB. This operation performs a singular 
        `GetItem` operation, costing you a single capacity unit. Raises 
        `class`:NotFoundError if the item could not be found.

        This method can be called multiple ways. If the full key is known, 
        then simply pass it to :meth:`get`. If using compound keys, keyword
        arguments may be used which will then be used to build the key.
        """
        cls._load_meta()
        if len(a) == 1:
            k = cls.prepare_key(a[0])
        elif len(kw) == len(cls.__key_attributes__):
            k = cls.prepare_key(kw)
        else:
            raise ValueError('Either provide a singular key or keyword '
                             'arguments to build the key from the provided '
                             'key_format, in which case it must include all '
                             'the possible attributes.')
        try:
            r = None
            t1 = time.time()
            try:
                r = cls._table.get_item(k)
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
        """
        Creates a new instance of this :class:`PersistentObject` subclass
        populated from keyword arguments or a provided dictionary. If you are
        using compound hash keys, all dependent keys must be present, otherwise
        all that needs to be in provided is the configured hash key.

        NOTE that the returned object is not persisted to the database. You must
        call :meth:`save` on it.

        :type d: dict
        :param d: The attributes from which to build this object.
        """
        cls._load_meta()
        if d is None:
            d = {}
        d.update(other)
        key = None
        if cls._hash_key_name not in d:
            # the hashkey is not present, try building it
            try:
                key = cls.prepare_key(d)
            except ValueError:
                raise ValueError('Creation attrubuts must contain at least the '
                                 'hash key or the hash key attributes')
        else:
            key = d[cls._hash_key_name]
        # create the underlying boto.dynamodb.item.Item
        args = {'hash_key': cls._hash_key_name, 
                'attrs': {cls._hash_key_name: key}}
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
        """
        Retrieves an item fron DynamoDB based on the hash key or compount hash
        key in the provided dictionary. Keyword arguments also work.

        NOTE that the returned object is not persisted to the database if it
        previously did not exist. You must call :meth:`save` on it. Remember
        that this is a no-op if the object previously did not exist.

        :type d: dict
        :param d: A dictionary from which to query or create the object.
        """
        if d is None:
            d = {}
        d.update(other)
        # k = cls._hash_key_proto(d[cls._hash_key_name])
        k = cls.prepare_key(d)
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

        This operation performs `BatchGetItem` on the DynamoDB store. This
        method is typically limited to 100 items. Depending on your configured
        capacity, this can easily outstrip it. This method will retry in a loop
        until all the keys you asked for are satisfied. `keys` is not limited
        to 100 items.

        :type keys: list
        :param keys: A list of keys
        """
        keys = map(cls.prepare_key, keys)
        cls._load_meta()
        t1 = time.time()
        # get the items
        items, unprocessed, consumed_capacity = cls._fetch_batch_queue(
                cls._get_batch_queue(keys))
        # if there are unprocessed items, create a batch from them
        if len(unprocessed):
            unprocessed_queue = cls._get_batch_queue(unprocessed)
        else:
            unprocessed_queue = []
        # and continue fetching unprocessed items until there are no more
        while len(unprocessed_queue):
            new_items, new_unprocessed, new_consumed = cls._fetch_batch_queue(
                    unprocessed_queue)
            consumed_capacity += new_consumed
            items.extend(new_items)
            if len(new_unprocessed):
                unprocessed_queue = cls._get_batch_queue(new_unprocessed)
            else:
                unprocessed_queue = []
        # create a hash out of the values' keys for quick reordering
        h = dict((item[cls._hash_key_name], idx) 
                    for idx, item in enumerate(items))
        ret = []
        for key in keys:
            if key in h:
                ret.append(cls(Item(cls._table, key, None, items[h[key]])))
            else:
                ret.append(None)
        logger.info('Got %i of %s in %s ConsumedCapacityUnits=%f' % (
                        len(items), cls.__name__, time.time() - t1, 
                        consumed_capacity))
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
            consumed_capacity = 0.0
            # import pprint
            # pprint.pprint(batch_ret)
            if ('UnprocessedKeys' in batch_ret and cls._full_table_name 
                    in batch_ret['UnprocessedKeys']):
                u = batch_ret['UnprocessedKeys'][cls._full_table_name]
                u = [k['HashKeyElement'] for k in u['Keys']]
                unprocessed.extend(u)
            if ('Responses' in batch_ret and cls._full_table_name 
                    in batch_ret['Responses']):
                tbl = batch_ret['Responses'][cls._full_table_name]
                results.extend(tbl['Items'])
                consumed_capacity = tbl['ConsumedCapacityUnits']
        return results, unprocessed, consumed_capacity
    
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
        on the instances. Remember that :meth:`save` only performs a save if
        the object has been modified, so it's a no-op on freshly retrieved 
        instances.

        :type dicts: list
        :param dicts: A list of dictionaries same as provided to `get_or_create`
        """
        # keys = [item[cls._hash_key_name] for item in dicts]
        ret = cls.get_many(dicts)
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
        """
        Return a string that includes all the fields stringified.
        """
        return '<%s %s>' % (self.__class__.__name__, 
                            ' '.join(['='.join(list(map(str, p))) 
                                      for p in self.to_dict().iteritems()]))

    def save(self, force_put=False):
        """
        Performs a save operation if any properties have been changed. 
        Underneath it actually performs one of two potential DynamoDB 
        operations: `UpdateItem` if the item already exists in the store,
        `PutItem` otherwise. 

        `UpdateItem` saves only the fields that have been changed. This is
        potentially a faster operation, minimizing network traffic.

        `PutItem` sends the entire item, replacing all fields no matter what.

        :type force_put: bool
        :param force_put: Forces the entire item to be sent to DynamoDB using
            `PutItem`
        """
        if self._dirty:
            t1 = time.time()
            ret = {'ConsumedCapacityUnits': 0}
            try:
                if self._exists and not force_put:
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
        """
        Convenience method for updating multiple attributes at once.

        :type d: dict
        :param d: The dictionary from which to update attributes.
        """
        for k, v in d.iteritems():
            setattr(self, k, v)
        return self
    
    def delete(self):
        """
        Removes this item from DynamoDB.
        """
        raise NotImplementedError

