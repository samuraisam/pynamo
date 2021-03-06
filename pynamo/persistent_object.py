import json, logging, time, string
from boto import connect_dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from boto.exception import DynamoDBResponseError
from boto.dynamodb.schema import Schema
from boto.dynamodb.batch import BatchList
from boto.dynamodb.item import Item
from .exceptions import NotFoundError
from .configuration import Configure
from .fields import Field, StringField

# connection = None
logger = logging.getLogger(__name__)


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


class PersistentObjectMeta(type):
    """
    """
    def __init__(cls, name, bases, classdict):
        new_values = {}
        _props = []
        _remove_props = []
        _meta = []
        _property_instances = {}
        try:
            # hax, if building PersistentObject this will throw a NameError
            in_base = (PersistentObject,)
        except NameError:
            pass
        else:
            found_hash_key = found_range_key = found_table_name = None
            found_hash_key_format = False
            for k, v in classdict.iteritems():
                if isinstance(v, Field):
                    # set hash key
                    if v.options.get('hash_key', False) == True:
                        if found_hash_key:
                            raise TypeError('Only one Field is allowed to be '
                                            'marked hash_key per class. '
                                            '(class %s)' % (name,))
                        new_values['_hash_key_name'] = k
                        new_values['_hash_key_proto'] = v.proto
                        new_values['_hash_key_proto_val'] = v.proto_val
                        found_hash_key = v
                    # set range key
                    if v.options.get('range_key', False) == True:
                        if found_range_key:
                            raise TypeError('Only one Field is allowed to be '
                                            'marked range_key per class '
                                            '(class %s)' % (name,))
                        new_values['_range_key_name'] = k
                        new_values['_range_key_proto'] = v.proto
                        new_values['_range_key_proto_val'] = v.proto_val
                        found_range_key = v
                    v.name = k
                    _props.append(k)
                    _property_instances[k] = v
                if isinstance(v, Meta):
                    _meta.append((k, v))
                    if k == 'hash_key_format':
                        found_hash_key_format = True
                    if k == 'table_name':
                        found_table_name = True
            if not found_hash_key:
                raise TypeError('At least one field must be marked a hash_key '
                                '(even if hash_key_format is defined) for '
                                '(class %s)' % (name,))
            if found_hash_key_format and type(found_hash_key) != StringField:
                raise TypeError('If defining a hash_key_format, the field '
                                'marked as hash_key must be a StringField. '
                                '(class %s)' % (name,))
            if not found_table_name:
                raise TypeError('Must define a table_name for class ' + name)  
        type.__init__(cls, name, bases, classdict)

        new_values['_properties'] = _props
        new_values['_property_instances'] = _property_instances
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
            attr_list = []
            pieces = string.Formatter().parse(fmt)
            for literal_text, field_name, format_spec, conversion in pieces:
                attr_list.append(field_name)
                if field_name not in classdict:
                    raise TypeError('hash_key_format definition requires %s but'
                                    ' it is not an attribute on the class. (%s)' 
                                    % (field_name, name,))
            setattr(cls, '__hash_key_attributes__', tuple(attr_list))


class PersistentObject(object):
    """
    """
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
    _full_table_name = None
    _schema = None
    _table = None
    _properties = None
    _property_instances = None

    __metaclass__ = PersistentObjectMeta

    # TABLE MANIPULATION

    @classmethod
    def _load_meta(cls):
        if cls == PersistentObject:
            raise TypeError('Can not perform that operation on the base class. '
                            'Please subclass PersistentObject to do that.')
        connection = Configure.get_connection()
        if cls._table is not None:
            return
        # get the full table name
        cls._full_table_name = Configure.get_table_prefix() + cls.__table_name__
        # get the table
        cls._table = connection.get_table(cls._full_table_name)
    
    @classmethod
    def create_table(cls, wait=True):
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
        connection = Configure.get_connection()
        cls._full_table_name = Configure.get_table_prefix() + cls.__table_name__
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
        
        if wait:
            resp = connection.describe_table(cls._full_table_name)
            while resp['Table']['TableStatus'] == 'CREATING':
                time.sleep(1)
                resp = connection.describe_table(cls._full_table_name)
            cls._table.update_from_response(resp)
    
    @classmethod
    def drop_table(cls, wait=True):
        """
        Removes the table and does not wait for DynamoDB to compeltely remove
        it. The table will be in the `DELETING` state for some time afterwards.
        Sends a `DeleteTable` to DynamoDB.
        """
        cls._load_meta()
        cls._table.delete()
        conn = Configure.get_connection()

        if wait:
            for i in xrange(30):
                try:
                    cls._table.update_from_response(conn.describe_table(
                        cls._table.name))
                except DynamoDBResponseError, e:
                    if e.data['__type'].endswith('ResourceNotFoundException'):
                        return
                time.sleep(1)
            else:
                raise Exception('Could not verify that drop table was '
                                'completed successfully.')
    
    @classmethod
    def reset_table(cls, wait=True):
        """
        Drops the table (it must previously exist) then waits until it's totally
        gone to recreate it. Doesn't wait for recreation to complete. Sends 
        `DeleteTable` followed by `CreateTable` to DynamoDB.
        """
        cls._load_meta()
        cls.drop_table()
        conn = Configure.get_connection()
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
        cls.create_table(wait=True)
    
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
        # provided dict and _hash_key_name is already there
        if (isinstance(key_or_dict, dict) and cls._hash_key_name 
                in key_or_dict):
            ret = key_or_dict[cls._hash_key_name]
        # provided a dict and key_attribute and key_format are filled out
        elif (isinstance(key_or_dict, dict) and cls.__hash_key_attributes__ is 
                not None and cls.__hash_key_format__ is not None):
            for k in cls.__hash_key_attributes__:
                if k not in key_or_dict:
                    raise ValueError('Tried to build a key but not all the '
                                     'required attributes were present: ' 
                                    + repr(cls.__hash_key_attributes__))
            ret = cls.__hash_key_format__.format(**key_or_dict)
        # otherwise the key is assumed to be already valid 
        else:
            ret = key_or_dict
        # validate it
        cls._property_instances[cls._hash_key_name].validate(ret)
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
            # a single key or a single dictionary
            k = cls.prepare_key(a[0])
        elif len(kw) >= len(cls.__hash_key_attributes__):
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
            attr_typ = getattr(cls, cls._hash_key_name)
            if attr_typ.options.get('auto', False):
                key = getattr(cls, cls._hash_key_name).new()
            else:
                try:
                    key = cls.prepare_key(d)
                except ValueError:
                    raise ValueError('Creation attrubuts must contain at least '
                                     'the hash key or the hash key attributes')
        else:
            key = d[cls._hash_key_name]
        # create the underlying boto.dynamodb.item.Item
        _hk_typ = getattr(cls, cls._hash_key_name)
        args = {
            'hash_key': cls._hash_key_name, 
            'attrs': {
                cls._hash_key_name: _hk_typ.from_python(key)
            }
        }
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
    def get_many(cls, keys, attributes_to_get=None):
        """
        Returns a list of :class:`PersistentObject` identical in length to the
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
        cls._load_meta()
        keys = map(cls.prepare_key, keys)
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
    def _fetch_batch_queue(cls, batch_queue, attributes_to_get=None):
        results = []
        unprocessed = []
        consumed_capacity = 0.0
        while len(batch_queue):
            batch_keys = batch_queue.pop()
            if not len(batch_keys):
                continue
            batch = BatchList(Configure.get_connection())
            batch.add_batch(cls._table, [cls._hash_key_proto(k) 
                                         for k in batch_keys],
                            attributes_to_get=attributes_to_get)
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
                tbl = batch_ret['Responses'][cls._full_table_name]
                results.extend(tbl['Items'])
                consumed_capacity += tbl['ConsumedCapacityUnits']
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
        instead. Returns a list of :class:`PersistentObject` the same as 
        :meth:`get_many` except that the None slots (non-existing entries) are
        filled in.

        Note that the :class:`PersistentObject`s, whether they are created or not
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
        return {n: getattr(self.__class__, n).render(getattr(self, n)) 
                    for n in self._properties}
    
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

