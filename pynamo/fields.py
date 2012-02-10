import json
from .exceptions import ValidationError
from .lexical_uuid import LexicalUUID


class Field(object):
    """
    A base descriptor class which facilitates creating typed and validated 
    properties on :class:`PersistentObject` classes.
    """
    def __init__(self, **options):
        if self.__class__ == Field:
            raise TypeError('Field is a baseclass. Instantiate one of the '
                            'Field subclasses instead.')
        self.options = options
        self.name = None
    
    def __get__(self, obj, type=None):
        if obj is None:
            return self
        c = obj._property_cache
        if self.name not in c:
            c[self.name] = self.to_python(obj._item.get(self.name, None))
        return c[self.name]

    def __set__(self, obj, value):
        if value is None:
            return self.__delete__(obj)
        c = obj._property_cache
        c[self.name] = value
        cleaner = getattr(obj, 'clean_' + self.name, lambda val: (val, None))
        # clean it
        value, error = cleaner(value)
        if error is not None:
            raise ValidationError(error)
        # validate it
        self.validate(value)
        old_value = obj._item.get(self.name, None)
        if value != old_value:
            # convert it
            value = self.from_python(value)
            print 'from_python', self.name, ':', value
            # see if it's actually any different and set it
            self.do_set(obj, old_value, value)
    
    def do_set(self, obj, old_value, value, set_dirty=True):
        if obj._exists:
            if old_value is None:
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
        pass


# NATIVE TYPES
# These are fields that DynamoDB understands directly


class StringField(Field):
    """
    A string field. For strings, like `unicode` and `str`
    """
    proto_val = ''
    proto = str

    def validate(self, value):
        if value is not None and not isinstance(value, basestring):
            raise ValidationError("An instance of str is required.")


class IntegerField(Field):
    """
    Numbers. `long`s and `int`s only please.
    """
    proto_val = 1
    proto = int

    def validate(self, value):
        if value is not None and not isinstance(value, (int, long)):
            raise ValidationError("An instance of int or long is required")


class LexicalUUIDField(IntegerField):
    """
    A field that can be used as a `hash_key`. It will automatically generate
    a new LexicalUUID for new items.
    """
    
    @classmethod
    def new(cls):
        return LexicalUUID()
    
    # def __get__(self, obj, type=None):
    #     s = super(AutoLexicalUUIDField, self).__get__(obj, type=type)
    #     auto = self.options.get('auto', False)
    #     if s is None and auto and not obj._exists:
    #         s  = LexicalUUID()
    #         setattr(obj, self.name, s)
    #     return s
    
    def to_python(self, value):
        return LexicalUUID(value)
    
    def from_python(self, value):
        return value.int
    
    def validate(self, value):
        if value is not None and not isinstance(value, LexicalUUID):
            raise ValidationError('An instance of LexicalUUID is required.')


class FloatField(Field):
    proto_val = 1.0
    proto = float

    def validate(self, value):
        if value is not None and not isinstance(value, float):
            raise ValidationError("An instance of float is required")

    def to_python(self, value):
        if value is None:
            return value
        return float(value)


class BoolField(Field):
    proto_val = False
    proto = bool

    def validate(self, value):
        if value is not None and not isinstance(value, bool):
            raise ValidationError("An instance of bool is required")

    def to_python(self, value):
        if value is None:
            return value
        return bool(int(value))


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
        
        def add_to_set(instance, items):
            """
            Adds the provided items to the SetItem attribute.

            NOTE:
                You can only add to this set if you are ONLY adding to the set.
                Otherwise just set it as an attribute like normal.
            
            :type items: set|list
            :param items: The items to add to this set.
            """
            items = set(items)
            old_value = getattr(instance, self.name)
            if old_value is None:
                old_value = set()
            # get the existing pending updates and ensure they are in the items
            existing_pending = instance._item._updates.get(
                    self.name, (None, None))
            if existing_pending[0] not in (None, 'ADD'):
                raise ValueError('There are already pending updates for this '
                                 'attribute=%s that are not adding (%s). You '
                                 'can only perform either add or delete once '
                                 'per update.' 
                                 % (self.name, existing_pending[0]))
            if isinstance(existing_pending[1], set):
                items |= existing_pending[1]
            elif existing_pending[1] is not None:
                # it was set to something invalid entirely so this operation
                # will totally stomp on it
                raise ValueError('Trying to update an attribute (%s) that was '
                                 'not previously updated as a set.' 
                                 % (self.name,))
            new_value = old_value | items
            self.validate(new_value)
            if new_value != old_value:
                if instance._exists:
                    instance._item.add_attribute(self.name, items)
                else:
                    instance._item.put_attribute(self.name, new_value)
                instance._dirty = True
                instance._item[self.name] = new_value
                instance._property_cache[self.name] = new_value
        
        def remove_from_set(instance, items):
            """
            Remove the provided items from the SetItem attribute.

            NOTE:
                You can only delete from this set if you are ONLY deleting from
                the set. Otherwise just set it like a normal attribute.
            
            :type items: set|list
            :param items: The items to remove from this set.
            """
            items = set(items)
            old_value = getattr(instance, self.name)
            if old_value is None:
                old_value = set()
            existing_pending = instance._item._updates.get(
                    self.name, (None, None))
            if existing_pending[0] not in (None, 'DELETE'):
                raise ValueError('There are already pending updates for this '
                                 'attribute=%s that are not adding (%s). You '
                                 'can only perform either add or delete once '
                                 'per update.' 
                                 % (self.name, existing_pending[0]))
            if isinstance(existing_pending[1], set):
                items |= existing_pending[1]
            elif existing_pending[1] is not None:
                # it was set to something invalid entirely so this operation
                # will totally stomp on it, try to avoid doing that
                raise ValueError('Trying to update an attribute (%s) that was '
                                 'not previously updated as a set.' 
                                 % (self.name,))
            # get the existing pending updates and ensure they are in the items
            existing_pending = instance._item._updates.get(self.name, )
            new_value = old_value - items
            self.validate(new_value)
            if new_value != old_value:
                if instance._exists:
                    instance._item.delete_attribute(self.name, items)
                else:
                    instance._item.put_attribute(new_value)
                instance._dirty = True
                instance._item[self.name] = new_value
                instance._property_cache[self.name] = new_value
        
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
        if value is None:
            return
        super(NumberSetField, self).validate(value)
        valid_values = map(lambda v: isinstance(v, (long, int, bool, float)), 
                           value)
        if False in valid_values:
            raise ValidationError("Invalid value inside NumberSetField - all "
                                  "values must be numbers (int, long, bool, "
                                  "float) got: " + repr(map(type, value)))

class StringSetField(SetField):
    """
    A :class:`SetField` which contains only strings (anything descending from
    `basestring`). Upon setting or modifying this attribute this class will 
    validate that it's contents contain only strings.
    """
    proto_val = set([''])

    def validate(self, value):
        if value is None:
            return
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
        if value is not None and not isinstance(value, self.object_types):
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
