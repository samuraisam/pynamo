import unittest, uuid, random
from pynamo import *
from .common import TestPersistentObjectPreparedKey


TestPO = TestPersistentObjectPreparedKey
new_key = lambda: uuid.uuid1().hex
new_int = lambda: random.randint(500000, 500000000)
def new_PO(d=None, **kw):
    if d is None:
        d = {}
    d.update({'key_1': new_key(), 'key_2': new_int()})
    d.update(kw)
    return d, TestPO.create(d)


class ATotallyInvalidValue(object):
    pass

class FieldTests(unittest.TestCase):
    value = 'hello'
    alternate = 'goodbye'
    attr = 'key_string'

    @staticmethod
    def setUpClass():
        Configure.with_ini_file()
    
    def test_field_instantiate(self):
        with self.assertRaises(TypeError):
            Field()

    def test_field_creation(self):
        # creation
        d, t = new_PO(**{self.attr: self.value})
        self.assertEquals(getattr(t, self.attr), self.value)
        t.save()
        t = TestPO.get(d)
        # retrieval
        self.assertEquals(getattr(t, self.attr), self.value)
    
    def test_field_update(self):
        d, t = new_PO(**{self.attr: self.value})
        self.assertEquals(getattr(t, self.attr), self.value)
        t.save()
        t = TestPO.get(d)
        setattr(t, self.attr, self.alternate)
        # changing
        self.assertEquals(getattr(t, self.attr), self.alternate)
        t.save()
        t = TestPO.get(d)
        # retrieval after changing
        self.assertEquals(getattr(t, self.attr), self.alternate)
    
    def test_field_deletion(self):
        # regular deletion
        d, t = new_PO(**{self.attr:self.value})
        t.save()
        t = TestPO.get(d)
        delattr(t, self.attr)
        self.assertEquals(getattr(t, self.attr), None)
        t.save()
        # retrieval after delete
        t = TestPO.get(d)
        self.assertEquals(getattr(t, self.attr), None)
        # delete before save
        d, t = new_PO(**{self.attr:self.value})
        delattr(t, self.attr)
        self.assertEquals(getattr(t, self.attr), None)
        t.save()
        # retrieval after delete before save
        t = TestPO.get(d)
        self.assertEquals(getattr(t, self.attr), None)
    
    def test_field_set_none(self):
        d, t = new_PO(**{self.attr:self.value})
        t.save()
        t = TestPO.get(d)
        setattr(t, self.attr, None)
        t.save()
        t = TestPO.get(d)
        self.assertEquals(getattr(t, self.attr), None)
    
    def test_validate(self):
        with self.assertRaises(ValidationError):
            d, t = new_PO(**{self.attr:ATotallyInvalidValue()})
        d, t = new_PO(**{self.attr:self.value})
        t.save()
        with self.assertRaises(ValidationError):
            setattr(t, self.attr, ATotallyInvalidValue())
        t.save()
        t = TestPO.get(d)
        self.assertEquals(getattr(t, self.attr), self.value)
        

class IntegerTests(FieldTests):
    value = 1
    alternate = 2
    attr = 'key_integer'

class FloatTests(FieldTests):
    value = 1.11
    alternate = 2.22
    attr = 'key_float'

class BoolTests(FieldTests):
    value = True
    alternate = False
    attr = 'key_bool'

class DictTests(FieldTests):
    value = {'a': 'b', 'c': {'d': 'e'}}
    alternate = {'omg': 'wtf', 'nowai': 'yawai'}
    attr = 'key_dict'

class ListTests(FieldTests):
    value = [1,2,3]
    alternate = [5,6,'lolwut']
    attr = 'key_list'

class NumberSetTests(FieldTests):
    value = set([1,2,3])
    alternate = set([5,6,7])
    attr = 'key_number_set'

class StringSetTests(FieldTests):
    value = set(['a','b','c'])
    alternate = set(['d','e','f'])
    attr = 'key_string_set'
