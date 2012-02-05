import unittest, random
from boto.exception import DynamoDBResponseError
from boto.dynamodb.table import Table
from pynamo import *


class TestPersistentObject(PersistentObject):
    table_name = Meta('test_table')

    key = StringField(hash_key=True)


class TestPersistentObjectPreparedKey(PersistentObject):
    table_name = Meta('test_table_2')
    hash_key_format = Meta('{key_1}:{key_2}')

    key = StringField(hash_key=True)
    key_1 = StringField()
    key_2 = IntegerField()
    key_string = StringField()
    key_dict_field = DictField()
    key_list_field = ListField()
    key_string_set = StringSetField()
    key_number_set = NumberSetField()
    key_bool_field = BoolField()
    key_float_field = FloatField()
    key_integer_field = IntegerField()


class PersistentObjectClassTests(unittest.TestCase):
    def test_no_hash_key(self):
        er = 'At least one field must be marked a hash_key \(even if ' \
             'hash_key_format is defined\) for \(class A\)'
        with self.assertRaisesRegexp(TypeError, er):
            class A(PersistentObject):
                table_name = Meta('t1')
        
    def test_too_many_hash_keys(self):
        er = 'Only one Field is allowed to be marked hash_key per class. ' \
             '\(class A\)'
        with self.assertRaisesRegexp(TypeError, er):
            class A(PersistentObject):
                table_name = Meta('t1')
                hk1 = StringField(hash_key=True)
                hk2 = StringField(hash_key=True)
    
    def test_too_many_range_keys(self):
        er = 'Only one Field is allowed to be marked range_key per class ' \
             '\(class B\)'
        with self.assertRaisesRegexp(TypeError, er):
            class B(PersistentObject):
                table_name = Meta('t1')
                rk1 = IntegerField(range_key=True)
                rk2 = IntegerField(range_key=True)

    def test_fmt_wrong_key_type(self):
        er = 'If defining a hash_key_format, the field marked as hash_key ' \
             'must be a StringField. \(class B\)'
        with self.assertRaisesRegexp(TypeError, er):
            class B(PersistentObject):
                table_name = Meta('t1')
                hash_key_format = Meta('{key1}:{key2}')
                key = IntegerField(hash_key=True)
    
    def test_fmt_missing_piece(self):
        er = 'hash_key_format definition requires key2 but it is not an ' \
             'attribute on the class. \(C\)'
        with self.assertRaisesRegexp(TypeError, er):
            class C(PersistentObject):
                table_name = Meta('t1')
                hash_key_format = Meta('{key1}:{key2}')
                key = StringField(hash_key=True)
                key1 = StringField()
    
    def test_no_table_name(self):
        er = 'Must define a table_name for class C'
        with self.assertRaisesRegexp(TypeError, er):
            class C(PersistentObject):
                key = StringField(hash_key=True)


# these tests take unbearibly long to run
# dynamodb takes forever to create/destroy tables
class PersistentObjectTableTests(unittest.TestCase):
    def setUp(self):
        Configure.with_ini_file()
    
    def test_create_wait_drop(self): # waits for creation
        TestPersistentObject.create_table(wait=True)
        
        conn = Configure.get_connection()

        self.assertTrue(isinstance(conn.get_table(
            TestPersistentObject._full_table_name), Table))

        TestPersistentObject.drop_table(wait=True)

        self.assertRaises(DynamoDBResponseError, conn.get_table, 
                          TestPersistentObject._full_table_name)
    
    def test_reset_table(self):
        # create a table
        TestPersistentObject.create_table(wait=True)
        
        # create some objects in it
        TestPersistentObject.create(key='lol').save()
        TestPersistentObject.create(key='wut').save()
        
        # make sure they're really there
        self.assertTrue(isinstance(TestPersistentObject.get('lol'), 
            TestPersistentObject))
        
        conn = Configure.get_connection()

        TestPersistentObject.reset_table(wait=True)

        # now ensure they're really gone
        self.assertRaises(NotFoundError, TestPersistentObject.get, 'lol')
        self.assertRaises(NotFoundError, TestPersistentObject.get, 'wut')

        # and that the table is recreated
        self.assertTrue(isinstance(conn.get_table(
            TestPersistentObject._full_table_name), Table))

        TestPersistentObject.drop_table(wait=True)

        self.assertRaises(DynamoDBResponseError, conn.get_table, 
                          TestPersistentObject._full_table_name)


class PersistentObjectTests(unittest.TestCase):
    @staticmethod
    def setUpClass():
        # create a singular table
        Configure.with_ini_file()
        TestPersistentObject.create_table(wait=True)
        TestPersistentObjectPreparedKey.create_table(wait=True)
    
    @staticmethod
    def tearDownClass():
        TestPersistentObject.drop_table(wait=True)
        TestPersistentObjectPreparedKey.drop_table(wait=True)
    
    def test_prepare_key(self):
        # first test that prepare_key does not do anything if it's not set up
        self.assertEquals(TestPersistentObject.prepare_key('lol'), 'lol')
        # then test that prepare_key works with all the attribtues
        self.assertEquals(TestPersistentObjectPreparedKey.prepare_key(
            dict(key_1='hello', key_2=1, key_3='wut')), 'hello:1')
        # and test that it throws the proper error when they aren't there
        with self.assertRaises(ValueError): 
            TestPersistentObjectPreparedKey.prepare_key( 
                dict(key_1='hello', key_string='wut'))
    
    def test_creation(self):
        le_id1 = uuid.uuid1().hex
        r1 = PersistentObject.create(key=le_id1).save()
        self.assertEquals(TestPersistentObject.get(le_id1).key, le_id1)
    
        d2 = { # this is only a subset of the keys
            'key_1': uuid.uuid1().hex,
            'key_2': random.randint(50000, 500000000),
            'key_str': uuid.uuid1(),
        }
        r2 = TestPersistentObjectPreparedKey.create(d2).save()
        self.assertEquals(d2['key_1'], r2.key_1)
        self.assertEquals(d2['key_2'], r2.key_2)
        self.assertEquals(d2['key_str'], r2.key_str)
        res2 = TestPersistentObjectPreparedKey.get(d2)
        self.assertEquals(r2.key_1, res2.key_1)
        self.assertEquals(r2.key_2, res2.key_2)
        self.assertEquals(r2.key_str, res2.key_str)

    def test_get_or_create(self):
        le_id1 = uuid.uuid1().hex
        # non-existant

