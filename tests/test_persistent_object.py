import unittest
from boto.dynamodb.exception import DynamoDBResponseError
from boto.dynamodb.table import Table
from pynamo import PersistentObject, Meta, Configure, StringField, IntegerField


class TestPersistentObject(PersistentObject):
    table_name = Meta('test_table')

    key = StringField(hash_key=True)


class TestPersistentObjectPreparedKey(PersistentObject):
    table_name = Meta('test_table_2')
    hash_key_format = Meta('{key_1}:{key_2}')

    key_1 = StringField()
    key_2 = IntegerField()
    key_3 = StringField()


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


class TestMeta(unittest.TestCase):
    pass


class PersistentObjectTest(unittest.TestCase):
    def setUp(self):
        # create a singular table
        Configure.with_ini_file()
        TestPersistentObject.create_table(wait=True)
        TestPersistentObjectPreparedKey.create_table(wait=True)
    
    def tearDown(self):
        TestPersistentObject.drop_table(wait=True)
        TestPersistentObjectPreparedKey.drop_table(wait=True)

    def test_delete(self):
        pass
    
    ### ATTRIBUTE UPDATE TESTS

    def test_update_object(self):
        pass
    
    def test_update_single_attribute(self):
        pass
    
    ### SAVE TESTS

    def test_save(self):
        pass
    
    def test_save_new(self):
        pass
    
    def test_save_exists(self):
        pass
    
    ### BATCH TESTS
    
    def test_get_or_create_many_some_new(self):
        pass
    
    def test_get_or_create_many_all_new(self):
        pass
    
    def test_get_or_create_many_none_new(self):
        pass
    
    def test_get_or_create_many_prepared_key(self):
        pass
    
    def test_get_many_some_exist(self):
        pass
    
    def test_get_many_none_exist(self):
        pass
    
    def test_get_many_all_exist(self):
        pass
    
    def test_get_many_prepared_key(self):
        pass
    
