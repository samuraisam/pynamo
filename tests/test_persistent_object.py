import unittest
from boto.exception import DynamoDBResponseError
from boto.dynamodb.table import Table
from pynamo import (PersistentObject, Meta, Configure, StringField, 
                    IntegerField, NotFoundError)


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


class PersistentObjectTest(unittest.TestCase):
    def setUp(self):
        # create a singular table
        Configure.with_ini_file()
        TestPersistentObject.create_table(wait=True)
        TestPersistentObjectPreparedKey.create_table(wait=True)
    
    def tearDown(self):
        TestPersistentObject.drop_table(wait=True)
        TestPersistentObjectPreparedKey.drop_table(wait=True)
