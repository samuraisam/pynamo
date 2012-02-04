import unittest
from pynamo import PersistentObject, Meta, Configure, StringField


class TestPersistentObject(PersistentObject):
    table_name = Meta('test_table')

    key = StringField(hash_key=True)


class PersistentObjectTest(uitttest.TestCase):
    def setUp(self):
        # create a singular table
        Configure.with_environment_variables()
        TestPersistentObject.create_table()

    
    def tearDown(self):
        TestPersistentObject.drop_table()
