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
    key_dict = DictField()
    key_list = ListField()
    key_string_set = StringSetField()
    key_number_set = NumberSetField()
    key_bool = BoolField()
    key_float = FloatField()
    key_integer = IntegerField()