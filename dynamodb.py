from pyramid import threadlocal
from boto import connect_dynamodb

connection = None

def get_connection():
    s = threadlocal.get_current_registry().settings
    if connection is None:
        global connection # these connections are thread safe
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
    schema = conn.create_schema(**schema)
    return conn.create_table(name=get_table_prefix() + name, schema=schema, 
                             read_units=read_units, write_units=write_units)

def get_item(table_name, key):
    return get_table().get_item(hash_key=key)

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

