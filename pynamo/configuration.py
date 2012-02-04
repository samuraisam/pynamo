import os
import boto

class Configure(object):
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None
    _connection = None
    TABLE_PREFIX = None

    @classmethod
    def with_environment_variables(cls):
        cls.AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
        cls.AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
        cls.TABLE_PREFIX = os.environ['DYNAMODB_TABLE_PREFIX']
    
    @classmethod
    def with_ini_file(cls, ini_file_path=None):
        if ini_file_path is None:
            ini_file_path = os.path.join(os.getcwd(), 'pynamo.cfg')
        import ConfigParser
        p = ConfigParser.ConfigParser()
        p.read([ini_file_path, os.path.expanduser('~/.pynamo.cfg')])
        cls.AWS_ACCESS_KEY_ID = p.get('aws', 'access_key_id')
        cls.AWS_SECRET_ACCESS_KEY = p.get('aws', 'secret_access_key')
        cls.TABLE_PREFIX = p.get('dynamodb', 'table_prefix')
        print 'git', p.get('dynamodb', 'table_prefix')
    
    @classmethod
    def get_connection(cls):
        print 'access key', cls.AWS_ACCESS_KEY_ID, 'secret key', cls.AWS_SECRET_ACCESS_KEY
        if cls._connection is None:
            cls._connection = boto.connect_dynamodb(
                aws_access_key_id=cls.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=cls.AWS_SECRET_ACCESS_KEY)
        return cls._connection
    
    @classmethod
    def get_table_prefix(cls):
        return cls.TABLE_PREFIX
