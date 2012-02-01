from buddy.utils import dynamodb

_adaptors = {}


class InvalidCredentials(Exception):
    pass


class InvalidPermissions(Exception):
    pass


class FriendsAdaptorMeta(type):
    def __init__(self, name, bases, dict):
        _adaptors[dict['name']] = self
        return type.__init__(self, name, bases, dict)


class FriendsAdaptor(object):
    __metaclass__ = FriendsAdaptorMeta
    name = '__none__'

    @classmethod
    def by_name(cls, name):
        return _adaptors[name]
    
    @property
    def connections_key(self):
        return '%s:%s' % (self.name, self.user['user_id'])
    
    def credentials():
        doc = "The credentials property."
        def fget(self):
            return self._credentials
        def fset(self, value):
            self._credentials = value
        def fdel(self):
            del self._credentials
        return locals()
    credentials = property(**credentials())
    
    def __init__(self, user):
        self.user = user
        self._credentials = None
    
    def get_existing_friends(self):
        """
        Return a list of friend-dictionaries
        """
        raise NotImplementedError
    
    def scrape_friends(self):
        """
        Returns a list of 
        """
        raise NotImplementedError
    
    def validate_login(self):
        """
        Determins is the credentials work
        """
        raise NotImplementedError


class FacebookFriendsAdaptor(FriendsAdaptor):
    name = 'facebook'


class TwitterFriendsAdaptor(FriendsAdaptor):
    name = 'twitter'

