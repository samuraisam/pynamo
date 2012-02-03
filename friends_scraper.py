import requests, json, oauth_hook

_adaptors = {}


class InvalidCredentialsError(Exception):
    pass


class InvalidPermissionsError(Exception):
    pass


class ServiceError(Exception):
    pass


class FriendsAdaptorMeta(type):
    def __init__(self, name, bases, dict):
        _adaptors[dict['name']] = self
        return type.__init__(self, name, bases, dict)


class FriendsAdaptor(object):
    __metaclass__ = FriendsAdaptorMeta
    name = '__none__'

    @classmethod
    def by_name(cls, name, user):
        return _adaptors[name](user)
    
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
    
    def get_friends(self, persist=False):
        """
        Returns a list of Accounts
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

    def validate_login(self):
        d = self.credentials.copy()
        resp = requests.get('https://graph.facebook.com/me', params=d)
        if resp.status_code != 200:
            raise InvalidCredentialsError
        return json.loads(resp.content)

    def get_friends(self, persist=False):
        from buddy.data.account import Account, UserConnections
        d = self.credentials.copy()
        resp = requests.get('https://graph.facebook.com/me/friends', params=d)
        data = json.loads(resp.content)
        friends = []
        while 'data' in data and len(data['data']):
            friends.extend(data['data'])
            resp = requests.get(data['paging']['next'])
            data = json.loads(resp.content)
        else:
            if (resp.status_code == 400 and 'error' in data and
                    data['error']['type'] == OAuthError):
                raise InvalidCredentialsError()
            elif resp.status_code < 200 or resp.status_code > 399:
                raise ServiceError()
        ret = Account.get_or_create_many([{
                'key':          'facebook:' + str(f['id']),
                'service_type': 'facebook',
                'service_id':   str(f['id']),
                'data':         f
            } for f in friends])
        if persist:
            ret = map(lambda s: s.save(), ret)
            # get the user's connections list
            conns = UserConnections.get_or_create(user_id=self.user.user_id)
            conns.ensure_connections(ret)
            conns.save()
        return ret


class TwitterFriendsAdaptor(FriendsAdaptor):
    name = 'twitter'
    verify_url = 'https://api.twitter.com/1/account/verify_credentials.json'
    friends_url = 'https://api.twitter.com/1/friends/ids.json'

    def _get_client(self):
        hook = oauth_hook.OAuthHook(
            access_token=self.credentials['access_token'],
            access_token_secret=self.credentials['access_token_secret'],
            consumer_key=self.credentials['consumer_key'],
            consumer_secret=self.credentials['consumer_secret'])
        return requests.session(hooks={'pre_request': hook})

    def validate_login(self):
        c = self._get_client()
        resp = c.get(self.verify_url)
        if resp.status_code != 200:
            raise InvalidCredentialsError
        return json.loads(resp.content)
    
    def get_friends(self, persist=False):
        from buddy.data.account import Account, UserConnections
        c = self._get_client()
        d = {'cursor': '-1'}
        resp = c.get(self.friends_url, params=d)
        data = json.loads(resp.content)
        items = data['ids']
        while len(data['ids']):
            d['cursor'] = str(data['next_cursor'])
            data = json.loads(c.get(self.friends_url, params=d).content)
            items.extend(data['ids'])
        ret = Account.get_or_create_many([{
                'key': 'twitter:' + str(p),
                'service_type': 'twitter',
                'service_id': str(p),
        } for p in items])
        if persist:
            ret = map(lambda s: s.save(), ret)
            conns = UserConnections.get_or_create(user_id=self.user.user_id)
            conns.ensure_connections(ret)
            conns.save()
        return ret



