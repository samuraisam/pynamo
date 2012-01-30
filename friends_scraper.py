from buddy.utils import dynamodb

_adaptors = {}


class FriendsAdaptorMeta(type):
  def __init__(self, name, bases, dict):
  	print 'dict', dict
    _adaptors[dict['name']] = self
    dict['friend_class'] = make_friend_class(dict['name'])
    return type.__init__(self, name, bases, dict)


class FriendsAdaptor(object):
	__metaclass__ = FriendsAdaptorMeta

	@classmethod
	def get_adaptor(cls, name):
		return _adaptors[name]
	
	@property
	def connections_key(self):
		return '%s:%s' % (self.name, self.user['user_id'])
	
	def __init__(self, user):
		self.user = user
	
	def get_existing_friends(self):
		"""
		Return a list of friend-dictionaries
		"""
		item = dynamodb.get_item('account_connections', self.connections_key)

	
	def scrape_friends(self):
		"""
		Returns a list of 
		"""
		raise NotImplementedError


class FacebookFriendsAdaptor(FriendsAdaptor):
	name = 'facebook'


class TwitterFriendsAdaptor(FriendsAdaptor):
	name = 'twitter'

