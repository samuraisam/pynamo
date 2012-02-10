import socket, os, struct, datetime, time, threading, string

__doc__ = """
An adaptation of https://github.com/jamesgolick/lexical_uuid for pythons.
Borrowing code from https://github.com/jakedouglas/fnv-ruby
And http://hg.python.org/cpython/file/90eda29a8e03/Lib/uuid.py
Generates lexicographically sortable unique IDs.
"""


ALPHABET = string.ascii_uppercase + string.ascii_lowercase + \
           string.digits + '-_'
ALPHABET_REVERSE = dict((c, i) for (i, c) in enumerate(ALPHABET))
BASE = len(ALPHABET)
SIGN_CHARACTER = '$'

def new_timestamp():
    return long(time.time()*1000000)

def fnv1a_64(data):
    """
    Hashes a string using 64bit fnv1a: http://isthe.com/chongo/tech/comp/fnv/
    """
    r = 0xcbf29ce484222325
    mh = 2 ** 64
    for i in data:
        r = r ^ ord(i)
        r = (r * 0x100000001b3) % mh
    return r


class IncreasingMicrosecondClock(object):
    """
    A clock that returns a new timestamp value unique across all threads, 
    acheived by locking a mutex each call and checking the previous value.
    """
    def __init__(self, timestamp_factory=new_timestamp, mutex=threading.Lock):
        self.timestamp_factory = timestamp_factory
        self.mutex = mutex()
        self.time = timestamp_factory()
    
    def __call__(self):
        with self.mutex:
            new_time = self.timestamp_factory()
            if new_time > self.time:
                self.time = new_time
            else:
                self.time += 1
            return self.time


class LexicalUUID(object):
    worker_id = fnv1a_64("{}-{}".format(socket.getfqdn(), os.getpid()))
    
    def __init__(self, value=None, worker_id=None, 
                 timestamp_factory=IncreasingMicrosecondClock):
        self.timestamp_factory = timestamp_factory()

        if isinstance(value, self.__class__):
            self.timestamp = value.timestamp
            self.worker_id = value.worker_id
        elif isinstance(value, (int, long)):
            bytes = ''
            for sh in range(0, 128, 8):
                bytes = chr((value >> sh) & 0xff) + bytes
            self.from_bytes(bytes)
        elif isinstance(value, basestring):
            if len(value) == 16:
                self.from_bytes(value)
            elif len(value) == 36:
                elements = value.split('-')
                self.from_bytes(struct.pack('I32', ''.join(elements)))
            else:
                raise ValueError('{} was incorrectly sized.'.format(value))
        elif isinstance(value, datetime.datetime):
            self.timestamp = long(time.mktime(value.timetuple())*1000000)
        elif value is None:
            self.timestamp = self.timestamp_factory()
        else:
            raise ValueError("Can not convert {} into a "
                             "LexicalUUID".format(value))
        
        self.int = int(('%02x'*16) % self.byte_tuple, 16)
    
    def from_bytes(self, bytes):
        th, tl, wh, wl = struct.unpack('!IIII', bytes)
        self.timestamp = (th << 32) | tl
        self.worker_id = (wh << 32) | wl

    def encode(self):
        n = self.int
        s = []
        while True:
            n, r = divmod(n, BASE)
            s.append(ALPHABET[r])
            if n == 0:
                break
        return ''.join(reversed(s))

    @classmethod
    def decode(cls, s):
        n = 0
        for c in s:
            n = n * BASE + ALPHABET_REVERSE[c]
        return cls(n)

    @property
    def guid(self):
        h = '%032x' % self.int
        return '%s-%s-%s-%s-%s' % (h[:8], h[8:12], h[12:16], h[16:20], h[20:])
    
    @property    
    def bytes(self):
        return struct.pack('!IIII', self.timestamp >> 32, 
                           self.timestamp & 0xffffffff,
                           self.worker_id >> 32, self.worker_id & 0xffffffff)
    
    @property
    def byte_tuple(self):
        return tuple(map(ord, list(self.bytes)))
    
    @property
    def node(self):
        return self.worker_id

    def __str__(self):
        return '<%s %s>' % (self.__class__.__name__, self.guid)
    
    def __repr__(self):
        return self.__str__()
    
    def __eq__(self, other):
        return (
            isinstance(other, LexicalUUID) and
            self.timestamp == other.timestamp and
            self.worker_id == other.worker_id
        )
    
    def __cmp__(self, other):
        return (
            self.worker_id > other.worker_id 
            if self.timestamp == other.timestamp 
            else self.timestamp > other.timestamp
        )
    
    def __hash__(self):
        return hash(self.int)

