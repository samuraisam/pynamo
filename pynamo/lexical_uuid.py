import socket, os, struct, datetime, time, threading, array

__doc__ = """
An adaptation of https://github.com/jamesgolick/lexical_uuid for pythons.
Borrowing code from https://github.com/jakedouglas/fnv-ruby
And http://hg.python.org/cpython/file/90eda29a8e03/Lib/uuid.py
Generates lexicographically sortable unique IDs.
"""

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
    
    def __init__(self, timestamp=None, worker_id=None, 
                 timestamp_factory=IncreasingMicrosecondClock):
        self.timestamp_factory = timestamp_factory()
        if isinstance(timestamp, (int, long)):
            self.timestamp = timestamp
            if worker_id:
                self.worker_id = worker_id
        elif isinstance(timestamp, basestring):
            if len(timestamp) == 16:
                self.from_bytes(timestamp)
            elif len(timestamp) == 36:
                elements = timestamp.split('-')
                self.from_bytes(struct.pack('H32', ''.join(elements)))
            else:
                raise ValueError(
                        '{timestamp} was incorrectly sized.'.format(locals()))
        elif isinstance(timestamp, datetime.datetime):
            self.timestamp = long(time.mktime(timestamp.timetuple())*1000000)
        elif timestamp is None:
            self.timestamp = self.timestamp_factory()
        self.int = int(('%02x'*16) % self.byte_tuple, 16)

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

    
    def __eq__(self, other):
        return (
            isinstance(other, LexicalUUID) and
            self.timestam == other.timestamp and
            self.worker_id == other.worker_id
        )
    
    def __cmp__(self, other):
        return (
            self.worker_id > other.worker_id 
            if self.timestamp == other.timestamp 
            else self.timestamp > other.timestamp
        )
    
    def from_bytes(self, bytes):
        th, tl, wh, wl = struct.unpack('!IIII', bytes)
        self.timestamp = (th << 32) | tl
        self.worker_id = (wh << 32) | wl
    
    def __hash__(self):
        return hash(self.bytes())

