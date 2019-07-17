# -*- coding: utf-8 -*-

import time
import uuid
from redis.exceptions import RedisError


class RedistockNotObtained(Exception):
    """Raise for not obtained the lock"""
    pass


# script for acquiring lock
AcquireScript = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return 1
else
    local r = 0
    if ARGV[2] then
        r = redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX')
    else
        r = redis.call('SET', KEYS[1], ARGV[1], 'NX')
    end
    if r then
        return 1
    else
        return 0
    end
end
"""

# script for releasing lock
ReleaseScript = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
end
return 0
"""


class Redistock(object):
    """A simple and easy-to-use and cluster-supported distributed lock implement
    based on Redis and Python.

    Attributes:
        client: a redis connection.
        key: the distributed lock name.
        value: the distributed lock value.
        block: A boolean block when acquiring the lock, default True.
        ttl: seconds expire flag of the lock, None means indefinitely.
        timeout: timeout seconds for acquiring lock when block,
                None means indefinitely.
        delay: seconds for retrying wait, default 0.001.
        lock: A integer lock flag.
        acquire_script: redis.client.Script for acquiring lock.
        release_script: redis.client.Script for releasing lock.
    """

    def __init__(self, client, key, block=True, ttl=None, timeout=None, delay=0.001):
        """Instantiate Redistock."""
        self.client = client
        self.key = key
        self.value = uuid.uuid1().hex.encode('utf-8')
        self.block = block
        self.ttl = int(ttl * 1000) if ttl is not None else None
        self.timeout = timeout
        self.delay = delay
        self.lock = 0
        self.acquire_script = self.client.register_script(AcquireScript)
        self.release_script = self.client.register_script(ReleaseScript)

    def get_timeout(self):
        """Get the wati timeout for acquiring lock.
        Returns:
            (float) timeout
        """
        if self.timeout:
            t = time.time() + self.timeout
        else:
            t = float('inf')
        return t

    def acquire(self):
        """Acquire lock.
        Returns:
            (int) 1 if acquired lock successfully else 0
        Raises:
            RedisError
        """
        args = [self.value, self.ttl] if self.ttl is not None else [self.value]
        timeout = self.get_timeout()
        while 1:
            self.lock = self.acquire_script(keys=[self.key], args=args)
            if not self.block:
                break

            if self.lock:
                break

            if time.time() < timeout:
                time.sleep(self.delay)
                continue

            break
        return self.lock

    def release(self):
        """Release lock.
        Raises:
            RedisError
        """
        if self.lock:
            self.lock = self.release_script(keys=[self.key], args=[self.value])

    def __enter__(self):
        """Enter context manager.
        Returns:
            (int) 1 if acquired lock successfully else 0
        Raises:
            RedistockError
        """
        self.acquire()
        if self.lock:
            return self.lock
        else:
            raise RedistockNotObtained('Acquire Lock Failed')

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.release()
