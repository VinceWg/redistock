# Redistock
A simple and easy-to-use and cluster-supported distributed lock implement based on Redis and Python.

## Installation
```bash
pip install redistock
```

## Usage

Simple usage:
```python
import redis
from redistock import Redistock

redis_conn = redis.StrictRedis(...)

# simple
lock = Redistock(redis_conn, 'name')
if lock.acquire():
    print('Obtained lock')
lock.release()

# with statement
with Redistock(redis_conn, 'name'):
    print('Obtained lock')

```

Advanced usage:
```python
import redis
from redistock import Redistock
from redistock import RedistockNotObtained

redis_conn = redis.StrictRedis(...)


with Redistock(redis_conn, 'name', ttl=1):
    # will be blocked for 10 seconds
    print('Obtained lock')


with Redistock(redis_conn, 'name', ttlms=100):
    # will be blocked for 100 milliseconds
    print('Obtained lock')


with Redistock(redis_conn, 'name', delay=1):
    # will delay 1 seconds between twice retry, default 0.001
    print('Obtained lock')


lock = Redistock(redis_conn, 'name', block=False)
# do not retry, raise RedistockNotObtained if not obtained lock
if lock.acquire():
    print('Obtained lock')
lock.release()


try:
    with Redistock(redis_conn, 'name', timeout=10):
        # will try to acquire for 10 seconds, success or raise RedistockNotObtained
        # after 10 seconds
        print('Obtained lock')
except RedistockNotObtained:
    print('Not obtained lock')

```