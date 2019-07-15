# -*- coding: utf-8 -*-

import pytest
import redis
from redistock.redistock import Redistock, RedistockNotObtained

LockKey = 'redistock_lock'


@pytest.fixture(scope='function')
def conn_6379():
    conn = redis.StrictRedis.from_url('redis://127.0.0.1:6379')
    conn.ping()
    return conn


@pytest.fixture(scope='function')
def conn_6380():
    conn = redis.StrictRedis.from_url('redis://127.0.0.1:6380')
    conn.ping()
    return conn


def test_basic(conn_6379):
    redis_lock = Redistock(conn_6379, LockKey, ttlms=10000)
    redis_lock.acquire()
    assert redis_lock.lock
    redis_lock.release()
    assert not redis_lock.lock


def test_reacquire(conn_6379):
    redis_lock = Redistock(conn_6379, LockKey, ttl=1, timeout=0.1)
    redis_lock.acquire()
    assert redis_lock.lock
    redis_lock.acquire()
    assert redis_lock.lock
    redis_lock.release()
    assert not redis_lock.lock


def test_with_statement(conn_6379):
    redis_lock = Redistock(conn_6379, LockKey, ttl=10)
    with redis_lock as lock:
        assert lock
    assert not redis_lock.lock


def test_no_block(conn_6379):
    with Redistock(conn_6379, LockKey, ttl=10):
        assert not Redistock(conn_6379, LockKey, block=False).acquire()


def test_with_acquire_error(conn_6379):
    with Redistock(conn_6379, LockKey, ttl=10):
        with pytest.raises(RedistockNotObtained):
            with Redistock(conn_6379, LockKey, timeout=1) as lock:
                assert not lock


def test_timeout_lock(conn_6379):
    with Redistock(conn_6379, LockKey, ttl=1):
        redis_lock = Redistock(conn_6379, LockKey, timeout=2, delay=0.5)
        lock = redis_lock.acquire()
        assert lock
        redis_lock.release()


def test_timeout_gt_ttl(conn_6379):
    with Redistock(conn_6379, LockKey, ttlms=1):
        redis_lock = Redistock(conn_6379, LockKey)
        assert redis_lock.acquire()
        redis_lock.release()


def test_timeout_lt_ttl(conn_6379):
    with Redistock(conn_6379, LockKey, ttl=10):
        redis_lock = Redistock(conn_6379, LockKey, timeout=2)
        assert not redis_lock.acquire()
        redis_lock.release()


def test_different_server(conn_6379, conn_6380):
    with Redistock(conn_6379, LockKey, ttl=10) as lock_6379:
        assert lock_6379
        with Redistock(conn_6380, LockKey, timeout=1) as lock_6380:
            assert lock_6380
