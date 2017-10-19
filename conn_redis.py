#!usr/bin/env

"""

    Module to create a connection between python and redis, and set/get key/values

"""

import redis
from datetime import datetime


def conn_redis(host, port, db=0):
    """

        Module to create a connection python/redis

        Args:
            host: Server ip
            port: Port Server Redis
            db: db modules
        Returns:
            r: connection object

    """
    r = redis.Redis(host=host, port=port, db=db)
    return r


def set_email_date(email,  last_date_value, conn):
    last_date_field = 'last_date_field'
    conn.hset(email, last_date_field, last_date_value)


def get_email_last_date(email, conn, last_date_field='last_date_field'):
    last_date_field_return = conn.hget(email, last_date_field)
    return last_date_field_return


def compare_dates(now_date, redis_date):
    try:
        if redis_date < now_date:
            return redis_date
        else:
            return now_date
    except (ValueError, TypeError) as e:
        raise
    return None
