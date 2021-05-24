from __future__ import absolute_import
import json

import redis

from django.conf import settings
import six


class RedisObject(object):
    conn = redis.StrictRedis(**settings.REDIS_CONNECTION)

    def __init__(self, entity, id):
        self.entity = entity
        self.id = id
        self.key = '{}:{}'.format(entity, id)

    def set(self, data):
        pipe = self.conn.pipeline()
        pipe.delete(self.key)

        if isinstance(data, (list, tuple)):
            i = 0
            for item in data:
                pipe.zadd(self.key, i, json.dumps(item))
                i += 1

        elif isinstance(data, dict):
            for k, v in six.iteritems(data):
                pipe.hset(self.key, k, json.dumps(v))

        else:
            pipe.set(self.key, json.dumps(data))

        pipe.execute()
        return data

    def set_field(self, field, data):
        pipe = self.conn.pipeline()
        pipe.hset(self.key, field, json.dumps(data))
        pipe.execute()
        return data

    def set_value(self, index, data):
        pipe = self.conn.pipeline()
        pipe.zremrangebyscore(self.key, index, index)
        pipe.zadd(self.key, index, json.dumps(data))
        pipe.execute()
        return data

    def get(self):
        datatype = self.get_datatype().decode('utf-8')
        try:
            if datatype == 'zset':
                return [json.loads(i.decode('utf-8')) for i in self.conn.zrange(self.key, 0, -1)]
            if datatype == 'hash':
                return {k: json.loads(v.decode('utf-8')) for k, v in six.iteritems(self.conn.hgetall(self.key))}
            if datatype == 'string':
                return json.loads(self.conn.get(self.key).decode('utf-8'))
            if datatype == 'list':
                return [json.loads(i.decode('utf-8')) for i in self.conn.lrange(self.key, 0, -1)]
            if datatype == 'set':
                return [json.loads(i.decode('utf-8')) for i in self.conn.smembers(self.key)]
        except TypeError:
            return None

    def get_field(self, field):
        try:
            return json.loads(self.conn.hget(self.key, field))
        except TypeError:
            return None

    def get_index(self, value):
        try:
            return self.conn.zrank(self.key, json.dumps(value))
        except IndexError:
            return None

    def get_value(self, index):
        try:
            return json.loads(self.conn.zrange(self.key, index, index)[0])
        except IndexError:
            return None

    def get_datatype(self):
        return self.conn.type(self.key)