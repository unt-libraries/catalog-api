from __future__ import absolute_import

import ujson

import redis
from django.conf import settings
from six import iteritems


REDIS_CONNECTION = redis.StrictRedis(
    decode_responses=True, **settings.REDIS_CONNECTION
)
NONE_KEY = '~~~|_DOESNOTEXIST_|~~~'
STR_OBJ_PREFIX = '~~~|_STR_OBJ_|~~~'


class Accumulator(object):
    """
    Class for making accumulators to use with Pipeline.add.

    An accumulator wraps a Python type and a callable method, putting
    a generic 'add' method in front, allowing a consistent interface
    for accumulating items into a collective variable -- whether it's
    adding to a list or set, updating a dict, compiling a string, or
    performing a reduction.
    """

    def __init__(self, acctype, accmethod):
        """
        Inits the accumulator instance with 'acctype' and 'accmethod'.

        The 'acctype' is the Python type of the accumulated value.

        The 'accmethod' is the function or method to use to add an item
        to the accumulated value. It should take two args: 1) the
        accumulated value and 2) the item to add. It should either
        add the item to the accumulated value directly (mutating it)
        and return None, or it should return the new combined value.
        """
        self.acctype = acctype
        self.accmethod = accmethod
        self.reset()

    def reset(self):
        """
        Resets the accumulated value to empty.
        """
        self.accumulated = self.acctype()

    def pop_all(self):
        """
        Resets the accumulated value and returns it.
        """
        result = self.accumulated
        self.reset()
        return result

    def push(self, item):
        """
        Adds 'item' to the accumulated value and returns it.
        """
        val = self.accmethod(self.accumulated, item)
        if val is not None:
            self.accumulated = val
        return self.accumulated


class Pipeline(object):
    """
    Class that wraps 'redis.client.Pipeline' to add two features.

    The two new features are:
    - Callback functions, for manipulating results from Redis before
      returning them, such as to cast to a specific Python type.
    - Accumulators, for combining singular return values from Redis
      into one before returning them. Such as, to put return values
      from several LINDEX calls into one list.

    Use Pipeline instances the same way you use redis.client.Pipelines.
    But instead of calling the desired method directly, add it to the
    pipeline by passing the method name, args, and kwargs to 'add'.
    Then call 'execute' as usual to run the pipeline and return
    results.

    The reason to use 'add' is that you can also include a callback and
    an accumulator along with your command. The return value from the
    command is passed through each (if provided) before it's added to
    the results.

    Example: You have multiple lists in Redis and want to return items
    from each, putting them into lists based on the originating lists,
    without having to run a compilation process on the final results.

    # key 'prefix:my_int_list' == ['1', '2', '3', '4', '5']
    # key 'prefix:my_str_list' == ['a', 'b', 'c', 'd', 'e']
    # key 'prefix:my_str' == 'foobarbaz'
    pl = Pipeline()
    accumulator = Accumulator(list, list.append)
    pl.add(
        'lindex', 'prefix:my_int_list', args=[0], callback=int,
        accumulator=accumulator
    )
    pl.add(
        'lindex', 'prefix:my_int_list', args=[2], callback=int,
        accumulator=accumulator
    )
    pl.add(
        'lindex', 'prefix:my_int_list', args=[4], callback=int,
        accumulator=accumulator
    )
    pl.mark_accumulator_pop()
    pl.add(
        'lindex', 'prefix:my_str_list', args=[1],
        accumulator=accumulator
    )
    pl.add(
        'lindex', 'prefix:my_str_list', args=[3],
        accumulator=accumulator
    )
    pl.mark_accumulator_pop()
    pl.add('get', 'prefix:my_str')
    results = pl.execute()

    The final 'results' value would contain:
    [[1, 3, 5], ['b', 'd'], 'foobarbaz']
    """

    def __init__(self, conn=REDIS_CONNECTION):
        """
        Inits a new Pipeline instance.
        """
        self.pipe = conn.pipeline()
        self.reset(False)

    def reset(self, reset_pipe=True):
        """
        Resets this Pipeline object's state.

        Use 'reset_pipe=True' to reset self.pipe. Default is True.
        """
        self.entries = []
        self.accumulators = []
        self.pending_cache = {}
        if reset_pipe:
            self.pipe.reset()

    def add(self, cmd, key, args=[], kwargs={}, callback=None,
            accumulator=None):
        """
        Queues up the next command you wish to add to the pipeline.

        Provide the 'method_name' for the redis.client.Pipeline method
        you want to run, the Redis 'key' you want to manipulate, and
        any additional 'args' and 'kwargs' to pass to that method.

        Optionally, provide a 'callback' function that takes one
        value (the result from the Redis command) and returns a final
        value, such as if you want to cast strings to ints.

        Optionally, provide an 'accumulator' (Accumulator instance) to
        use for adding the result from this Redis command to a
        container, like a list.

        Returns the Pipeline instance so you can string together
        multiple 'add's, if desired.
        """
        getattr(self.pipe, cmd)(key, *args, **kwargs)
        self.entries.append((callback, accumulator, False))
        if accumulator is not None and accumulator not in self.accumulators:
            self.accumulators.append(accumulator)
        return self

    def noop(self):
        """
        Queues up a no-op command that adds None to pipeline output.
        """
        self.add('get', NONE_KEY)

    def mark_accumulator_pop(self):
        """
        Mark a point at which to pop acc contents during execution.
        """
        if len(self.entries):
            callback, accumulator, _ = self.entries.pop()
            self.entries.append((callback, accumulator, True))
        return self

    def execute(self):
        """
        Executes the current command queue and returns the results.

        The returned results contains a list of return values from
        Redis, in command order. If you used accumulators, then you
        will have one or more nested objects containing the accumulated
        results.

        The command queue is cleared when executed.
        """
        results = []
        raw = self.pipe.execute()
        for result, entry in zip(raw, self.entries):
            callback, accumulator, acc_pop = entry
            if callback is not None:
                result = callback(result)
            if accumulator is None:
                results.append(result)
            else:
                accumulator.push(result)
                if acc_pop:
                    results.append(accumulator.pop_all())
        for accumulator in self.accumulators:
            if accumulator.accumulated:
                results.append(accumulator.pop_all())
        self.reset(False)
        return results


def make_single_or_list_callback(decode_list=None):
    """
    Make a callback function to decode a list or unpack/decode one val.

    Use this when your Redis query returns a list of values, and you
    want to return the list (with values decoded) OR just a single
    decoded value if the list has one item.

    Provide the 'decode_list' method or function you want to use that
    will decode values in the raw list from Redis.
    """
    def callback(raw):
        if raw:
            vals = decode_list(raw) if decode_list else raw
            nvals = len(vals) 
            return vals[0] if nvals == 1 else vals
    return callback


class _RedisJsonEncoder(object):
    """
    Private class with methods for encoding Redis data types.

    Contains methods named with a Redis type that return a generator
    for encoding items. Ultimately the caller is responsible for
    converting the generated items to the needed args for Redis.

    'encoded_obj' is a special type representing a JSON-serializable
    object of an unknown type. This method doesn't return a generator,
    since it's a one-shot encoding.

    A 'member' property defines how individual members of zsets, lists,
    hashes, and sets should be encoded.
    """

    def __init__(self):
        self.member = ujson.dumps

    @staticmethod
    def is_encoded_obj(obj):
        end_of_prefix = len(STR_OBJ_PREFIX) - 1
        return obj.conn.getrange(obj.key, 0, end_of_prefix) == STR_OBJ_PREFIX

    def encoded_obj(self, data):
        return ''.join([STR_OBJ_PREFIX, self.member(data)])

    def zset(self, data):
        # 'data' should be a list of (item, score) tuples
        return ((self.member(item), score) for item, score in data)

    def list(self, data):
        return (self.member(item) for item in data)

    def hash(self, data):
        # 'data' should be the dict representing the hash
        return ((k, self.member(v)) for k, v in iteritems(data))

    def set(self, data):
        return (self.member(item) for item in data)


class _RedisJsonDecoder(object):
    """
    Private class with methods for decoding from Redis to Python.

    Contains methods named with the target Python type, which may or
    may not be the type returned from redis-py. They return the named
    type.

    'encoded_obj' is a special type representing a JSON-serializable
    object of an unknown type.

    The 'member' method/property is used to decode individual list,
    dict, set, etc. elements.
    """

    @staticmethod
    def member(raw):
        return None if raw is None else ujson.loads(raw)

    def encoded_obj(self, raw):
        return self.member(raw.lstrip(STR_OBJ_PREFIX))

    def list(self, raw):
        return [self.member(v) for v in raw] if raw else None

    def dict(self, raw):
        return {k: self.member(v) for k, v in iteritems(raw)} if raw else None

    def set(self, raw):
        return set([self.member(v) for v in raw]) if raw else None


class _RedisSetter(object):
    """
    Private class that implements 'setting' behaviors for RedisObject.
    """
    conn = REDIS_CONNECTION
    types_to_rtype = {
        'list_unique': 'zset',
        'tuple_unique': 'zset',
        'list': 'list',
        'tuple': 'list',
        'dict': 'hash',
        'str': 'string',
        'set': 'set'
    }
    rtype_compatibility = {
        'zset': {'types': (list, tuple), 'label': 'list or tuple'},
        'list': {'types': (list, tuple), 'label': 'list or tuple'},
        'string': {'types': (str,), 'label': 'string'},
        'hash': {'types': (dict,), 'label': 'dict'},
        'set': {'types': (set,), 'label': 'set'},
        'encoded_obj': {
            'types': tuple(),
            'label': 'any JSON-serializable type except list, tuple, string, '
                     'dict, or set'
        }
    }
    default_rtype = 'encoded_obj'

    def __init__(self, redis_object, encoder):
        self.obj = redis_object
        self.key = redis_object.key
        self.pipe = redis_object.pipe
        self.encoder = encoder
        self.bypass_encoding = False

    def encode(self, method, data):
        if self.bypass_encoding:
            return data
        return getattr(self.encoder, method)(data)

    def get_rtype_from_data(self, data, force_unique):
        ptype = type(data)
        unstr = '_unique' if ptype in (list, tuple) and force_unique else ''
        type_key = ''.join([ptype.__name__, unstr])
        return self.types_to_rtype.get(type_key) or self.default_rtype

    def get_cmp_ptypes(self, rtype):
        return self.rtype_compatibility.get(rtype, {}).get('types', str)

    def get_cmp_ptypes_label(self, rtype):
        entry = self.rtype_compatibility.get(rtype, {})
        return entry.get('label', self.default_rtype)

    def zset(self, data, accumulator, update=False, index=None):
        acc = accumulator
        if update:
            prev_end = self.pipe.pending_cache.get(self.key)
            if prev_end is None:
                prev_max = self.conn.zrange(self.key, -1, -1, withscores=True)
                prev_end = int(prev_max[0][1]) if prev_max else -1
            if index is None:
                offset = prev_end + 1
            else:
                if index < 0:
                    try_convert = prev_end + index + 1
                    offset = try_convert if try_convert > 0 else 0
                else:
                    offset = index
            end = offset + len(data) - 1
            if offset <= prev_end:
                acc = acc or self.obj.make_result_accumulator()
                self.pipe.add(
                    'zremrangebyscore', self.key, args=[offset, end],
                    accumulator=acc
                )
            zset_end = end if end > prev_end else prev_end
        else:
            offset = 0 if (index is None or index < 0) else index
            zset_end = offset + len(data) - 1
        self.pipe.add(
            'zadd', self.key, args=[dict(self.encode(
                'zset', ((v, offset + i) for i, v in enumerate(data))
            ))], accumulator=acc
        )
        self.pipe.pending_cache[self.key] = zset_end
        if acc is not None and accumulator is None:
            self.pipe.mark_accumulator_pop()
        return self.pipe

    def list(self, data, accumulator, update=False, index=None):
        acc = accumulator
        if update:
            llen = self.pipe.pending_cache.get(
                self.key, self.conn.llen(self.key)
            )
        else:
            llen = 0
        if index is not None:
            if index < 0:
                try_convert = index + llen
                index = try_convert if try_convert > 0 else 0
            if index < llen:
                len_data = len(data)
                if len_data > 1 and not acc:
                    acc = self.obj.make_result_accumulator()
                end_index = llen - index
                for i, value in enumerate(data[:end_index]):
                    args = [i + index, self.encode('member', value)]
                    self.pipe.add('lset', self.key, args=args, accumulator=acc)
                data = data[end_index:]
            elif index > llen:
                if self.bypass_encoding:
                    # 'bypass_encoding' means the user data is already
                    # encoded, but we still need to encode any values
                    # we're adding ourselves, like None padding.
                    none = self.encoder.member(None)
                else:
                    none = None
                data = [none] * (index - llen) + list(data)
        if data:
            self.pipe.add(
                'rpush', self.key, args=self.encode('list', data),
                accumulator=acc
            )
            llen += len(data)
        self.pipe.pending_cache[self.key] = llen
        if acc is not None and accumulator is None:
            self.pipe.mark_accumulator_pop()
        return self.pipe

    def hash(self, data, accumulator):
        return self.pipe.add(
            'hset', self.key,
            kwargs={'mapping': dict(self.encode('hash', data))},
            accumulator=accumulator
        )

    def set(self, data, accumulator):
        return self.pipe.add(
            'sadd', self.key, args=self.encode('set', data),
            accumulator=accumulator
        )

    def string(self, data, accumulator, update=False, index=None):
        if update:
            strlen = self.pipe.pending_cache.get(
                self.key, self.conn.strlen(self.key)
            )
        else:
            strlen = 0
        if index is None:
            index = strlen
        elif index < 0:
            try_convert = index + strlen
            index = try_convert if try_convert > 0 else 0
        new_len = len(data) + index
        strlen = new_len if new_len > strlen else strlen
        self.pipe.pending_cache[self.key] = strlen
        return self.pipe.add(
            'setrange', self.key, args=[index, data],
            accumulator=accumulator
        )

    def encoded_obj(self, data, accumulator):
        return self.pipe.add(
            'set', self.key, args=[self.encode('encoded_obj', data)],
            accumulator=accumulator
        )


class _RedisGetter(object):
    """
    Private class that implements 'getting' behaviors for RedisObject.
    """
    lookup_types = {
        'hash': {'valid': ('field',), 'default': 'field'},
        'list': {'valid': ('value', 'values', 'index'), 'default': 'index'},
        'zset': {'valid': ('value', 'values', 'index'), 'default': 'index'},
        'string': {'valid': ('index',), 'default': 'index'},
        'set': {'valid': ('value_exists', 'values_exist'),
                'default': 'value_exists'},
        'encoded_obj': {'valid': (), 'default': None},
        'none': {'valid': (), 'default': None}
    }
    multi_types = {
        'values': 'value',
        'values_exist': 'value_exists'
    }

    def __init__(self, redis_object, decoder):
        self.obj = redis_object
        self.key = redis_object.key
        self.pipe = redis_object.pipe
        self.decoder = decoder

    def _process_lookup(self, rtype, lookup, lookup_type):
        if lookup is None and lookup_type is None:
            return lookup, 'all', True
        if lookup_type is None:
            lookup_type = self.lookup_types[rtype]['default']
        if lookup_type in self.lookup_types[rtype]['valid']:
            if lookup_type == 'index':
                if isinstance(lookup, int):
                    multi = False
                    lookup = (lookup, lookup)
                else:
                    multi = True
            elif lookup_type == 'field':
                multi = isinstance(lookup, (list, tuple))
            elif lookup_type in self.multi_types:
                multi = True
                lookup_type = self.multi_types[lookup_type]
            else:
                multi = False
            if lookup or lookup == 0:
                return lookup, lookup_type, multi
        return lookup, None, None

    def add_to_pipe(self, rtype, lookup, lookup_type):
        lookup, lookup_type, multi = self._process_lookup(
            rtype, lookup, lookup_type
        )
        if lookup_type is None:
            return self.none()
        if lookup_type == 'all':
            return getattr(self, rtype)()
        return getattr(self, f'{rtype}_{lookup_type}')(lookup, multi)

    def zset(self):
        return self.pipe.add(
            'zrange', self.key, args=[0, -1], callback=self.decoder.list
        )

    def zset_index(self, index_range, multi):
        callback = self.decoder.list
        if not multi:
            callback = make_single_or_list_callback(callback)
        return self.pipe.add(
            'zrange', self.key, args=index_range, callback=callback
        )

    def zset_value(self, values, multi):
        if multi:
            encoded_vals = list(self.obj.setter.encoder.list(values))
            return self.pipe.add(
                'zmscore', self.key, args=[encoded_vals],
                callback=lambda scores: [
                    score if score is None else int(score) for score in scores
                ] if scores else None
            )
        return self.pipe.add(
            'zscore', self.key,
            args=[self.obj.setter.encoder.member(values)],
            callback=lambda score: score if score is None else int(score)
        )

    def list(self):
        return self.pipe.add(
            'lrange', self.key, args=[0, -1], callback=self.decoder.list
        )

    def list_index(self, index_range, multi):
        callback = self.decoder.list
        if not multi:
            callback = make_single_or_list_callback(callback)
        return self.pipe.add(
            'lrange', self.key, args=index_range, callback=callback
        )

    def list_value(self, values, multi):
        callback = make_single_or_list_callback()
        kwargs = {'count': 0}
        if multi:
            acc = self.obj.make_result_accumulator()
            for value in self.obj.setter.encoder.list(values):
                self.pipe.add(
                    'lpos', self.key, args=[value], kwargs=kwargs,
                    callback=callback, accumulator=acc
                )
            self.pipe.mark_accumulator_pop()
            return self.pipe
        return self.pipe.add(
            'lpos', self.key, args=[self.obj.setter.encoder.member(values)],
            kwargs=kwargs, callback=callback
        )

    def hash(self):
        return self.pipe.add('hgetall', self.key, callback=self.decoder.dict)

    def hash_field(self, fields, multi):
        if multi:
            return self.pipe.add(
                'hmget', self.key, args=fields, callback=self.decoder.list
            )
        return self.pipe.add(
            'hget', self.key, args=[fields], callback=self.decoder.member
        )

    def set(self):
        return self.pipe.add(
            'smembers', self.key, callback=self.decoder.set
        )

    def set_value_exists(self, values, multi):
        if multi:
            encoded_vals = list(self.obj.setter.encoder.list(values))
            return self.pipe.add(
                'smismember', self.key, args=[encoded_vals],
                callback=lambda raw: [bool(v) for v in raw]
            )
        return self.pipe.add(
            'sismember', self.key,
            args=[self.obj.setter.encoder.member(values)], callback=bool
        )

    def string(self):
        return self.pipe.add('get', self.key)

    def string_index(self, index_range, multi):
        return self.pipe.add(
            'getrange', self.key, args=index_range,
            callback=lambda raw: raw or None
        )

    def encoded_obj(self):
        return self.pipe.add(
            'get', self.key, callback=self.decoder.encoded_obj
        )

    def none(self):
        return self.pipe.noop()


class RedisObject(object):
    """
    Models a Redis key / object to simplify Redis interaction.

    Instantiate this wherever you need to interact with a Redis key.
    The goal is to provide methods for seamless interaction, using the
    best and most appropriate Redis data structures for the supplied
    Python data. JSON is used to encode nested data structures in Redis
    and decode them back into Python.

    The class attribute 'conn' contains the underlying redis-py
    connection object, which you can use if you need to send commands
    directly to Redis.

    Summary / example:

    rdata = RedisObject('key_namespace', 'my_rad_key')
    rdata.set([{
        'id': 1,
        'colors': ['red', 'blue'],
        'values': [9, 5, 3, 8],
    }, {
        'id': 2,
        'colors': ['purple', 'green'],
        'values': [3, 6, 5]
    }])
    # Later in the code, or in a totally different module:
    my_rad_obj = RedisObject('key_namespace', 'my_rad_key')
    which_colors = my_rad_obj.get_value(1)['colors']
    assert which_colors == ['purple', 'green']

    You can also use this with Pipeline objects for deferred execution,
    if you want to send multiple commands at once to Redis, instead of
    performing each operation and returning results immediately. See
    method docstrings for more information.
    """
    conn = REDIS_CONNECTION

    def __init__(self, entity, id_, pipe=None, defer=False):
        """
        Initializes a new RedisObject.

        Provide two arguments, an 'entity' string and an 'id' string.
        These are combined to create the full Redis key -- e.g.,
        entity:id. You can access the combined key via the 'key'
        instance attribute.

        Optionally, if you want to use a pipeline to delay execution so
        you can send commands to Redis in batches, use 'defer=True'. If
        you have a pipeline of your own you want to use for that,
        provide that via the 'pipe' arg.

        When 'defer' is True, commands are queued on the pipe but not
        executed. You have to call 'execute' on self.pipe to execute
        them. Default for 'defer' is False.
        """
        self.entity = entity
        self.id = id_
        self.key = f'{entity}:{id_}'
        self.pipe = pipe or Pipeline()
        self.defer = defer
        self._rtype = None
        self.setter = _RedisSetter(self, _RedisJsonEncoder())
        self.getter = _RedisGetter(self, _RedisJsonDecoder())

    @property
    def rtype(self):
        """
        The Redis data type used for the object with the current key.
        """
        if self._rtype is None:
            self._rtype = self.conn.type(self.key)
            if self._rtype == 'string':
                if self.setter.encoder.is_encoded_obj(self):
                    self._rtype = 'encoded_obj'
        return self._rtype

    def make_result_accumulator(self):
        return Accumulator(list, list.append)

    def set(self, data, force_unique=None, update=False, index=None):
        """
        Sets the given data in Redis at the current key.

        The 'data' can be any type that is JSON-serializable. Strings
        and numbers get converted to JSON. Lists, sets, and dicts are
        stored as specialized types for more granular access; each
        member is converted to JSON.

          - List/tuple with force_unique=True: sorted set (zset).
          - List/tuple with force_unique=False: list.
          - Set: set.
          - Dictionary: hash.

        Other data types just get converted to JSON and are stored as
        strings.

        The 'force_unique' option only applies to lists/tuples. It
        determines whether Redis stores the data as a zset (thereby
        forcing members to be unique) or a plain list. A zset allows
        MUCH faster access compared to a plain list, so use this any
        time you have unique data. Default is True (zset). Note that,
        if you do provide a duplicate item when setting a zset, its
        position resets each time it appears -- so its final position
        will be wherever it appears LAST.

        Note also that a zset works by keeping a "score" associated
        with each member, and the scores determine the sort order.
        RedisObject uses these to store index positions and thus
        provides sortable set functionality (mostly) transparently --
        i.e., you usually don't have to worry about the scores.
        However, when duplicates appear in your data, it leaves holes
        in the score sequence where all but the last instance of each
        duplicate fell. (E.g., a b c b => a|0, c|2, b|3.) This doesn't
        affect the sort order, but if you need to update the zset in
        place starting at a specific 'index' value, then you need to be
        aware of where the holes are. Given the 'a b c b' example,
        putting a value at index position 1 will insert it between a
        and c, not overwrite c.

        If 'update' is False, then any existing data at the given key
        is deleted before setting the new value.

        If 'update' is True, it updates the existing key according to
        its data type, using 'index' as appropriate.

          - For a set: adds the data values, like set.update. The
            'index' value is ignored.
          - For a hash: updates existing fields and adds new ones, like
            dict.update. The 'index' value is ignored.
          - For a list, zset, or string: replaces data starting at the
            given 'index' value, or tacks new values to the end if
            'index' is None. Negative indexes set values starting from
            the end of the list.
          - For a list or string, if 'index' is larger than the size of
            the data, then it pads the data appropriately -- it adds
            None/null list members or null bytes.
          - Zset behavior is more complicated. It doesn't have indexes
            per se; instead, sort order is determined by a "score"
            associated with each member, in Redis. For our purposes we
            simply store the index position as the score. When you
            first set a zset, scores are incremental unless your data
            has duplicates. E.g.: a b c => a|0, b|1, c|2. BUT,
            a c b c => a|0, b|2, c|3. When you 'get' each of these,
            they return the same value, (['a', 'b', 'c']). But they
            behave differently when you set 'index' position 1. If you
            issue a command to set position 1 to 'd' -- with the first,
            'b' is at position 1 and gets overwritten: a|0, d|1, c|2.
            The second has nothing at position 1, so it inserts the new
            value between 'a' and 'b': a|0, d|1, b|2, c|3.
          - If you provide a zset 'index' larger than the size of the
            data, it doesn't pad with None/null the way a list or
            string does because zset members must be unique. However,
            since it converts the index positions to scores, you end up
            with an invisible score gap. E.g., say you start with
            'a b c', and you add 'd e f' at position 10. You'll have
            a|0, b|1, c|2, d|10, e|11, f|12. From there, setting values
            at positions 3-9 will insert them between b and c.

        Generally: when you're working with zsets and you need to
        update one in place, you must account for how index values are
        mapped to scores.

        Other types, labeled 'encoded_obj', cannot be updated -- data
        is reset whether 'update' is True or False.

        When updating existing data, your new data values must be
        compatible with the stored type. E.g., if you wish to add to a
        list or zset, you must provide a list or a tuple.

        The default for 'update' is False, and the default for 'index'
        is None.

        If self.defer is False, it executes the operation immediately
        and returns the original data value. If self.defer is True, it
        queues up the operation(s) on self.pipe and returns that.
        """
        if data or data == 0:
            kwargs = {}
            if force_unique is None and self.rtype in ('zset', 'list'):
                force_unique = self.rtype == 'zset' if update else True
            drtype = self.setter.get_rtype_from_data(data, force_unique)
            if drtype in ('zset', 'list', 'string'):
                kwargs = {'update': update, 'index': index}
            if update:
                if self.rtype not in ('none', drtype):
                    cmp_ptypes = self.setter.get_cmp_ptypes_label(self.rtype)
                    raise TypeError(
                        f'Cannot update existing {self.rtype} data with '
                        f'{drtype} data for key {self.key}. You must '
                        f'provide data of a compatible type: {cmp_ptypes}.'
                    )
                self.pipe = getattr(self.setter, drtype)(data, None, **kwargs)
            else:
                acc = self.make_result_accumulator()
                self.pipe.add('delete', self.key, accumulator=acc)
                self.pipe = getattr(self.setter, drtype)(data, acc, **kwargs)
                self.pipe.mark_accumulator_pop()
            self._rtype = drtype
        elif update:
            self.pipe.noop()
        else:
            self.pipe.add('delete', self.key)
        if self.defer:
            return self.pipe
        self.pipe.execute()
        return data

    def set_field(self, mapping):
        """
        Sets one or more field values using the given mapping.

        This is the equivalent of: my_dict.update(mapping). It is a
        convenience method that just calls 'set' with 'update=True'.
        For more information, see the 'set' method.
        """
        return self.set(mapping, update=True)

    def set_value(self, index, *values):
        """
        Sets one or more data values starting at the given index pos.

        This is the equivalent of: my_list[index:] = values. It is a
        convenience method that just calls 'set' with 'update=True'.
        For more information, see the 'set' method.
        """
        rval = self.set(values, update=True, index=index)
        if not self.defer and len(values) == 1:
            return rval[0]
        return rval

    def get(self, lookup=None, lookup_type=None):
        """
        Fetches and returns the data from Redis using the current key.

        If no lookup criteria is provided, it fetches the entire data
        structure and attempts to rebuild your original Python data
        type as best it can. (But be careful when fetching very large
        amounts of data!)

        You may provide optional lookup criteria: a 'lookup' value and
        a 'lookup_type' (a string). Valid lookup types vary by the
        Redis data type. If a lookup is provided with no lookup_type,
        then a default lookup type will be assumed, depending on the
        Redis data type.

          - String. Supports lookup_type 'index': for 'lookup', provide
            an int value to get a single character, or a (start, end)
            tuple to get a range (inclusive).
          - List & zset. Support lookup_type 'index', same as for
            strings. Also support lookup_types 'value' and 'values'.
            Type 'value' returns the index position for one lookup
            value; 'values' returns a list of positions given multiple
            values. Default lookup type for lists and zsets is 'index.'
          - Hash. Supports lookup_type 'field', which gets and returns
            multiple hash values given a lookup field or list of
            fields.
          - Set. Supports lookup_types 'value_exists', and
            'values_exist'. Type 'value_exists' returns True if the
            lookup value belongs to the set or False if not;
            'values_exist' checks a list of values. Default is
            'value_exists'.

        If the object's key does not exist in Redis or if the lookup
        does not make sense given the type of data in Redis, it returns
        None. A lookup that fails also returns None. (In context of a
        lookup type that returns a list of values, one failed lookup
        returns None in the appropriate list position.)

        If self.defer is False, it executes the operation immediately
        and returns the retrieved value(s). If self.defer is True, it
        queues up the operation(s) on self.pipe and returns that.
        """
        self.getter.add_to_pipe(self.rtype, lookup, lookup_type)
        if self.defer:
            return self.pipe
        return self.pipe.execute()[-1]

    def get_field(self, *fields):
        """
        Fetches the value(s) for the given hash field(s).

        This is the equivalent of mydict.get(field) for each field you
        provide. It is a convenience method that just calls 'get' with
        your field list. For more information, see the 'get' method.
        """
        if len(fields) == 1:
            return self.get(fields[0], 'field')
        return self.get(fields, 'field')

    def get_index(self, *values):
        """
        Fetches the list/zset index positions for 1 or more values.

        This is the equivalent of mylist.index(v) for each value you
        provide. It is a convenience method that just calls 'get' with
        your values list. For more information, see the 'get' method.
        """
        if len(values) == 1:
            return self.get(values[0], 'value')
        return self.get(values, 'values')

    def get_value(self, start, end=None):
        """
        Fetches the values for a range of list/zset index positions.

        This is the equivalent of: my_list[start:end] (except 'end' is
        inclusive). It is a convenience method that just calls 'get',
        creating an appropriate lookup based on the 'start' and 'end'
        values you provide. For more information, see the 'get' method.
        """
        lookup = start if end is None else (start, end)
        return self.get(lookup, 'index')
