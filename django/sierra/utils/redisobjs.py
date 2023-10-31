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
        self.entries = []
        self.accumulators = []
        self.pipe = conn.pipeline()

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
        self.entries = []
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
    Private class with methods for encoding JSON in Redis data types.

    All methods are static. This is just to provide a convenient
    grouping and namespace for these functions.
    """

    @staticmethod
    def is_encoded_obj(obj):
        end_of_prefix = len(STR_OBJ_PREFIX) - 1
        return obj.conn.getrange(obj.key, 0, end_of_prefix) == STR_OBJ_PREFIX

    @staticmethod
    def encoded_obj(data):
        return ''.join([STR_OBJ_PREFIX, ujson.dumps(data)])

    @staticmethod
    def member(data):
        return ujson.dumps(data)

    @staticmethod
    def zset(data):
        member_encode = _RedisJsonEncoder.member
        return {
            member_encode(item): score for item, score in data
        }

    @staticmethod
    def list(data):
        member_encode = _RedisJsonEncoder.member
        return [member_encode(item) for item in data]

    @staticmethod
    def hash(data):
        member_encode = _RedisJsonEncoder.member
        return {k: member_encode(v) for k, v in iteritems(data)}

    @staticmethod
    def set(data):
        member_encode = _RedisJsonEncoder.member
        return [member_encode(item) for item in data]


class _RedisJsonDecoder(object):
    """
    Private class with methods for decoding from Redis to Python.

    All methods are static. This is just to provide a convenient
    grouping and namespace for these functions.
    """

    @staticmethod
    def encoded_obj(raw):
        if raw is not None:
            return ujson.loads(raw.lstrip(STR_OBJ_PREFIX))

    @staticmethod
    def list(raw):
        member_decode = _RedisJsonDecoder.member
        if raw:
            return [v if v is None else member_decode(v) for v in raw]
    
    @staticmethod
    def dict(raw):
        member_decode = _RedisJsonDecoder.member
        if raw:
            return {k: member_decode(v) for k, v in iteritems(raw)}

    @staticmethod
    def set(raw):
        member_decode = _RedisJsonDecoder.member
        if raw:
            return set([v if v is None else member_decode(v) for v in raw])

    @staticmethod
    def member(raw):
        if raw is not None:
            return ujson.loads(raw)

    @staticmethod
    def int(raw):
        member_decode = _RedisJsonDecoder.member
        if raw is not None:
            return int(member_decode(raw))


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
            zset_len = self.conn.zcard(self.key)
            if index is None or index > zset_len:
                index = zset_len
            else:
                if index < 0:
                    try_convert = index + zset_len
                    index = try_convert if try_convert > 0 else 0
                acc = acc or self.obj.make_result_accumulator()
                end_index = index + len(data) - 1
                self.pipe.add(
                    'zremrangebyscore', self.key, args=[index, end_index],
                    accumulator=acc
                )
        else:
            index = 0
        self.pipe.add(
            'zadd', self.key, args=[self.encoder.zset(
                ((v, index + i) for i, v in enumerate(data))
            )], accumulator=acc
        )
        if acc is not None and accumulator is None:
            self.pipe.mark_accumulator_pop()
        return self.pipe

    def list(self, data, accumulator, update=False, index=None):
        acc = accumulator
        if index is not None:
            llen = self.conn.llen(self.key) if update else 0
            if index < 0:
                try_convert = index + llen
                index = try_convert if try_convert > 0 else 0
            if index < llen:
                if len(data) > 1 and not acc:
                    acc = self.obj.make_result_accumulator()
                end_index = llen - index
                for i, value in enumerate(data[:end_index]):
                    args = [i + index, self.encoder.member(value)]
                    self.pipe.add('lset', self.key, args=args, accumulator=acc)
                data = data[end_index:]
            elif index > llen:
                data = [None] * (index - llen) + list(data)
        if data:
            self.pipe.add(
                'rpush', self.key, args=self.encoder.list(data),
                accumulator=acc
            )
        if acc is not None and accumulator is None:
            self.pipe.mark_accumulator_pop()
        return self.pipe

    def hash(self, data, accumulator):
        return self.pipe.add(
            'hset', self.key, kwargs={'mapping': self.encoder.hash(data)},
            accumulator=accumulator
        )

    def set(self, data, accumulator):
        return self.pipe.add(
            'sadd', self.key, args=self.encoder.set(data),
            accumulator=accumulator
        )

    def string(self, data, accumulator, update=False, index=None):
        strlen = self.conn.strlen(self.key) if update else 0
        if index is None:
            index = strlen
        elif index < 0:
            try_convert = index + strlen
            index = try_convert if try_convert > 0 else 0
        return self.pipe.add(
            'setrange', self.key, args=[index, data],
            accumulator=accumulator
        )

    def encoded_obj(self, data, accumulator):
        return self.pipe.add(
            'set', self.key, args=[self.encoder.encoded_obj(data)],
            accumulator=accumulator
        )


class _RedisGetter(object):
    """
    Private class that implements 'getting' behaviors for RedisObject.
    """
    
    def __init__(self, redis_object, decoder):
        self.obj = redis_object
        self.key = redis_object.key
        self.pipe = redis_object.pipe
        self.decoder = decoder

    def zset(self):
        return self.pipe.add(
            'zrange', self.key, args=[0, -1], callback=self.decoder.list
        )

    def list(self):
        return self.pipe.add(
            'lrange', self.key, args=[0, -1], callback=self.decoder.list
        )

    def hash(self):
        return self.pipe.add('hgetall', self.key, callback=self.decoder.dict)

    def set(self):
        return self.pipe.add('smembers', self.key, callback=self.decoder.set)

    def string(self):
        return self.pipe.add('get', self.key)

    def encoded_obj(self):
        return self.pipe.add(
            'get', self.key, callback=self.decoder.encoded_obj
        )

    def none(self):
        return self.pipe.add('get', NONE_KEY)

    def hash_field(self, field):
        return self.pipe.add(
            'hget', self.key, args=[field], callback=self.decoder.member
        )

    def hash_fields(self, fields):
        return self.pipe.add(
            'hmget', self.key, args=[fields], callback=self.decoder.list
        )

    def zset_index(self, value):
        encoded_val = self.obj.setter.encoder.member(value)
        return self.pipe.add(
            'zscore', self.key, args=[encoded_val],
            callback=lambda score: score if score is None else int(score)
        )

    def zset_indexes(self, values):
        encoded_vals = [self.obj.setter.encoder.member(v) for v in values]
        return self.pipe.add(
            'zmscore', self.key, args=[encoded_vals],
            callback=lambda scores: [
                score if score is None else int(score) for score in scores
            ] if scores else None
        )

    def list_index(self, value):
        callback = make_single_or_list_callback()
        encoded_val = self.obj.setter.encoder.member(value)
        return self.pipe.add(
            'lpos', self.key, args=[encoded_val], kwargs={'count': 0},
            callback=callback
        )

    def list_indexes(self, values):
        acc = self.obj.make_result_accumulator()
        callback = make_single_or_list_callback()
        encoded_vals = (self.obj.setter.encoder.member(v) for v in values)
        for encoded_val in encoded_vals:
            self.pipe.add(
                'lpos', self.key, args=[encoded_val], kwargs={'count': 0},
                callback=callback, accumulator=acc
            )
        self.pipe.mark_accumulator_pop()
        return self.pipe

    def zset_value(self, index):
        return self.pipe.add(
            'zrange', self.key, args=[index, index],
            callback=lambda v: self.decoder.member(v[0]) if v else None
        )

    def zset_values(self, start, end):
        return self.pipe.add(
            'zrange', self.key, args=[start, end], callback=self.decoder.list
        )

    def list_value(self, index):
        return self.pipe.add(
            'lindex', self.key, args=[index], callback=self.decoder.member
        )

    def list_values(self, start, end):
        return self.pipe.add(
            'lrange', self.key, args=[start, end], callback=self.decoder.list
        )

    def get_key(self):
        return getattr(self, self.obj.rtype, self.none)()


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
        self.setter = _RedisSetter(self, _RedisJsonEncoder)
        self.getter = _RedisGetter(self, _RedisJsonDecoder)

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
        MUCH faster access compared to a plain list, so use this if
        possible. Default is True (zset).

        If 'update' is False, then any existing data at the given key
        is deleted before setting the new value.

        If 'update' is True, it updates the existing key according to
        its data type, using 'index' as appropriate.

          - For a list, zset, or string: replaces data starting at the
            given 'index' value, or tacks new values to the end if
            'index' is None. Negative indexes set values starting from
            the end of the list.
          - For a list or string, if 'index' is larger than the size of
            the data, then it pads the data appropriately -- it adds
            None/null list members or null bytes.
          - For a zset, if 'index' is larger than the size of the data,
            it treats it as though 'index' is None, tacking values onto
            the end of the zset. (Because zset members must be unique,
            we cannot pad them with null values like we can lists and
            strings.)
          - For a set: adds the data values, like set.update. The
            'index' value is ignored.
          - For a hash: updates existing fields and adds new ones, like
            dict.update. The 'index' value is ignored.

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
            self.getter.none()
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

    def get(self):
        """
        Fetches and returns the data from Redis using the current key.

        It attempts to rebuild your original Python data type as best
        it can, except tuples get converted to lists. All strings are
        run through ujson.loads().

        If the object's key does not exist in Redis, it retrieves None.

        If self.defer is False, it executes the operation immediately
        and returns the retrieved value(s). If self.defer is True, it
        queues up the operation(s) on self.pipe and returns that.
        """
        self.getter.get_key()
        if self.defer:
            return self.pipe
        return self.pipe.execute()[-1]

    def get_field(self, *fields):
        """
        Fetches and returns value(s) for the given hash field(s).

        If you request a single field, this is the equivalent of
        mydict.get(field). If you provide multiple fields, it's equal
        to [mydict.get(f) for f in fields]. It lets you get a subset of
        a hash without fetching the whole thing from Redis.

        Returns None if the current key does not exist or the Redis
        data type isn't 'hash' -- i.e., if you didn't set this using a
        dict. If you request the value from one field, it returns that.
        Otherwise, it returns a list of values corresponding to the
        provided fields. (If a field does not exist, None is used.)

        If self.defer is False, it executes the operation immediately
        and returns the retrieved value(s). If self.defer is True, it
        queues up the operation(s) on self.pipe and returns that.
        """
        multi = len(fields) > 1
        if self.rtype == 'hash' and fields:
            if multi:
                self.getter.hash_fields(fields)
            else:
                self.getter.hash_field(fields[0])
        else:
            self.getter.none()
        if self.defer:
            return self.pipe
        return self.pipe.execute()[-1]

    def get_index(self, *values):
        """
        Fetches/returns the list/zset ind positions for 1+ values.

        If you provide one value, this is the equivalent of
        my_list.index(v). For multiple values, it's like
        [my_list.index(v) for v in values]. This way, you can get the
        index positions for one or more values without having to fetch
        the entire data structure from Redis.

        The retrieved value is None if the current key does not exist
        or the Redis data type isn't 'list' or 'zset' -- i.e., if you
        didn't set this using a list/tuple. If you only have one value,
        it retrieves that one position. Otherwise, it retrieves a list
        of positions. None is used for values that don't exist. If your
        Redis data is a list (non-unique values) and a value occurs
        more than once, a nested list with all occurrences is returned.

        If self.defer is False, it executes the operation immediately
        and returns the retrieved value(s). If self.defer is True, it
        queues up the operation(s) on self.pipe and returns that.
        """
        multi = len(values) > 1
        if self.rtype in ('zset', 'list') and values:
            if multi:
                plural = 'es'
            else:
                plural = ''
                values = values[0]
            getattr(self.getter, f'{self.rtype}_index{plural}')(values)
        else:
            self.getter.none()
        if self.defer:
            return self.pipe
        return self.pipe.execute()[-1]

    def get_value(self, start, end=None):
        """
        Fetches/returns vals for a range of list/zset index positions.

        This is the equivalent of: my_list[start:end] (except 'end' is
        inclusive). It lets you get a subset of values without having
        to fetch the entire data structure from Redis.

        You may provide a negative value for 'start' or 'end', which
        counts from the end of the list/zset.

        The retrieved value is None if the current key does not exist
        or the Redis data type isn't 'list' or 'zset' -- i.e., if you
        didn't set this using a list/tuple. If your range is a range of
        one item, it retrieves that item. Otherwise, it retrieves a
        list of values.

        If self.defer is False, it executes the operation immediately
        and returns the retrieved value(s). If self.defer is True, it
        queues up the operation(s) on self.pipe and returns that.
        """
        multi = end is not None
        if self.rtype in ('zset', 'list'):
            if multi:
                plural = 's'
                args = [start, end]
            else:
                plural = ''
                args = [start]
            getattr(self.getter, f'{self.rtype}_value{plural}')(*args)
        else:
            self.getter.none()
        if self.defer:
            return self.pipe
        return self.pipe.execute()[-1]
