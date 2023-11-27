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

    @classmethod
    def from_ptype(cls, ptype, multi=False):
        if ptype == dict:
            return cls(ptype, ptype.update)
        if ptype == set:
            if multi:
                return cls(ptype, ptype.update)
            return cls(ptype, ptype.add)
        if ptype == list:
            if multi:
                return cls(ptype, ptype.extend)
            return cls(ptype, ptype.append)
        if ptype == str:
            return cls(ptype, lambda coll, new: f"{coll}{new}")

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


def normalize_index_lookup(index, llen, default=0, low_default=None):
    """
    Returns a normalized Redis index lookup.

    The 'index' arg is the raw index value to convert. It could be None
    or it could be negative.

    The 'llen' arg should be the total number of items in the data
    structure.

    The 'default' kwarg defines what is returned when 'index' is
    None.

    The 'low_default' kwarg defines what is returned when it tries to
    convert a negative value to a positive but still gets a negative
    (i.e., when a negative index is out of range).
    """
    if index is None:
        return default
    if index < 0 and llen is not None:
        converted = llen + index
        return converted if converted >= 0 else low_default
    return index


def normalize_index_range_lookup(lookup, llen):
    """
    Returns a normalized Redis index range lookup.

    This is designed to convert a value provided for an index lookup
    to a concrete (start, end) tuple for a 'range' type lookup. Note
    that Redis ranges are fully inclusive, unlike Python ranges which
    exclude the end value.

    The 'lookup' arg is the raw lookup value -- it could be None, or a
    single integer, or a 2-member list or tuple: (start, end). Index
    values can be negative, which counts from the end of the list
    instead of the start.

    The 'llen' arg is the total number of items in the data structure.

    - Returns None if 'lookup' is None.
    - If the first 'lookup' range value is None, it is converted to 0.
    - If the second 'lookup' range value is None, it is converted to
      llen - 1.
    - A single integer value X is converted to (X, X).
    - Negative values are converted to positive index values, starting
      from the end of the list. Returns None if the entire range is out
      of bounds, otherwise converts the first index to 0 and the second
      to the corresponding positive index value.
    """
    if lookup is None:
        return None
    if isinstance(lookup, int):
        lookup = normalize_index_lookup(lookup, llen)
        return None if lookup is None else (lookup, lookup)
    ret_val = tuple([
        normalize_index_lookup(lookup[0], llen, 0, 0),
        normalize_index_lookup(lookup[1], llen, llen - 1, None)
    ])
    if ret_val[1] is None:
        return None
    return ret_val


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
    indexed_rtypes = ('list', 'zset', 'string')
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

    def _process_rtype(self, old_rtype, data, force_unique, update, index):
        if force_unique is None and old_rtype in ('zset', 'list'):
            force_unique = old_rtype == 'zset' if update else True
        new_rtype = self.get_rtype_from_data(data, force_unique)
        if update and (old_rtype not in ('none', new_rtype)):
            rtype_cmp_entry = self.rtype_compatibility.get(old_rtype, {})
            cmp_ptypes = rtype_cmp_entry.get('label', self.default_rtype)
            raise TypeError(
                f'Cannot update existing {old_rtype} data with {new_rtype} '
                f'data for key {self.key}. You must provide data of a '
                f'compatible type: {cmp_ptypes}.'
            )
        add_args = [update, index] if new_rtype in self.indexed_rtypes else []
        return new_rtype, add_args

    def add_to_pipe(self, data, force_unique, update, index):
        if not data and data != 0:
            if update:
                return self.pipe.noop()
            return self.pipe.add('delete', self.key)
        old_rtype = self.obj.rtype
        new_rtype, add_args = self._process_rtype(
            old_rtype, data, force_unique, update, index
        )
        self.obj.rtype = new_rtype
        if update or old_rtype == 'none':
            return getattr(self, new_rtype)(data, None, *add_args)
        acc = Accumulator.from_ptype(list)
        self.pipe.add('delete', self.key, accumulator=acc)
        getattr(self, new_rtype)(data, acc, *add_args)
        self.pipe.mark_accumulator_pop()
        return self.pipe

    def _process_zset_update(self, data, accumulator, index):
        prev_zlen = self.obj.len
        offset = normalize_index_lookup(index, prev_zlen, prev_zlen, 0)
        data_end = offset + len(data) - 1
        if offset < prev_zlen:
            accumulator = accumulator or Accumulator.from_ptype(list)
            self.pipe.add(
                'zremrangebyscore', self.key, args=[offset, data_end],
                accumulator=accumulator
            )
        new_zlen = data_end + 1 if data_end >= prev_zlen else prev_zlen
        return offset, new_zlen, accumulator

    def _process_zset_set(self, data, accumulator, index):
        offset = 0 if (index is None or index < 0) else index
        return offset, offset + len(data), accumulator

    def zset(self, data, accumulator, update, index):
        operation_label = 'update' if update else 'set'
        process = getattr(self, f"_process_zset_{operation_label}")
        offset, new_zlen, acc = process(data, accumulator, index)
        self.pipe.add(
            'zadd', self.key, args=[dict(self.encode(
                'zset', ((v, offset + i) for i, v in enumerate(data))
            ))], accumulator=acc
        )
        self.obj.len = new_zlen
        if acc != accumulator:
            self.pipe.mark_accumulator_pop()
        return self.pipe

    def _pad_list_data_with_none(self, data, how_many):
        none = self.encoder.member(None) if self.bypass_encoding else None
        return [none] * how_many + list(data)

    def _process_list_update(self, data, accumulator, index):
        prev_llen = self.obj.len
        offset = normalize_index_lookup(index, prev_llen, prev_llen, 0)
        if offset < prev_llen:
            if len(data) > 1 and not accumulator:
                accumulator = Accumulator.from_ptype(list)
            for i, value in enumerate(data[:prev_llen - offset]):
                args = [i + offset, self.encode('member', value)]
                self.pipe.add(
                    'lset', self.key, args=args, accumulator=accumulator
                )
            data = data[prev_llen - offset:]
        elif offset > prev_llen:
            data = self._pad_list_data_with_none(data, offset - prev_llen)
        return offset, prev_llen + len(data), data, accumulator

    def _process_list_set(self, data, accumulator, index):
        if index is not None and index > 0:
            offset = index
            data = self._pad_list_data_with_none(data, offset)
        else:
            offset = 0
        return offset, len(data), data, accumulator

    def list(self, data, accumulator, update, index):
        operation_label = 'update' if update else 'set'
        process = getattr(self, f"_process_list_{operation_label}")
        offset, new_llen, data, acc = process(data, accumulator, index)
        if data:
            self.pipe.add(
                'rpush', self.key, args=self.encode('list', data),
                accumulator=acc
            )
        self.obj.len = new_llen
        if acc != accumulator:
            self.pipe.mark_accumulator_pop()
        return self.pipe

    def _process_string_update(self, data, index):
        prev_strlen = self.obj.len
        offset = normalize_index_lookup(index, prev_strlen, prev_strlen, 0)
        new_len = len(data) + offset
        return offset, new_len if new_len > prev_strlen else prev_strlen

    def _process_string_set(self, data, index):
        offset = index if index is not None and index > 0 else 0
        return offset, len(data) + offset

    def string(self, data, accumulator, update, index):
        operation_label = 'update' if update else 'set'
        process = getattr(self, f"_process_string_{operation_label}")
        offset, new_strlen = process(data, index)
        self.obj.len = new_strlen
        return self.pipe.add(
            'setrange', self.key, args=[offset, data], accumulator=accumulator
        )

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

    def encoded_obj(self, data, accumulator):
        return self.pipe.add(
            'set', self.key, args=[self.encode('encoded_obj', data)],
            accumulator=accumulator
        )


class _RedisGetter(object):
    """
    Private class that implements 'getting' behaviors for RedisObject.
    """
    rtypes_to_ptypes = {
        'hash': dict,
        'list': list,
        'zset': list,
        'set': set,
        'string': str
    }
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

    def get_ptype_from_obj_rtype(self):
        return self.rtypes_to_ptypes.get(self.obj.rtype)

    def configure_lookup(self, lookup, lookup_type):
        rtype = self.obj.rtype
        if lookup is None and lookup_type is None:
            if rtype in ('list', 'zset', 'string'):
                lookup = (0, -1)
                lookup_type = 'index'
            else:
                return lookup, 'all', True

        if lookup_type is None:
            lookup_type = self.lookup_types[rtype]['default']

        if lookup_type in self.lookup_types[rtype]['valid']:
            if lookup_type == 'index':
                multi = not isinstance(lookup, int)
                lookup = normalize_index_range_lookup(lookup, self.obj.len)
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

    def add_to_pipe(self, lookup, lookup_type, multi):
        rtype = self.obj.rtype
        if lookup_type is None:
            return self.none()
        if lookup_type == 'all':
            return getattr(self, rtype)()
        return getattr(self, f'{rtype}_{lookup_type}')(lookup, multi)

    def zset_index(self, index_range, multi):
        callback = self.decoder.list
        if not multi:
            callback = make_single_or_list_callback(callback)
        return self.pipe.add(
            'zrange', self.key, args=index_range, kwargs={'byscore': True},
            callback=callback
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
            acc = Accumulator.from_ptype(list)
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

    def __len__(self):
        return self.len

    @property
    def len(self):
        """
        The length of the Redis obj, including pending transactions.
        """
        pending = self.pipe.pending_cache.get(self.key, {})
        if 'len' in pending:
            return pending['len']
        if self.rtype == 'none':
            return 0
        if self.rtype == 'zset':
            resp = self.conn.zrange(self.key, -1, -1, withscores=True)
            return int(resp[0][1]) + 1 if resp else 0
        rtype_cmds = {
            'list': 'llen',
            'hash': 'hlen',
            'set': 'scard',
            'string': 'strlen',
            'encoded_obj': 'strlen'
        }
        obj_len = getattr(self.conn, rtype_cmds[self.rtype])(self.key)
        pending['len'] = obj_len
        self.pipe.pending_cache[self.key] = pending
        return obj_len

    @len.setter
    def len(self, value):
        """
        Sets the pending length of this Redis obj.
        """
        pending = self.pipe.pending_cache.get(self.key, {})
        pending['len'] = value
        self.pipe.pending_cache[self.key] = pending

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

    @rtype.setter
    def rtype(self, value):
        """
        Set the Redis data type used for the current key.

        Having a setter for this property is useful so that you can set
        the rtype if you already know what it is, without the added
        call to Redis. But normally you'll just want to let this class
        and the _RedisSetter class handle that, since you'll break it
        if you use the wrong rtype string.
        """
        self._rtype = value

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
        self.setter.add_to_pipe(data, force_unique, update, index)
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
        lookup_args = self.getter.configure_lookup(lookup, lookup_type)
        self.getter.add_to_pipe(*lookup_args)
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


class _SendBatches(object):

    def __init__(self, obj, data, force_unique, target_batch_size):
        self.obj = obj
        self.data = data
        self.num_items = len(data)
        nbatches = self.num_items / target_batch_size
        self.num_batches = int(nbatches) + (0 if nbatches.is_integer() else 1)
        self.encoder = obj.setter.encoder
        self.rtype = obj.setter.get_rtype_from_data(data, force_unique)
        if self.rtype == 'encoded_obj':
            raise ValueError(
                "cannot batch update an 'encoded_obj' type object -- please "
                "convert to a string first if you really need to stream this "
                "object to Redis"
            )
        self.batch_type = obj.getter.rtypes_to_ptypes[self.rtype]
        self.target_batch_size = target_batch_size
        self.iterator = getattr(
            self, f'_{self.rtype}_iterator', self._default_iterator
        )
        self.make_batch = getattr(
            self, f'_make_{self.rtype}_batch', self._default_make_batch
        )

    def _zset_iterator(self, data):
        return (self.encoder.member(item) for item in data)

    def _hash_iterator(self, data):
        return self.encoder.hash(data)

    def _default_iterator(self, data):
        return self.encoder.list(data)

    def _make_string_batch(self):
        total_size = len(self.data)
        index = 0
        while index < total_size:
            yield self.data[(index):(index + self.target_batch_size)]
            index += self.target_batch_size

    def _default_make_batch(self):
        batch = []
        for i, item in enumerate(self.iterator(self.data)):
            batch.append(item)
            if (i + 1) % self.target_batch_size == 0:
                yield self.batch_type(batch)
                batch = []
        if batch:
            yield self.batch_type(batch)

    def __call__(self):
        return self.make_batch()


class _AccumulatorFactory(object):
    accumulated_types = {
        'dict_all': dict,
        'str_index': str,
    }

    @staticmethod
    def _flatten(accumulated, new_values):
        accumulated.extend([
            item for vals in new_values for item in (vals or [])
        ])

    @staticmethod
    def dict_all(accumulated, new_values):
        for keys, vals in zip(*new_values):
            accumulated.update(dict(zip(keys, vals)))

    @staticmethod
    def dict_field(accumulated, new_values):
        accumulated.extend([
            item for vals in new_values[1] for item in (vals or [])
        ])

    @staticmethod
    def str_index(collected, new_vals):
        return f"{collected}{''.join([str(v) if v else '' for v in new_vals])}"

    def __call__(self, ptype, lookup_type):
        full_lookup_name = f'{ptype.__name__}_{lookup_type}'
        acc_type = self.accumulated_types.get(full_lookup_name, list)
        method = getattr(self, full_lookup_name, self._flatten)
        return Accumulator(acc_type, method)


class RedisObjectStream(object):

    def __init__(self, obj, target_batch_size, execute_every_nth_batch=None):
        self.obj = obj
        self.target_batch_size = target_batch_size
        self.execute_every = execute_every_nth_batch
        self.pipe = obj.pipe
        self.key = obj.key
        self.batches = None
        self.accumulator_factory = _AccumulatorFactory()

    def set(self, data, force_unique=None, update=False, index=None):
        prev_defer = self.obj.defer
        prev_bypass = self.obj.setter.bypass_encoding
        self.obj.defer = True
        self.obj.setter.bypass_encoding = True
        self.batches = _SendBatches(
            self.obj, data, force_unique, self.target_batch_size
        )
        execute_rvals = []
        try:
            for i, batch in enumerate(self.batches()):
                update = update if i == 0 else True
                if index is None:
                    offset = index
                else:
                    offset = (self.target_batch_size * i) + index
                self.obj.set(batch, force_unique, update, offset)
                nbatch = i + 1
                is_final = nbatch == self.batches.num_batches
                do_it = self.execute_every and nbatch % self.execute_every == 0
                if is_final or do_it:
                    execute_rvals.extend(self.pipe.execute())
        except Exception:
            self.pipe.reset()
            raise
        self.obj.defer = prev_defer
        self.obj.setter.bypass_encoding = prev_bypass
        return execute_rvals

    def _get_str(self, lookup, lookup_type, multi):
        accumulator = self.accumulator_factory(str, lookup_type)
        batches = [
            (num, min(num + self.target_batch_size - 1, lookup[1]))
            for num in range(
                lookup[0], lookup[1] + 1, self.target_batch_size
            )
        ]
        lookup_type = 'index'
        try:
            for i, batch in enumerate(batches):
                self.obj.getter.add_to_pipe(batch, lookup_type, multi)
                nbatch = i + 1
                is_final = nbatch == len(batches)
                do_it = self.execute_every and nbatch % self.execute_every == 0
                if is_final or do_it:
                    accumulator.push(self.obj.pipe.execute())
        except Exception:
            self.pipe.reset()
            raise
        return accumulator.pop_all()

    def _get_set(self, lookup, lookup_type, multi):
        if lookup_type == 'all':
            raise TypeError(
                "cannot use 'RedisObjectStream' to get entire Redis 'set' "
                "objects in batches, as they have no methods for doing this "
                "-- use 'RedisObject' instead"
            )
        accumulator = self.accumulator_factory(set, lookup_type)
        batches = [
            lookup[(num):(num + self.target_batch_size)]
            for num in range(0, len(lookup), self.target_batch_size)
        ]
        try:
            for i, batch in enumerate(batches):
                self.obj.getter.add_to_pipe(batch, lookup_type, multi)
                nbatch = i + 1
                is_final = nbatch == len(batches)
                do_it = self.execute_every and nbatch % self.execute_every == 0
                if is_final or do_it:
                    accumulator.push(self.obj.pipe.execute())
        except Exception:
            self.pipe.reset()
            raise
        return accumulator.pop_all()

    def _get_list(self, lookup, lookup_type, multi):
        accumulator = self.accumulator_factory(list, lookup_type)
        if lookup_type == 'value':
            batches = [
                lookup[(num):(num + self.target_batch_size)]
                for num in range(0, len(lookup), self.target_batch_size)
            ]
        else:
            batches = [
                (num, min(num + self.target_batch_size - 1, lookup[1]))
                for num in range(
                    lookup[0], lookup[1] + 1, self.target_batch_size
                )
            ]
            lookup_type = 'index'
        try:
            for i, batch in enumerate(batches):
                self.obj.getter.add_to_pipe(batch, lookup_type, multi)
                nbatch = i + 1
                is_final = nbatch == len(batches)
                do_it = self.execute_every and nbatch % self.execute_every == 0
                if is_final or do_it:
                    accumulator.push(self.obj.pipe.execute())
        except Exception:
            self.pipe.reset()
            raise
        return accumulator.pop_all()

    def _get_dict(self, lookup, lookup_type, multi):
        accumulator = self.accumulator_factory(dict, lookup_type)
        hash_keys = lookup or self.obj.conn.hkeys(self.obj.key)
        keys_stack = []
        key_batches = [
            hash_keys[(num):(num + self.target_batch_size)]
            for num in range(0, len(hash_keys), self.target_batch_size)
        ]
        try:
            for i, batch_keys in enumerate(key_batches):
                self.obj.getter.add_to_pipe(batch_keys, 'field', True)
                keys_stack.append(batch_keys)
                nbatch = i + 1
                is_final = nbatch == len(key_batches)
                do_it = self.execute_every and nbatch % self.execute_every == 0
                if is_final or do_it:
                    accumulator.push((keys_stack, self.obj.pipe.execute()))
                    keys_stack = []
        except Exception:
            self.pipe.reset()
            raise
        return accumulator.pop_all()

    def get(self, lookup=None, lookup_type=None):
        lookup_args = self.obj.getter.configure_lookup(lookup, lookup_type)
        lookup, lookup_type, multi = lookup_args
        if lookup_type is None:
            return None
        if not multi:
            self.obj.getter.add_to_pipe(lookup, lookup_type, multi)
            return self.pipe.execute()[-1]
        ptype = self.obj.getter.get_ptype_from_obj_rtype()
        # accumulator = Accumulator.from_ptype(ptype, multi=True)
        data = getattr(self, f'_get_{ptype.__name__}')(*lookup_args)
        return data
