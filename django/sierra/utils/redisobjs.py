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
LISTLIKE_TYPES = [list, tuple]


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

    def __len__(self):
        """
        Returns the number of currently pending cmds on the pipeline.
        """
        return self.pipe.__len__()

    def __bool__(self):
        # Since we have __len__ defined we need __bool__ too to
        # override cases where __len__ is 0 causing an instance to
        # evaluate as False.
        return True

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


class _Lookup(object):
    """
    Internal base class for storing info about Redis object lookups.

    Each subclass should represent one type of lookup. They may be
    RedisType dependent, since different rtypes have different ways of
    being looked up.

    A subclass must at least supply a string 'label' attribute. This is
    a way to identify and address the lookup easily. Subclasses may
    optionally supply 'plural_label', which is a valid secondary way to
    identify and address the lookup. When 'plural_label' is used with
    lookup instances, it always indicates a lookup requesting multiple
    values. (Use of 'label' could indicate either.)

    Instances of a subclass represent a concrete lookup attempt and
    store the corresponding parameters: the RedisObject, the lookup
    value, the provided label, whether to force a multi-lookup request,
    and whether the whole object is being requested.
    """
    label = None
    plural_label = None

    def __init__(self, obj, value, provided_label=None, multi=None,
                 whole_obj_requested=False):
        """
        Instantiates one concrete lookup request.

        In subclasses, the goal for '__init__' should be to parse the
        provided information and store it canonically for the given
        lookup attempt. The lookup 'value' may need to be normalized,
        for example.

        Use the optional 'provided_label' to track what was originally
        requested, if your lookup has variations -- such as a singular
        and plural form.

        Use 'multi' to denote explicitly whether this is a singular or
        multi lookup: when False, an atomic return value is expected.
        When True, a list of return values is expected.

        By default, the canonical 'multi' instance attribute is
        determined as follows: if 'multi' is supplied, that takes
        precedence; otherwise if a 'plural_label' class attribute
        exists and 'provided_label' == 'plural_label', then multi is
        True.

        When the entire content of an object is being requested, set
        'whole_obj_requested' to True.
        """
        self.obj = obj
        self.value = value
        self.provided_label = provided_label
        if multi is None and self.plural_label:
            self.multi = provided_label == self.plural_label
        else:
            self.multi = multi
        self.whole_obj_requested = whole_obj_requested

    def __str__(self):
        return self.label


class _GenericAllLookup(_Lookup):
    """
    Class representing a generic "get the entire object" lookup.
    """
    label = 'all'

    def __init__(self, obj, value, provided_label=None, multi=True,
                 whole_obj_requested=True):
        super().__init__(obj, None, provided_label, True, True)


class _FieldLookup(_Lookup):
    """
    Class representing a 'field' lookup, for hsets.

    (For getting the values that correspond to one or many fields.)
    """
    label = 'field'
    plural_label = 'fields'

    def __init__(self, obj, value, provided_label=None, multi=None,
                 whole_obj_requested=False):
        """
        Initializes a 'field' lookup instance.

        Note that, currently, field values must be strings; therefore
        we can assume that a list/tuple denotes a 'multi' lookup
        request. However, providing 'multi' explicitly OR setting
        'provided_label="fields"' will override this, and it will
        wrap/unwrap the value appropriately.
        """
        val_is_multi = isinstance(value, (list, tuple))
        if multi is None:
            if provided_label:
                multi = provided_label == self.plural_label
            else:
                multi = val_is_multi
        if value:
            if multi and not val_is_multi:
                value = [value]
            elif val_is_multi and not multi:
                value = value[0]
        super().__init__(
            obj, value, provided_label, multi, whole_obj_requested
        )


class _IndexLookup(_Lookup):
    """
    Class representing an 'index' lookup, for lists, zsets, & strings.

    (For getting a value at the given index position, or all values in
    a given range.)

    No plural form of index is used, so there is no 'plural_label.' But
    plurality can be inferred from the lookup value (one index versus a
    range).
    """
    label = 'index'

    def __init__(self, obj, value, provided_label=None, multi=None,
                 whole_obj_requested=False):
        """
        Initializes an 'index' lookup instance.

        The supplied 'value' may be a single integer or a two-member
        list/tuple indicating the start and end values of a range
        (inclusive). Redis index lookups require both a start and end
        value, so the provided lookup value is always converted to a
        range. E.g., 0 becomes (0, 0).

        Each index value is normalized appropriately using the
        'normalize_index_lookup' method. Negative values get converted
        to positive values based on the length of the provided
        RedisObject -- note that this is done because not all Redis
        index lookup types (particularly for zsets) allow negative
        values, so converting them manually provides the most
        consistent behavior. If an invalid / out-of-range negative is
        used as the end of the range, then the value is converted to
        None (because it is invalid). An out-of-range negative as the
        start value is converted to 0.

        The 'multi' kwarg is honored. But, if not supplied, then a
        single index 'value' is assumed to be 'multi = False' and a
        range is assumed to be 'multi = True'.
        """
        if value is not None:
            llen = obj.len
            if isinstance(value, int):
                multi = multi or False
                index_val = self.normalize_index_lookup(value, llen)
                value = (index_val, index_val)
            else:
                multi = multi or True
                value = tuple([
                    self.normalize_index_lookup(value[0], llen, 0, 0),
                    self.normalize_index_lookup(value[1], llen, llen - 1, None)
                ])
            if value[1] is None:
                value = None
        super().__init__(
            obj, value, provided_label, multi, whole_obj_requested
        )

    @staticmethod
    def normalize_index_lookup(index, llen, default=0, low_default=None):
        """
        Returns a normalized Redis index lookup.

        The 'index' arg is the raw index value to convert. It could be
        None or it could be negative.

        The 'llen' arg should be the length of the RedisObject data
        structure, for converting negative indexes to positive values.
        Use None if you don't want to convert negative index positions.

        The 'default' kwarg defines what is returned when 'index' is
        None.

        The 'low_default' kwarg defines what is returned when a
        negative index is provided and the conversion attempt reveals
        it to be out of range.
        """
        if index is None:
            return default
        if index < 0 and llen is not None:
            converted = llen + index
            return converted if converted >= 0 else low_default
        return index


class _ValueLookup(_Lookup):
    """
    Class representing a 'value' lookup, for lists & zsets.

    (Gets the index position(s) for the given value(s).)

    Supplying "values" as the 'provided_field' indicates a plural
    lookup.
    """
    label = 'value'
    plural_label = 'values'


class _ValueExistsLookup(_Lookup):
    """
    Class representing a lookup to find set membership.

    (For one or more values -- returns True for a value if it is in the
    set, or False if it is not.)

    Supplying "values_exist" as the 'provided_field indicates a plural
    lookup.
    """
    label = 'value_exists'
    plural_label = 'values_exist'


def make_invalid_lookup(err_type, err_msg):
    """
    Factory for generating an invalid lookup type.

    Pass the 'err_type' and 'err_msg' you want to raise when the lookup
    type is used. It returns a class that can act as a lookup type but
    raises the specified error when instantiated.
    """
    class _DynamicInvalidLookup(object):
        def __init__(self, *args, **kwargs):
            raise err_type(err_msg)
    return _DynamicInvalidLookup


class _LookupCollection(object):
    """
    Internal class representing a full collection of lookups.

    Implements collective lookup behavior, specifically, the ability to
    get a particular _Lookup class by its canonical 'label' attribute,
    via the 'get' method.
    """

    def __init__(self):
        lookup_types = (
            _GenericAllLookup, _FieldLookup, _IndexLookup, _ValueLookup,
            _ValueExistsLookup
        )
        self.ltypes = {}
        self.plural_ltypes = {}
        for lt in lookup_types:
            self.ltypes[lt.label] = lt
            if lt.plural_label:
                self.ltypes[lt.plural_label] = lt
                self.plural_ltypes[lt.plural_label] = lt

    def get(self, label, default=None):
        """
        Returns a _Lookup type by its 'label' attribute.
        """
        return self.ltypes.get(label, default)


LOOKUP_TYPES = _LookupCollection()


class _RedisTypeMetadata(object):
    """
    Internal class for storing info about a Redis Type.

    This class is intended to be instantiated directly in _RedisType
    subclass definitions, to store canonical information about the
    rtype.

    'encode_member' and 'decode_member' are for encoding/decoding the
    individual data members for collective types (list/zset elements,
    hash elements, set elements) -- here we encode to JSON, but you
    could subclass this to use something different.

    'label' -- a string for identifying and addressing the rtype
    easily. Using the type string that Redis returns from a 'TYPE'
    command is generally expected, assuming there's a one-to-one
    correlation with an actual Redis type.

    'to_ptype' -- stores the ONE ptype that this rtype should convert
    to.

    'compatible_ptypes' -- stores a tuple of Python types that are
    compatible with this rtype. It should include the 'to_ptype'.

    'incompatible_ptypes' -- stores a tuple of Python types that are
    NOT compatible with this rtype. (It should be assumed that all
    types that are not incompatible are compatible. You should not have
    'compatible_ptypes' and 'incompatible_ptypes' on the same object.)

    'compatibility_label' -- see the 'compatiblity_label' property for
    a description.

    'all_lookup_type' -- the _Lookup class for fetching an entire
    object of this Redis Type.

    'all_lookup_value' -- optionally, if the 'all_lookup_type' requires
    a value to get the full object, define that here.

    'default_lookup_type' -- the _Lookup class used by default when the
    user provides a lookup value but no lookup type.

    'batch_lookup_type' -- optionally, the _Lookup class used to fetch
    one piece of entire object when a piecemeal (batch) get operation
    is performed.

    'valid_lookup_types' -- a list of all _Lookup classes that are
    valied for this rtype. It should include the all, default, and
    batch lookup types, plus any others.

    'attributes' -- a set of string attributes or tags that apply to
    this rtype. If a tag is included, it applies; if not, it does not.
    """
    encode_member = ujson.dumps

    def __init__(self, label, compatible_ptypes=[], incompatible_ptypes=[],
                 compatibility_label=None, all_lookup_type=None,
                 all_lookup_value=None, batch_lookup_type=None,
                 default_lookup_type=None, other_valid_lookup_types=None,
                 attributes=None):
        """
        Initializes a _RedisTypeMetadata instance.

        See the class description for a description of the instance
        attributes created from the args and kwargs.

        'label' -- required.

        'compatible_ptypes' -- optional. Default is an empty tuple. If
        provided, the first Python type is used as the 'to_ptype' type.

        'incompatible_ptypes' -- optional. Default is an empty tuple.
        
        'compatibility_label' -- optional. Default is None. If None,
        the 'compatibility_label' property generates a dynamic value
        base on the 'compatible_ptypes'. Otherwise, whatever you supply
        overrides that.

        'all_lookup_type' -- optional. Default is _GenericAllLookup.

        'default_lookup_type' -- optional. Default is 'all_lookup_type'.

        'batch_lookup_type' -- optional. Default is None.

        'other_valid_lookup_types' -- optional. Gets combined with the
        previous three lookup types to populate 'valid_lookup_types'.
        """
        self.label = label
        self.compatible_ptypes = tuple(compatible_ptypes)
        self.incompatible_ptypes = tuple(incompatible_ptypes)
        self.to_ptype = (compatible_ptypes or [None])[0]
        self.compatibility_label = compatibility_label
        self.all_lookup_value = all_lookup_value
        self.all_lookup_type = all_lookup_type or LOOKUP_TYPES.get('all')
        self.default_lookup_type = default_lookup_type or self.all_lookup_type
        self.batch_lookup_type = batch_lookup_type
        self.valid_lookup_types = set(
            ([self.all_lookup_type] if self.all_lookup_type else []) +
            ([self.default_lookup_type] if self.default_lookup_type else []) +
            ([self.batch_lookup_type] if self.batch_lookup_type else []) +
            (other_valid_lookup_types or [])
        )
        self.attributes = set(attributes or [])

    @staticmethod
    def decode_member(raw):
        return None if raw is None else ujson.loads(raw)

    @property
    def compatibility_label(self):
        """
        A readable label describing compatible Py types for this rtype.

        A static 'compatibility_label' can be supplied during __init__.
        If not, then it generates a label from 'compatible_ptypes'.
        """
        if self._compatibility_label is None:
            return ' or '.join([pt.__name__ for pt in self.compatible_ptypes])
        return self._compatibility_label

    @compatibility_label.setter
    def compatibility_label(self, label):
        self._compatibility_label = label

    def data_is_compatible(self, data):
        """
        Returns True if 'data' is compatible with this rtype.
        """
        if isinstance(data, self.compatible_ptypes):
            return True
        if isinstance(data, self.incompatible_ptypes):
            return False
        return bool(self.incompatible_ptypes)

    def has(self, attribute):
        """
        Returns True if 'attribute' describes this rtype.
        """
        return attribute in self.attributes


class _RedisType(object):
    """
    Internal base class for representing a Redis type (rtype).

    Each subclass represents one rtype. (Note that it doesn't HAVE to
    correspond with an actual Redis type; it could an internal type
    that maps to a Redis type.) Subclasses are responsible for
    implementing methods that interact with Redis to set, update, and
    get data for a given RedisObject. Exactly what these methods are
    and what args/kwargs they take depend on the 'lookup' and 'ptype'
    attributes in the 'info' (_RedisTypeMetadata) object.
    """
    info = _RedisTypeMetadata('invalid', [])

    def __str__(self):
        return self.info.label

    def __init__(self):
        """
        Initializes a _RedisType instance.
        """
        self.encode_member = self.info.encode_member
        self.decode_member = self.info.decode_member
    
    def configure_lookup(self, obj, lvalue, ltype_label, batch=False):
        """
        Converts a user-supplied value/label to a _Lookup.

        None/None => 'all' (get the whole object).
        value/None => default lookup type for that object.

        Returns None if the requested lookup type is not valid for this
        RedisType, or if a valid lookup type is specified but the
        lookup value is None or empty.
        """
        if ltype_label is None:
            if lvalue is None:
                lvalue = self.get_all_lookup_value(obj, batch)
                if batch and self.info.batch_lookup_type:
                    return self.info.batch_lookup_type(
                        obj, lvalue, None, whole_obj_requested=True
                    )
                return self.info.all_lookup_type(
                    obj, lvalue, None, whole_obj_requested=True
                )
            return self.info.default_lookup_type(obj, lvalue, None)
        ltype = LOOKUP_TYPES.get(ltype_label)
        if ltype in self.info.valid_lookup_types:
            lookup = ltype(obj, lvalue, ltype_label)
            if lookup.value or lookup.value == 0:
                return lookup

    def get_all_lookup_value(self, obj, for_batch=False):
        """
        Returns the 'all' lookup value for the given object.
        """
        return self.info.all_lookup_value

    @staticmethod
    def get_obj_length(obj):
        """
        Returns the length of the given object (as an integer).

        Implement this in each subclass.
        """
        pass

    def get(self, obj, lookup):
        """
        Returns a Pipeline with cmds to get Redis data.

        This defers to the appropriate method on the rtype subclass:
        - 'get_all' if the lookup is a _GenericAllLookup.
        - 'get_by_{label}' for all other lookups.

        It's up to each subclass to implement the appropriate methods.
        """
        if lookup is None:
            return obj.pipe.noop()
        if lookup.label == 'all':
            return self.get_all(obj)
        return getattr(self, f'get_by_{lookup}')(
            obj, lookup.value, lookup.multi
        )

    def get_all(self, obj):
        """
        Returns a Pipeline with cmds to get a whole Redis object.

        Implement this in each subclass.
        """
        pass

    def save(self, obj, data, update, index, was_none=False):
        """
        Returns a Pipeline with cmds to save a Redis object.

        This defers to the appropriate method on the rtype subclass:
        - 'set' if setting data on the object from scratch.
        - 'update' if updating existing data.

        It's up to each subclass to implement these methods.

        Raises a TypeError if 'data' is not compatible given the
        compatibility information in self.info.
        """
        if not self.info.data_is_compatible(data):
            raise TypeError(
                f"cannot save key '{obj.key}' as a Redis {self.info.label} "
                f"using {type(data).__name__} data; must be a compatible "
                f"type: {self.info.compatibility_label}"
            )

        add_args = [index] if self.info.has('indexable') else []
        if was_none:
            return self.set(obj, data, None, *add_args)
        if update:
            return self.update(obj, data, None, *add_args)
        acc = Accumulator.from_ptype(list)
        obj.pipe.add('delete', obj.key, accumulator=acc)
        self.set(obj, data, acc, *add_args)
        obj.pipe.mark_accumulator_pop()
        return obj.pipe


class _ZsetRedisType(_RedisType):
    """
    Class that implements features for Redis zsets (sorted sets).
    """
    info = _RedisTypeMetadata(
        'zset', LISTLIKE_TYPES, all_lookup_type=LOOKUP_TYPES.get('index'),
        all_lookup_value=(0, -1), batch_lookup_type=LOOKUP_TYPES.get('index'),
        default_lookup_type=LOOKUP_TYPES.get('index'),
        other_valid_lookup_types=[LOOKUP_TYPES.get('value')],
        attributes=['indexable', 'unique', 'listlike', 'def-listlike']
    )

    def encode(self, data, bypass_encoding=False):
        if bypass_encoding:
            return data
        # 'data' should be a list of (item, score) tuples
        return ((self.encode_member(item), score) for item, score in data)

    def decode(self, raw):
        return [self.decode_member(v) for v in raw] if raw else None

    @staticmethod
    def get_obj_length(obj):
        resp = obj.conn.zrange(obj.key, -1, -1, withscores=True)
        return int(resp[0][1]) + 1 if resp else 0

    def _set_from_offset(self, obj, data, accumulator, offset, new_zlen):
        obj.pipe.add(
            'zadd', obj.key, args=[dict(self.encode(
                ((v, offset + i) for i, v in enumerate(data)),
                obj.bypass_encoding
            ))], accumulator=accumulator
        )
        obj.len = new_zlen
        return obj.pipe

    def set(self, obj, data, accumulator, index):
        offset = 0 if (index is None or index < 0) else index
        new_zlen = offset + len(data)
        return self._set_from_offset(obj, data, accumulator, offset, new_zlen)

    def update(self, obj, data, accumulator, index):
        acc = accumulator
        prev_zlen = obj.len
        offset = LOOKUP_TYPES.get('index').normalize_index_lookup(
            index, prev_zlen, prev_zlen, 0
        )
        data_end = offset + len(data) - 1
        if offset < prev_zlen:
            acc = acc or Accumulator.from_ptype(list)
            obj.pipe.add(
                'zremrangebyscore', obj.key, args=[offset, data_end],
                accumulator=acc
            )
        new_zlen = data_end + 1 if data_end >= prev_zlen else prev_zlen
        self._set_from_offset(obj, data, acc, offset, new_zlen)
        if acc != accumulator:
            obj.pipe.mark_accumulator_pop()
        return obj.pipe

    def get_by_index(self, obj, lval, multi):
        callback = self.decode
        if not multi:
            callback = make_single_or_list_callback(callback)
        return obj.pipe.add(
            'zrange', obj.key, args=lval, kwargs={'byscore': True},
            callback=callback
        )

    def get_by_value(self, obj, lval, multi):
        if multi:
            encoded_vals = [self.encode_member(v) for v in lval]
            return obj.pipe.add(
                'zmscore', obj.key, args=[encoded_vals],
                callback=lambda scores: [
                    score if score is None else int(score) for score in scores
                ] if scores else None
            )
        return obj.pipe.add(
            'zscore', obj.key, args=[self.encode_member(lval)],
            callback=lambda score: score if score is None else int(score)
        )


class _ListRedisType(_RedisType):
    """
    Class that implements features for Redis lists.
    """
    info = _RedisTypeMetadata(
        'list', LISTLIKE_TYPES, all_lookup_type=LOOKUP_TYPES.get('index'),
        all_lookup_value=(0, -1), batch_lookup_type=LOOKUP_TYPES.get('index'),
        default_lookup_type=LOOKUP_TYPES.get('index'),
        other_valid_lookup_types=[LOOKUP_TYPES.get('value')],
        attributes=['indexable', 'not-unique', 'listlike']
    )

    def encode(self, data, bypass_encoding=False):
        if bypass_encoding:
            return data
        return (self.encode_member(item) for item in data)

    def decode(self, raw):
        return [self.decode_member(v) for v in raw] if raw else None

    @staticmethod
    def get_obj_length(obj):
        return obj.conn.llen(obj.key)

    def _pad_list_data_with_none(self, obj, data, how_many):
        none = self.encode_member(None) if obj.bypass_encoding else None
        return [none] * how_many + list(data)

    def _set_from_offset(self, obj, data, accumulator, offset, new_llen):
        if data:
            obj.pipe.add(
                'rpush', obj.key,
                args=self.encode(data, obj.bypass_encoding),
                accumulator=accumulator
            )
        obj.len = new_llen
        return obj.pipe

    def set(self, obj, data, accumulator, index):
        if index is not None and index > 0:
            offset = index
            data = self._pad_list_data_with_none(obj, data, offset)
        else:
            offset = 0
        return self._set_from_offset(obj, data, accumulator, offset, len(data))

    def update(self, obj, data, accumulator, index):
        acc = accumulator
        prev_llen = obj.len
        offset = LOOKUP_TYPES.get('index').normalize_index_lookup(
            index, prev_llen, prev_llen, 0
        )
        if offset < prev_llen:
            if len(data) > 1 and not accumulator:
                acc = acc or Accumulator.from_ptype(list)
            for i, value in enumerate(data[:prev_llen - offset]):
                args = [
                    i + offset,
                    value if obj.bypass_encoding else self.encode_member(value)
                ]
                obj.pipe.add(
                    'lset', obj.key, args=args, accumulator=acc
                )
            data = data[prev_llen - offset:]
        elif offset > prev_llen:
            data = self._pad_list_data_with_none(obj, data, offset - prev_llen)
        self._set_from_offset(obj, data, acc, offset, prev_llen + len(data))
        if acc != accumulator:
            obj.pipe.mark_accumulator_pop()
        return obj.pipe

    def get_by_index(self, obj, lval, multi):
        callback = self.decode
        if not multi:
            callback = make_single_or_list_callback(callback)
        return obj.pipe.add('lrange', obj.key, args=lval, callback=callback)

    def get_by_value(self, obj, lval, multi):
        callback = make_single_or_list_callback()
        kwargs = {'count': 0}
        if multi:
            acc = Accumulator.from_ptype(list)
            for value in self.encode(lval):
                obj.pipe.add(
                    'lpos', obj.key, args=[value], kwargs=kwargs,
                    callback=callback, accumulator=acc
                )
            obj.pipe.mark_accumulator_pop()
            return obj.pipe
        return obj.pipe.add(
            'lpos', obj.key, args=[self.encode_member(lval)], kwargs=kwargs,
            callback=callback
        )


class _StringRedisType(_RedisType):
    """
    Class that implements features for Redis strings.
    """
    info = _RedisTypeMetadata(
        'string', [str], all_lookup_type=LOOKUP_TYPES.get('all'),
        default_lookup_type=LOOKUP_TYPES.get('index'),
        batch_lookup_type=LOOKUP_TYPES.get('index'),
        attributes=['indexable']
    )

    def get_all_lookup_value(self, obj, for_batch=False):
        if for_batch:
            return (0, -1)
        return super().get_all_lookup_value(obj, for_batch)

    def set(self, obj, data, accumulator, index):
        offset = index if index is not None and index > 0 else 0
        obj.len = len(data) + offset
        return obj.pipe.add(
            'setrange', obj.key, args=[offset, data], accumulator=accumulator
        )

    @staticmethod
    def get_obj_length(obj):
        return obj.conn.strlen(obj.key)

    def update(self, obj, data, accumulator, index):
        prev_strlen = obj.len
        offset = LOOKUP_TYPES.get('index').normalize_index_lookup(
            index, prev_strlen, prev_strlen, 0
        )
        new_len = len(data) + offset
        obj.len = new_len if new_len > prev_strlen else prev_strlen
        return obj.pipe.add(
            'setrange', obj.key, args=[offset, data], accumulator=accumulator
        )

    def get_all(self, obj):
        return obj.pipe.add('get', obj.key)

    def get_by_index(self, obj, lval, multi):
        return obj.pipe.add(
            'getrange', obj.key, args=lval, callback=lambda raw: raw or None
        )


class _HashRedisType(_RedisType):
    """
    Class that implements features for Redis hashes.
    """
    info = _RedisTypeMetadata(
        'hash', [dict], all_lookup_type=LOOKUP_TYPES.get('all'),
        default_lookup_type=LOOKUP_TYPES.get('field'),
        batch_lookup_type=LOOKUP_TYPES.get('field')
    )

    def get_all_lookup_value(self, obj, for_batch=False):
        if for_batch:
            return obj.conn.hkeys(obj.key)
        return super().get_all_lookup_value(obj, for_batch)

    def encode(self, data, bypass_encoding=False):
        if bypass_encoding:
            return data
        return ((k, self.encode_member(v)) for k, v in iteritems(data))

    def decode(self, raw):
        return {
            k: self.decode_member(v) for k, v in iteritems(raw)
        } if raw else None

    @staticmethod
    def get_obj_length(obj):
        return obj.conn.hlen(obj.key)

    def set(self, obj, data, accumulator):
        return obj.pipe.add(
            'hset', obj.key,
            kwargs={'mapping': dict(self.encode(data, obj.bypass_encoding))},
            accumulator=accumulator
        )

    def update(self, obj, data, accumulator):
        return self.set(obj, data, accumulator)

    def get_all(self, obj):
        return obj.pipe.add('hgetall', obj.key, callback=self.decode)

    def get_by_field(self, obj, lval, multi):
        if multi:
            return obj.pipe.add(
                'hmget', obj.key, args=lval,
                callback=_ListRedisType().decode
            )
        return obj.pipe.add(
            'hget', obj.key, args=[lval], callback=self.decode_member
        )


class _SetRedisType(_RedisType):
    """
    Class that implements features for Redis sets.
    """
    info = _RedisTypeMetadata(
        'set', [set], batch_lookup_type=make_invalid_lookup(
            TypeError,
            "cannot get an entire Redis 'set' object in batches, as they have "
            "no methods for this -- use a non-batch type instead, such as "
            "RedisObject"
        ),
        default_lookup_type=LOOKUP_TYPES.get('value_exists')
    )

    def encode(self, data, bypass_encoding=False):
        if bypass_encoding:
            return data
        return (self.encode_member(item) for item in data)

    def decode(self, raw):
        return set([self.decode_member(v) for v in raw]) if raw else None

    @staticmethod
    def get_obj_length(obj):
        return obj.conn.scard(obj.key)

    def set(self, obj, data, accumulator):
        return obj.pipe.add(
            'sadd', obj.key, args=self.encode(data, obj.bypass_encoding),
            accumulator=accumulator
        )

    def update(self, obj, data, accumulator):
        return self.set(obj, data, accumulator)

    def get_all(self, obj):
        return obj.pipe.add('smembers', obj.key, callback=self.decode)

    def get_by_value_exists(self, obj, lval, multi):
        if multi:
            encoded_vals = list(self.encode(lval))
            return obj.pipe.add(
                'smismember', obj.key, args=[encoded_vals],
                callback=lambda raw: [bool(v) for v in raw]
            )
        return obj.pipe.add(
            'sismember', obj.key, args=[self.encode_member(lval)],
            callback=bool
        )


class _EncodedObjRedisType(_RedisType):
    """
    Class that implements features for encoded objects.

    "Encoded object" is not a Redis type -- it is a catchall for
    anything NOT covered by the other types. If it can be converted to
    JSON, then it is stored as a JSON string in Redis, with a prefix
    that denotes it is JSON and not just a string.
    """
    info = _RedisTypeMetadata(
        'encoded_obj', compatible_ptypes=[],
        incompatible_ptypes=[list, tuple, str, dict, set],
        attributes=['default'],
        compatibility_label=(
            'any JSON-serializable type except list, tuple, str, dict, or set'
        )
    )

    @staticmethod
    def is_encoded_obj(obj):
        end_of_prefix = len(STR_OBJ_PREFIX) - 1
        return obj.conn.getrange(obj.key, 0, end_of_prefix) == STR_OBJ_PREFIX

    def encode(self, data, bypass_encoding=False):
        if bypass_encoding:
            return data
        return ''.join([STR_OBJ_PREFIX, self.encode_member(data)])

    def decode(self, raw):
        return self.decode_member(raw.lstrip(STR_OBJ_PREFIX))

    @staticmethod
    def get_obj_length(obj):
        return obj.conn.strlen(obj.key)

    def set(self, obj, data, accumulator):
        return obj.pipe.add(
            'set', obj.key, args=[self.encode(data, obj.bypass_encoding)],
            accumulator=accumulator
        )

    def update(self, obj, data, accumulator):
        return self.set(obj, data, accumulator)

    def get_all(self, obj):
        return obj.pipe.add('get', obj.key, callback=self.decode)


class _NoneRedisType(_RedisType):
    """
    Class representing a null object.
    """
    info = _RedisTypeMetadata('none', [type(None)])

    @staticmethod
    def get_obj_length(obj):
        return 0

    def get_all(self, obj):
        return obj.pipe.noop()


class _RedisTypeCollection(object):
    """
    Internal class for the full collection of Redis Types.

    Implements collective behavior for choosing the appropriate rtype
    in various circumstances (depending on known information).
    """

    def __init__(self):
        """
        Initializes a _RedisTypeCollection instance.
        """
        redis_types = (
            _ZsetRedisType(), _ListRedisType(), _StringRedisType(),
            _HashRedisType(), _SetRedisType(), _EncodedObjRedisType(),
            _NoneRedisType()
        )
        self.rtypes = {}
        self.by_ptype_id = {}
        self.by_attribute = {}
        for rt in redis_types:
            self.rtypes[rt.info.label] = rt
            for pt in rt.info.compatible_ptypes:
                key = (pt, True) if rt.info.has('unique') else pt
                self.by_ptype_id[key] = rt
            for attr in rt.info.attributes:
                self.by_attribute[attr] = self.by_attribute.get(attr, set())
                self.by_attribute[attr].add(rt)
        self.default_rtype = self.get_one_by_attributes(['default'])
        self.default_listlike = self.get_one_by_attributes(['def-listlike'])

    def get(self, label, default=None):
        """
        Returns the rtype corresponding with the given label.
        """
        return self.rtypes.get(label, default)

    def get_from_obj(self, obj):
        """
        Returns the rtype for the given RedisObject.

        Note that this is needed to convert a 'string' to an
        'encoded_obj', since both are stored in Redis as string data.
        """
        rt_label = obj.conn.type(obj.key)
        if rt_label == 'string':
            if self.get('encoded_obj').is_encoded_obj(obj):
                rt_label = 'encoded_obj'
        return self.get(rt_label)

    def get_by_attributes(self, attrs):
        """
        Returns the rtypes with all the given attributes (as a set).
        """
        return set.intersection(*[self.by_attribute[attr] for attr in attrs])

    def get_one_by_attributes(self, attrs):
        """
        Returns one rtype with all the given attributes.

        Use this if you're targeting ONE and only one rtype.
        """
        return (list(self.get_by_attributes(attrs)) or None)[0]

    def get_by_ptype(self, ptype, unique=False):
        """
        Returns the rtype corresponding with the given Py type.

        If 'ptype' is a listlike, then it uses desired 'unique'ness
        to narrow -- zset if 'unique' is True, otherwise list. Returns
        self.default_rtype if a valid rtype isn't found.
        """
        if ptype in LISTLIKE_TYPES:
            if unique:
                return self.by_ptype_id[(ptype, True)]
            elif unique is None:
                return self.default_listlike
        return self.by_ptype_id.get(ptype) or self.default_rtype


REDIS_TYPES = _RedisTypeCollection()


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
        self.bypass_encoding = False

    def __len__(self):
        return self.len

    def __bool__(self):
        # Since we have __len__ defined we need __bool__ too to
        # override cases where __len__ is 0 causing an instance to
        # evaluate as False.
        return True

    @property
    def len(self):
        """
        The length of the Redis obj, including pending transactions.
        """
        pending = self.pipe.pending_cache.get(self.key, {})
        if 'len' in pending:
            return pending['len']
        obj_len = self.rtype.get_obj_length(self)
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
            self._rtype = REDIS_TYPES.get_from_obj(self)
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
        if data or data == 0:
            was_none = self.rtype.info.label == 'none'
            if update and not was_none:
                force_unique = self.rtype.info.has('unique')
            if not update or was_none:
                self.rtype = REDIS_TYPES.get_by_ptype(type(data), force_unique)
            self.rtype.save(self, data, update, index, was_none)
        else:
            if update:
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
          - Hash. Supports lookup_types 'field' and 'fields', which get
            and return multiple hash values given a lookup field or
            list of fields.
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
        self.rtype.get(
            self, self.rtype.configure_lookup(self, lookup, lookup_type)
        )
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
        return self.get(fields, 'fields')

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
        self.encode_member = _RedisTypeMetadata.encode_member
        self.encode_hash = REDIS_TYPES.get('hash').encode
        self.encode_list = REDIS_TYPES.get('list').encode
        self.num_batches = int(nbatches) + (0 if nbatches.is_integer() else 1)
        self.rtype = REDIS_TYPES.get_by_ptype(type(data), force_unique)
        if self.rtype.info.label == 'encoded_obj':
            raise ValueError(
                "cannot batch update an 'encoded_obj' type object -- please "
                "convert to a string first if you really need to stream this "
                "object to Redis"
            )
        self.batch_type = self.rtype.info.to_ptype
        self.target_batch_size = target_batch_size
        self.iterator = getattr(
            self, f'_{self.rtype}_iterator', self._default_iterator
        )
        self._make = getattr(
            self, f'_make_{self.rtype}', self._default_make
        )

    def _zset_iterator(self, data):
        return (self.encode_member(item) for item in data)

    def _hash_iterator(self, data):
        return self.encode_hash(data)

    def _default_iterator(self, data):
        return self.encode_list(data)

    def _make_string(self):
        total_size = len(self.data)
        index = 0
        while index < total_size:
            yield self.data[(index):(index + self.target_batch_size)]
            index += self.target_batch_size

    def _default_make(self):
        batch = []
        for i, item in enumerate(self.iterator(self.data)):
            batch.append(item)
            if (i + 1) % self.target_batch_size == 0:
                yield self.batch_type(batch)
                batch = []
        if batch:
            yield self.batch_type(batch)

    def __call__(self):
        return self._make()


class _AccumulatorFactory(object):

    @staticmethod
    def flatten(accumulated, new_values):
        accumulated.extend([
            item for vals in new_values for item in (vals or [])
        ])

    @staticmethod
    def dict(accumulated, new_values):
        for keys, vals in zip(*new_values):
            accumulated.update(dict(zip(keys, vals)))

    @staticmethod
    def hash_fields(accumulated, new_values):
        accumulated.extend([
            item for vals in new_values[1] for item in (vals or [])
        ])

    @staticmethod
    def str(collected, new_vals):
        return f"{collected}{''.join([str(v) if v else '' for v in new_vals])}"

    def __call__(self, ptype, method_name):
        return Accumulator(ptype, getattr(self, method_name))


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
        prev_bypass = self.obj.bypass_encoding
        self.obj.defer = True
        self.obj.bypass_encoding = True
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
        self.obj.bypass_encoding = prev_bypass
        return execute_rvals

    def _get_str(self, lookup):
        accumulator = self.accumulator_factory(str, 'str')
        ltype_label = type(lookup).label
        lval = lookup.value
        batches = [
            (num, min(num + self.target_batch_size - 1, lval[1]))
            for num in range(lval[0], lval[1] + 1, self.target_batch_size)
        ]
        try:
            for i, batch in enumerate(batches):
                self.obj.rtype.get(self.obj, LOOKUP_TYPES.get(ltype_label)(
                    self.obj, batch, ltype_label
                ))
                nbatch = i + 1
                is_final = nbatch == len(batches)
                do_it = self.execute_every and nbatch % self.execute_every == 0
                if is_final or do_it:
                    accumulator.push(self.obj.pipe.execute())
        except Exception:
            self.pipe.reset()
            raise
        return accumulator.pop_all()

    def _get_set(self, lookup):
        accumulator = self.accumulator_factory(list, 'flatten')
        ltype_label = type(lookup).label
        lval = lookup.value
        batches = [
            lval[(num):(num + self.target_batch_size)]
            for num in range(0, len(lval), self.target_batch_size)
        ]
        try:
            for i, batch in enumerate(batches):
                self.obj.rtype.get(self.obj, LOOKUP_TYPES.get(ltype_label)(
                    self.obj, batch, multi=True
                ))
                nbatch = i + 1
                is_final = nbatch == len(batches)
                do_it = self.execute_every and nbatch % self.execute_every == 0
                if is_final or do_it:
                    accumulator.push(self.obj.pipe.execute())
        except Exception:
            self.pipe.reset()
            raise
        return accumulator.pop_all()

    def _get_list(self, lookup):
        accumulator = self.accumulator_factory(list, 'flatten')
        ltype_label = type(lookup).label
        lval = lookup.value
        if ltype_label == 'value':
            batches = [
                lval[(num):(num + self.target_batch_size)]
                for num in range(0, len(lval), self.target_batch_size)
            ]
        else:
            batches = [
                (num, min(num + self.target_batch_size - 1, lval[1]))
                for num in range(lval[0], lval[1] + 1, self.target_batch_size)
            ]
        try:
            for i, batch in enumerate(batches):
                self.obj.rtype.get(self.obj, LOOKUP_TYPES.get(ltype_label)(
                    self.obj, batch, multi=True
                ))
                nbatch = i + 1
                is_final = nbatch == len(batches)
                do_it = self.execute_every and nbatch % self.execute_every == 0
                if is_final or do_it:
                    accumulator.push(self.obj.pipe.execute())
        except Exception:
            self.pipe.reset()
            raise
        return accumulator.pop_all()

    def _get_dict(self, lookup):
        if lookup.whole_obj_requested:
            accumulator = self.accumulator_factory(dict, 'dict')
        else:
            accumulator = self.accumulator_factory(list, 'hash_fields')
        hash_keys = lookup.value 
        keys_stack = []
        key_batches = [
            hash_keys[(num):(num + self.target_batch_size)]
            for num in range(0, len(hash_keys), self.target_batch_size)
        ]
        try:
            for i, batch_keys in enumerate(key_batches):
                self.obj.rtype.get(self.obj, LOOKUP_TYPES.get('field')(
                    self.obj, batch_keys, multi=True
                ))
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
        lookup = self.obj.rtype.configure_lookup(
            self.obj, lookup, lookup_type, batch=True
        )
        if lookup is None:
            return None
        if not lookup.multi:
            self.obj.rtype.get(self.obj, lookup)
            return self.pipe.execute()[-1]
        ptype = self.obj.rtype.info.to_ptype
        # accumulator = self.accumulator_factory(ptype, type(lookup).label)
        data = getattr(self, f'_get_{ptype.__name__}')(lookup)
        return data
