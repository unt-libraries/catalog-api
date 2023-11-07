"""
Contains tests for utils.redisobjs.
"""

import pytest

from utils import redisobjs


# FIXTURES AND TEST DATA

@pytest.fixture
def list_accumulator():
    return redisobjs.Accumulator(list, list.append)


@pytest.fixture
def set_accumulator():
    return redisobjs.Accumulator(set, set.add)


@pytest.fixture
def str_accumulator():
    def strjoin(acc_str, new_str):
        if acc_str:
            return f'{acc_str}|{new_str}'
        return new_str

    return redisobjs.Accumulator(str, strjoin)


# TESTS

@pytest.mark.parametrize('acctype, accmeth, items, expected', [
    (list, list.append, (1, 2, 3, 4), [1, 2, 3, 4]),
    (list, list.extend, ([1], [2, 3, 4], [5, 6]), [1, 2, 3, 4, 5, 6]),
    (dict, dict.update, ({'a': 1}, {'b': 2}, {'a': 3}), {'a': 3, 'b': 2}),
    (set, set.add, (1, 2, 2, 3, 2), {1, 2, 3}),
    (str, lambda a, i: '|'.join(([a] if a else []) + [i]), ['a', 'b', 'c'],
     'a|b|c'),
    (int, lambda acc, item: acc + item, [5, 5, 5], 15)
])
def test_accumulator_push_and_popall(acctype, accmeth, items, expected):
    """
    Accumulator instances should use the provided type and method to
    accumulate items as they are 'push'ed. When 'pop_all' is called, it
    should return the stored accumulated value and reset it.
    """
    acc = redisobjs.Accumulator(acctype, accmeth)
    for item in items:
        acc.push(item)
    assert acc.pop_all() == expected
    assert acc.pop_all() == acctype()


def test_pipeline_init_state():
    """
    After __init__, a Pipeline instance should have the correct
    attributes.
    """
    pl = redisobjs.Pipeline()
    assert pl.entries == []
    assert pl.accumulators == []
    assert hasattr(pl.pipe, 'command_stack')
    assert len(pl.pipe.command_stack) == 0


def test_pipeline_add_appends_command_callback_and_accum(list_accumulator):
    """
    The Pipeline.add method should queue up the given command, with the
    given key, args, and kwargs, plus the given callback, accumulator
    function, and 'end_of_acc_group' signal. It should do this WITHOUT
    sending the commands to Redis.
    """
    pl = redisobjs.Pipeline()
    def callback(result):
        return 'bar'

    pl.add(
        'rpush', 'test:pipeline', args=['1'], callback=callback,
        accumulator=list_accumulator
    )
    pl.mark_accumulator_pop()
    pl.add('lpos', 'test:pipeline', args=['1'], kwargs={'rank': 1})
    assert len(pl.pipe.command_stack) == 2
    assert pl.entries == [
        (callback, list_accumulator, True),
        (None, None, False)
    ]
    assert len(redisobjs.REDIS_CONNECTION.keys()) == 0


def test_pipeline_execute_without_callback_or_accum_runs_commands():
    """
    The Pipeline.execute method should execute queued commands and
    return the value that redis.py and Redis return, directly.
    """
    conn = redisobjs.REDIS_CONNECTION
    pl = redisobjs.Pipeline()
    pl.add('rpush', 'test:pipeline_list', args=[1])
    pl.add('set', 'test:pipeline_string', args=['foo'])
    set_return = pl.execute()
    pl.add('lrange', 'test:pipeline_list', args=[0, -1])
    pl.add('get', 'test:pipeline_string')
    get_return = pl.execute()
    sanity_check_list = conn.lrange('test:pipeline_list', 0, -1)
    sanity_check_string = conn.get('test:pipeline_string')
    assert sanity_check_list == ['1']
    assert sanity_check_string == 'foo'
    assert set_return == [1, 1]
    assert get_return == [['1'], 'foo']


def test_pipeline_execute_with_callbacks_uses_callbacks():
    """
    The Pipeline.execute method should run each Redis return value
    through the corresponding callback function if one was provided
    when the command was added.
    """
    pl = redisobjs.Pipeline()
    pl.add(
        'rpush', 'test:pipeline_list', args=[1],
        callback=lambda r: 'yes' if r == 1 else 'no'
    )
    pl.add(
        'set', 'test:pipeline_string', args=['foo'],
        callback=lambda r: 'done' if r == 1 else 'not done'
    )
    set_return = pl.execute()
    pl.add(
        'lrange', 'test:pipeline_list', args=[0, -1],
        callback=lambda r: [int(i) for i in r]
    )
    pl.add('get', 'test:pipeline_string', callback=lambda r: f'{r}bar')
    get_return = pl.execute()
    assert set_return == ['yes', 'done']
    assert get_return == [[1], 'foobar']


def test_pipeline_execute_with_accums_uses_accums(list_accumulator,
                                                  set_accumulator,
                                                  str_accumulator):
    """
    The Pipeline.execute method should compile results using the
    corresponding accumulator, if one was provided when the command was
    added.
    """
    conn = redisobjs.REDIS_CONNECTION
    pl = redisobjs.Pipeline()
    groups = [
        { 
            'items': ['1', '2', '3'],
            'accumulator': list_accumulator,
            'expected': ['1', '2', '3']
        }, {
            'items': ['a', 'b', 'c', 'c', 'b'],
            'accumulator': set_accumulator,
            'expected': {'a', 'b', 'c'},
        }, {
            'items': ['x', 'y', 'z'],
            'accumulator': str_accumulator, 
            'expected': 'x|y|z'
        }
    ]
    for i, group in enumerate(groups):
        conn.rpush(f'test:group{i}', *group['items'])
        for j, val in enumerate(group['items']):
            pl.add(
                'lindex', f'test:group{i}', args=[j],
                accumulator = group['accumulator']
            )
        pl.mark_accumulator_pop()
    results = pl.execute()
    for result, group in zip(results, groups):
        assert result == group['expected']


def test_pipeline_execute_without_ending_accum(list_accumulator):
    """
    If an 'end_of_acc_group' signal is never sent, Pipeline.execute
    will continue accumulating into the same group for each command
    where an accumulator is provided. The final accumulated result is
    appended to the outer result list even without the 'end' signal.
    """
    conn = redisobjs.REDIS_CONNECTION
    pl = redisobjs.Pipeline()
    conn.set('test:string', 'foobarbaz')
    conn.rpush('test:list', '1', '2', '3', '4', '5')
    pl.add('lindex', 'test:list', args=[0], accumulator=list_accumulator)
    pl.add('lindex', 'test:list', args=[2], accumulator=list_accumulator)
    pl.add('get', 'test:string')
    pl.add('lindex', 'test:list', args=[4], accumulator=list_accumulator)
    results = pl.execute()
    assert results == ['foobarbaz', ['1', '3', '5']]


def test_pipeline_execute_w_callbacks_and_accums_uses_both(list_accumulator):
    """
    The Pipeline.execute method should use any callbacks and/or
    accumulators provided when commands were queued via 'add'.
    """
    conn = redisobjs.REDIS_CONNECTION
    pl = redisobjs.Pipeline()
    conn.rpush('test:list', 'a', 'b', 'c', 'd', 'e')
    for i in range(5):
        pl.add(
            'lindex', 'test:list', args=[i], callback=lambda r: f'_{r}_',
           accumulator=list_accumulator 
        )
    results = pl.execute()
    assert results == [['_a_', '_b_', '_c_', '_d_', '_e_']]


# Note -- as unit tests, many of the below are not exactly pure. Where
# I use the same class to set data in Redis and then retrieve it, I'm
# testing the setter and getter at once. So a failure could be caused
# by either method. But all I really care about is that RedisObject
# interacts with the data in the expected way.

def test_redisobject_init_state():
    """
    After __init__, a RedisObject instance should have the correct
    attributes. If the Redis key does not exist, it is not (yet)
    created.
    """
    r = redisobjs.RedisObject('my_entity', 'item_id')
    assert r.entity == 'my_entity'
    assert r.id == 'item_id'
    assert r.key == 'my_entity:item_id'
    assert r.pipe is not None
    assert r.defer == False
    assert r.conn.keys() == []


def test_redisobject_rtype_property_none_if_not_set():
    """
    The RedisObject.rtype property is "none" for a key that is not yet
    set.
    """
    r = redisobjs.RedisObject('test', 'thing')
    assert r.rtype == 'none'


def test_redisobject_rtype_property_correct_for_existing_key():
    """
    The RedisObject.rtype property should return the correct type if a
    key already exists.
    """
    conn = redisobjs.REDIS_CONNECTION
    conn.set('test:thing', 'foo')
    r = redisobjs.RedisObject('test', 'thing')
    assert r.rtype == 'string'


def test_redisobject_set_saves_key():
    """
    The RedisObject.set method should save the object key to Redis.
    """
    r = redisobjs.RedisObject('my_entity', 'item_id')
    r.set('my_value')
    assert r.conn.keys() == ['my_entity:item_id']


@pytest.mark.parametrize('value', [
    None,
    '',
    [],
    tuple(),
    {},
    set(),
    0,
    1,
    0.0,
    0.1,
    'MyString',
    [1, 2, 3, 4, 5],
    (1, 2, 3, 4, 5),
    ['a', 'b', 'c', 'd', 'e'],
    [[1, 2], [3, 4], [5, 6]],
    {'apples': ['Delicious', 'Jazz', 'Granny Smith', 'Gala'],
     'colors': ['red', 'blue', 'green', 'yellow'],
     'empty': [],
     'blorp': ['meep', 'morp']},
    {'red', 'blue', 'blue', 'green'},
    [{'what': 'location', 'code': 'w4m'},
     {'what': 'location', 'code': 'w3'},
     {'what': 'itype', 'code': 1}],
    ({'what': 'location', 'code': 'w4m'},
     {'what': 'location', 'code': 'w3'},
     {'what': 'itype', 'code': 1})
])
def test_redisobject_set_and_get_work_correctly(value):
    """
    The RedisObject.set method should save the given value to Redis
    at a specific key and return the value. Instantiating a new one
    that uses that same key and calling the `get` method should return
    the same value. (Tuples are returned as lists.)
    """
    assert redisobjs.RedisObject('test_table', 'test_item').set(value) == value
    r = redisobjs.RedisObject('test_table', 'test_item')
    if not value and value != 0:
        assert r.get() is None
    elif isinstance(value, tuple):
        assert r.get() == list(value)
    else:
        assert r.get() == value


@pytest.mark.parametrize('old_val, old_funq, new_val, new_funq', [
    (None, None, 'test val', None),
    ('test val', None, None, None),
    ('test val', None, 0, None),
    ('test val', None, [1, 2, 3, 4, 5], True),
    ('test val', None, [1, 2, 3, 4, 5], False),
    ([1, 2, 3, 4, 5], True, 'test val', None),
    ([1, 2, 3, 4, 5], False, 'test val', None),
    ([1, 2, 3, 4, 5], True, [5, 4, 3, 2, 1], True),
    ([1, 2, 3, 4, 5], True, [5, 4, 3, 2, 1], False),
    ([1, 2, 3, 4, 5], False, [5, 4, 3, 2, 1], True),
    ([1, 2, 3, 4, 5], False, [5, 4, 3, 2, 1], False),
    ({1, 2, 3}, None, [1, 2, 3], True),
    ({'t1': [1, 2, 3], 't2': [4, 5, 6]}, None, {'t3': [7, 8, 9]}, None)
])
def test_redisobject_set_without_update_overwrites_existing(old_val, old_funq,
                                                            new_val, new_funq):
    """
    By default (update=False), the RedisObject.set method should
    silently and automatically overwrite the existing key.
    """
    redisobjs.RedisObject('test_table', 'test_item').set(
        old_val, force_unique=old_funq
    )
    redisobjs.RedisObject('test_table', 'test_item').set(
        new_val, force_unique=new_funq
    )
    assert redisobjs.RedisObject('test_table', 'test_item').get() == new_val


@pytest.mark.parametrize('old_val, new_val', [
    ('test val', None),
    ('test val', ''),
    ('test val', []),
    ('test val', tuple()),
    ('test val', {}),
    ('test val', set())
])
def test_redisobject_set_empty_deletes_existing_key(old_val, new_val):
    """
    By default (update=False), if an existing key is set to a non-zero
    empty value, the key is deleted entirely from Redis. Attempting to
    get it returns None.
    """
    conn = redisobjs.REDIS_CONNECTION
    assert 'test:item' not in conn.keys()
    redisobjs.RedisObject('test', 'item').set(old_val)
    assert 'test:item' in conn.keys()
    redisobjs.RedisObject('test', 'item').set(new_val)
    assert 'test:item' not in conn.keys()
    assert redisobjs.RedisObject('test', 'item').get() is None
    

@pytest.mark.parametrize('value, force_unique, exp', [
    ([1, 2, 2, 3], True, [1, 2, 3]),

    # In a sorted set (where force_unique is True), repeating an
    # earlier element after another reorders it.
    ([1, 2, 2, 3, 2], True, [1, 3, 2]),
    ([1, 2, 2, 3], False, [1, 2, 2, 3]),
    ([1, 2, 2, 3, 2], False, [1, 2, 2, 3, 2]),

    # Strings, sets, and hashes accept but ignore force_unique.
    ('my string', True, 'my string'),
    ('my string', False, 'my string'),
    ({1, 2, 3, 2}, True, {1, 2, 3}),
    ({1, 2, 3, 2}, False, {1, 2, 3}),
    ({'test': 'val'}, True, {'test': 'val'}),
    ({'test': 'val'}, False, {'test': 'val'})
])
def test_redisobject_set_and_get_with_force_unique(value, force_unique, exp):
    """
    When RedisObject.set is used to save a list, the 'force_unique'
    option determines whether Redis stores only unique values or not.
    """
    r_set = redisobjs.RedisObject('test_table', 'test_item')
    r_set.set(value, force_unique=force_unique)
    r_get = redisobjs.RedisObject('test_table', 'test_item')
    assert r_get.get() == exp


@pytest.mark.parametrize('init, force_unique, newval, index, expected', [
    # New values are set from scratch
    (None, None, None, None, None),
    (None, None, '', None, None),
    (None, False, [], None, None),
    (None, True, [], None, None),
    (None, None, {}, None, None),
    (None, None, 'test', None, 'test'),
    (None, None, '0', None, '0'),
    (None, None, 0, None, 0),
    (None, None, 0.1, None, 0.1),
    (None, False, [1, 2, 3], None, [1, 2, 3]),
    (None, True, [1, 2, 3], None, [1, 2, 3]),
    (None, None, {'a': 'z'}, None, {'a': 'z'}),
    (None, None, {1, 2, 3}, None, {1, 2, 3}),
    (None, False, [1, 2, 3], 2, [None, None, 1, 2, 3]),
    (None, True, [1, 2, 3], 2, [1, 2, 3]),
    (None, None, 'test', 2, '\x00\x00test'),
    (None, None, {'a': 'z'}, 2, {'a': 'z'}),
    (None, None, {1, 2, 3}, 2, {1, 2, 3}),

    # Strings
    ('foo', None, None, None, 'foo'),
    ('foo', None, '', None, 'foo'),
    ('foo', None, ' bar', None, 'foo bar'),
    ('foo', None, '0', None, 'foo0'),
    # ('index' allows you to insert/overwrite part of a string)
    ('foo', None, 'bar', 0, 'bar'),
    ('foo', None, 'bar', 1, 'fbar'),
    ('foo', None, 'bar', 3, 'foobar'),
    ('foo', None, 'bar', 4, 'foo\x00bar'),
    ('foo', None, 'bar', 5, 'foo\x00\x00bar'),
    ('foo', None, 'bar', -1, 'fobar'),
    ('foo', None, 'bar', -3, 'bar'),
    ('foo', None, 'bar', -10, 'bar'),

    # Non-string encoded objects
    (100, None, None, None, 100),
    (100, None, 200, None, 200),
    (100, None, 200, 1, 200),
    (100, None, 0, None, 0),

    # Lists and zsets
    ([1, 2, 3], False, None, None, [1, 2, 3]),
    ([1, 2, 3], False, [], None, [1, 2, 3]),
    ([1, 2, 3], False, [3, 2, 1], None, [1, 2, 3, 3, 2, 1]),
    ([1, 2, 3], False, (3, 2, 1), None, [1, 2, 3, 3, 2, 1]),
    ([1, 2, 3], True, None, None, [1, 2, 3]),
    ([1, 2, 3], True, [], None, [1, 2, 3]),
    ([1, 2, 3], True, [3, 2, 1], None, [3, 2, 1]),
    ([1, 2, 3], True, [4], None, [1, 2, 3, 4]),
    ([1, 2, 3], True, [{'a': 'z'}], None, [1, 2, 3, {'a': 'z'}]),
    # ('index' allows you to insert/overwrite in a list/zset)
    (['a', 'b'], True, [None], 0, [None, 'b']),
    (['a', 'b'], True, [''], 0, ['', 'b']),
    (['a', 'b'], True, ['c'], 0, ['c', 'b']),
    (['a', 'b'], True, ['c'], 1, ['a', 'c']),
    (['a', 'b'], True, ('c', 'b', 'd'), 0, ['c', 'b', 'd']),
    (['a', 'b'], True, (['b1', 'b2'], ['c1']), 1, ['a', ['b1', 'b2'], ['c1']]),
    (['a', 'b', 'c'], True, ('1', '2'), 0, ['1', '2', 'c']),
    (['a', 'b', 'c'], True, ('1', '2'), 1, ['a', '1', '2']),
    (['a', 'b', 'c'], True, ('1', '2'), 2, ['a', 'b', '1', '2']),
    (['a', 'b', 'c'], True, ('1', '2'), 3, ['a', 'b', 'c', '1', '2']),
    # (for a zset, since members have to be unique, you can't skip
    # index numbers.)
    (['a', 'b', 'c'], True, ('1', '2'), 5, ['a', 'b', 'c', '1', '2']),
    (['a', 'b', 'c'], True, ('1', '2'), -1, ['a', 'b', '1', '2']),
    (['a', 'b', 'c'], True, ('1', '2'), -2, ['a', '1', '2']),
    (['a', 'b', 'c'], True, ('1', '2'), -3, ['1', '2', 'c']),
    (['a', 'b', 'c'], True, ('1', '2'), -10, ['1', '2', 'c']),
    (['a', 'b'], False, [None], 0, [None, 'b']),
    (['a', 'b'], False, [''], 0, ['', 'b']),
    (['a', 'b'], False, ['c'], 0, ['c', 'b']),
    (['a', 'b'], False, ['c'], 1, ['a', 'c']),
    (['a', 'b'], False, ('c', 'b', 'd'), 0, ['c', 'b', 'd']),
    (['a', 'b'], False, ({'b': 'z'}, 'c'), 1, ['a', {'b': 'z'}, 'c']),
    (['a', 'b', 'c'], False, ('1', '2'), 0, ['1', '2', 'c']),
    (['a', 'b', 'c'], False, ('1', '2'), 1, ['a', '1', '2']),
    (['a', 'b', 'c'], False, ('1', '2'), 2, ['a', 'b', '1', '2']),
    (['a', 'b', 'c'], False, ('1', '2'), 3, ['a', 'b', 'c', '1', '2']),
    # (a list lets you skip index numbers.)
    (['a', 'b', 'c'], False, ('1', '2'), 5,
     ['a', 'b', 'c', None, None, '1', '2']),
    (['a', 'b', 'c'], False, ('1', '2'), -1, ['a', 'b', '1', '2']),
    (['a', 'b', 'c'], False, ('1', '2'), -2, ['a', '1', '2']),
    (['a', 'b', 'c'], False, ('1', '2'), -3, ['1', '2', 'c']),
    (['a', 'b', 'c'], False, ('1', '2'), -10, ['1', '2', 'c']),

    # Hashes
    ({'a': 'z'}, None, None, None, {'a': 'z'}),
    ({'a': 'z'}, None, {}, None, {'a': 'z'}),
    ({'a': 'z'}, None, {'b': 'y'}, None, {'a': 'z', 'b': 'y'}),
    ({'a': 'z'}, None, {'b': 'y', 'c': 'x'}, None, 
     {'a': 'z', 'b': 'y', 'c': 'x'}),
    ({'a': 'z', 'b': 'y'}, None, {'a': 1, 'b': 2}, None, {'a': 1, 'b': 2}),
    ({'a': 'z'}, None, {'a': 1, 'b': 2}, None, {'a': 1, 'b': 2}),
    ({'a': 'z'}, None, {'b': {'t': 'v'}}, None, {'a': 'z', 'b': {'t': 'v'}}),
    ({'a': 'z', 'b': 'y'}, None, {'a': [1, 2, 3]}, None,
     {'a': [1, 2, 3], 'b': 'y'}),
    # ('index' is ignored for hashes)
    ({'a': 'z', 'b': 'y'}, None, {'a': 1, 'b': 2}, 1, {'a': 1, 'b': 2}),

    # Sets
    ({1, 2, 3}, None, None, None, {1, 2, 3}),
    ({1, 2, 3}, None, set(), None, {1, 2, 3}),
    ({1, 2, 3}, None, {3, 2, 1}, None, {1, 2, 3}),
    ({1, 2, 3}, None, {4}, None, {1, 2, 3, 4}),
    ({1, 2, 3}, None, {3, 4, 5}, None, {1, 2, 3, 4, 5}),
    # ('index' is ignored for sets)
    ({1, 2, 3}, None, {4}, 1, {1, 2, 3, 4}),
])
def test_redisobject_set_with_update(init, force_unique, newval, index,
                                     expected):
    """
    When RedisObject.set is used and the 'update' kwarg is True, the
    new data is added to the existing data. If the key does not yet
    exist, it is created and set normally.
    """
    key = ('test', 'item')
    if init is not None:
        redisobjs.RedisObject(*key).set(init, force_unique=force_unique)
    assert redisobjs.RedisObject(*key).set(
        newval, force_unique=force_unique, update=True, index=index
    ) == newval
    assert redisobjs.RedisObject(*key).get() == expected


@pytest.mark.parametrize('init, force_unique, newval, index, expected', [
    (None, None, 'test', 2, '\x00\x00test'),
    (None, None, 10, 2, 10),
    (None, False, [1, 2, 3], 2, [None, None, 1, 2, 3]),
    (None, True, [1, 2, 3], 2, [1, 2, 3]),
    (None, None, {1, 2, 3}, 2, {1, 2, 3}),
    (None, None, {'a': 'b'}, 2, {'a': 'b'}),
])
def test_redisobject_set_without_update_with_index(init, force_unique, newval,
                                                   index, expected):
    """
    This is to test the edge case where you use RedisObject.set with
    'update=False' but provide an index value. For the 'list' and
    'string' types, it should pad the beginning of the data. For other
    types, it should have no effect.
    """
    key = ('test', 'item')
    if init is not None:
        redisobjs.RedisObject(*key).set(init, force_unique=force_unique)
    assert redisobjs.RedisObject(*key).set(
        newval, force_unique=force_unique, update=False, index=index
    ) == newval
    assert redisobjs.RedisObject(*key).get() == expected


@pytest.mark.parametrize(
    'init, force_unique, newval, old_rtype, new_rtype, cmp_ptype_label',
    [
        ('ab', False, [1, 2, 3], 'string', 'list', 'string'),
        ('ab', True, [1, 2, 3], 'string', 'zset', 'string'),
        ('ab', None, {1, 2, 3}, 'string', 'set', 'string'),
        ('ab', None, {'a': 'z'}, 'string', 'hash', 'string'),
        ('ab', None, 0, 'string', 'encoded_obj', 'string'),
        (0, None, 'ab', 'encoded_obj', 'string',
         'any JSON-serializable type except list, tuple, string, dict, or '
         'set'),
        ([1, 2, 3], False, 'foo', 'list', 'string', 'list or tuple'),
        ([1, 2, 3], True, 'foo', 'zset', 'string', 'list or tuple'),
        ({1, 2, 3}, None, 'foo', 'set', 'string', 'set'),
        ({'a': 'z'}, None, 'foo', 'hash', 'string', 'hash'),
    ]
)
def test_redisobject_set_with_update_wrong_type(init, force_unique, newval,
                                                old_rtype, new_rtype,
                                                cmp_ptype_label):
    """
    When RedisObject.set is used and the 'update' kwarg is True but the
    provided data type is not compatible with the data type in Redis,
    it should raise a TypeError that includes the expected message.
    """
    key = ('test', 'item')
    redisobjs.RedisObject(*key).set(init, force_unique=force_unique)
    with pytest.raises(TypeError) as excinfo:
        redisobjs.RedisObject(*key).set(
            newval, force_unique=force_unique, update=True
        )
    err_msg = str(excinfo.value)
    exp_msg = f'Cannot update existing {old_rtype} data with {new_rtype} data'
    assert exp_msg in err_msg
    assert cmp_ptype_label in err_msg


@pytest.mark.parametrize('value', [
    {'my_set': {1, 2, 3}},
    [{1, 2, 3}, {4, 5, 6}],
])
def test_redisobject_inner_sets_fail(value):
    """
    Using sets inside a container type fails because sets are not JSON
    serializable.
    """
    r_set = redisobjs.RedisObject('test_table', 'test_item_with_sets')
    with pytest.raises(TypeError):
        r_set.set(value)


def test_redisobject_set_and_get_different_keys():
    """
    Setting different values using different keys should return the
    appropriate value for each key.
    """
    redisobjs.RedisObject('test_table1', 'test_item1').set('1_1')
    redisobjs.RedisObject('test_table1', 'test_item2').set('1_2')
    redisobjs.RedisObject('test_table2', 'test_item1').set('2_1')
    redisobjs.RedisObject('test_table2', 'test_item2').set('2_2')
    assert redisobjs.RedisObject('test_table1', 'test_item1').get() == '1_1'
    assert redisobjs.RedisObject('test_table1', 'test_item2').get() == '1_2'
    assert redisobjs.RedisObject('test_table2', 'test_item1').get() == '2_1'
    assert redisobjs.RedisObject('test_table2', 'test_item2').get() == '2_2'


def test_redisobject_set_and_defer_with_new_pipeline():
    """
    When 'defer' is True but no pipeline ('pipe') is provided on
    initialization, then a new Pipeline object should be created and
    used. The 'set' operation should return the Pipeline object. When
    executed, the Pipeline object should perform the operation.
    """
    r = redisobjs.RedisObject('test', 'item', defer=True)
    pipe = r.set('my_value')
    assert redisobjs.RedisObject('test', 'item').get() is None
    pipe = r.set('next_value')
    assert redisobjs.RedisObject('test', 'item').get() is None
    assert pipe.execute() == [[0, 8], [1, 10]]
    assert redisobjs.RedisObject('test', 'item').get() == 'next_value'


def test_redisobject_set_and_defer_with_user_pipeline():
    """
    When 'defer' is True and a user pipeline ('pipe') is provided on
    initialization, any 'set' operations should be queued on the user
    pipeline object. When executed, the Pipeline object should perform
    the operation.
    """
    pipe = redisobjs.Pipeline()
    r = redisobjs.RedisObject('test', 'item', pipe=pipe, defer=True)
    r2 = redisobjs.RedisObject('test', 'item2', pipe=pipe, defer=True)
    assert r.pipe == r2.pipe == pipe
    assert r.set('my_value') == pipe
    assert r2.set('other_value') == pipe
    assert redisobjs.RedisObject('test', 'item').get() is None
    assert redisobjs.RedisObject('test', 'item2').get() is None
    assert pipe.execute() == [[0, 8], [0, 11]]
    assert redisobjs.RedisObject('test', 'item').get() == 'my_value'
    assert redisobjs.RedisObject('test', 'item2').get() == 'other_value' 


@pytest.mark.parametrize('init, f_unq, i, vals, exp_return, exp_list', [
    (['a', 'b'], True, 0, [], [None], ['a', 'b']),
    (['a', 'b'], True, 0, ['c'], [[1, 1]], ['c', 'b']),
    (['a', 'b'], True, 0, ['c', 'b', 'd'], [[2, 3]], ['c', 'b', 'd']),
    (['a', 'b'], False, 0, [], [None], ['a', 'b']),
    (['a', 'b'], False, 0, ['c'], [True], ['c', 'b']),
    (['a', 'b'], False, 0, ['c', 'b', 'd'], [[True, True, 3]],
     ['c', 'b', 'd']),
    (['a', 'b'], False, 0, ['c', 'b', 'd', 'e'], [[True, True, 4]],
     ['c', 'b', 'd', 'e']),
])
def test_redisobject_set_and_defer_list_zset(init, f_unq, i, vals, exp_return,
                                             exp_list):
    """
    This is to test the pipe execution return values when using the
    'set' method when 'defer' is True and the data type is a zset or a
    list. When executed, the pipe will return the expected value(s)
    from Redis. Getting the full object will produce the expected list,
    but only after the pipeline is executed.
    """
    r_norm = redisobjs.RedisObject('test', 'list-zset-defer')
    r_norm.set(init, force_unique=f_unq)
    r_defer = redisobjs.RedisObject('test', 'list-zset-defer', defer=True)
    pipe = r_defer.set(vals, update=True, index=i)
    assert pipe == r_defer.pipe
    assert r_norm.get() == init
    assert pipe.execute() == exp_return
    assert r_norm.get() == exp_list


@pytest.mark.parametrize('updates, expected', [
    # zsets
    # appending
    ([(['1', '2'], True, False, None),
      (['3', '4'], True, True, None),
      (['5', '6', '7'], True, True, None)],
     ['1', '2', '3', '4', '5', '6', '7']),
    # using indexes
    ([(['1', '2'], True, False, None),
      (['3'], True, True, 0),
      (['4', '5', '6'], True, True, 1),
      (['7'], True, True, -1)], ['3', '4', '5', '7']),
    ([(['1', '2', '3', '4', '5'], True, False, None),
      (['10', '11', '12', '13', '14'], True, True, 2),
      (['3'], True, True, -2)], ['1', '2', '10', '11', '12', '3', '14']),
    # (with zsets, specifying an index > the current length adds the
    # items to the end, using the specified index as the score -- so
    # it doesn't pad with None, but you can fill the unused scores with
    # items and effectively insert them into the zset there)
    ([(['1', '2', '3'], True, False, None),
      (['4', '5', '6'], True, True, 10),
      (['7', '8'], True, True, 3)], ['1', '2', '3', '7', '8', '4', '5', '6']),
    # update is False
    ([(['1', '2', '3', '4', '5'], True, False, None),
      (['6', '7'], True, False, None),
      (['8'], True, True, None)], ['6', '7', '8']),
    # starts with an update, and specifies an index -- no None-padding
    # due to zset
    ([(['1', '2', '3', '4', '5'], True, True, 2),
      (['6', '7'], True, True, None)], ['1', '2', '3', '4', '5', '6', '7']),
    # zsets with duplicate members always set a duplicate item's score
    # using the last occurrence -- earlier occurrences leave score
    # holes
    ([(['1', '2', '3', '4', '2'], True, False, None),
      (['3'], True, True, None),
      (['5', '6'], True, True, None),
      (['7'], True, True, 1)], ['1', '7', '4', '2', '3', '5', '6']),

    # lists
    # appending
    ([(['1', '2'], False, False, None),
      (['3', '4'], False, True, None),
      (['5', '6', '7'], False, True, None)],
     ['1', '2', '3', '4', '5', '6', '7']),
    # using indexes
    ([(['1', '2'], False, False, None),
      (['3'], False, True, 0),
      (['4', '5', '6'], False, True, 1),
      (['7'], False, True, -1)], ['3', '4', '5', '7']),
    ([(['1', '2', '3', '4', '5'], False, False, None),
      (['10', '11', '12', '13', '14'], False, True, 2),
      (['3'], False, True, -2)], ['1', '2', '10', '11', '12', '3', '14']),
    ([(['1', '2', '3'], False, False, None),
      (['4', '5', '6'], False, True, 6),
      (['7', '8'], False, True, 3)],
     ['1', '2', '3', '7', '8', None, '4', '5', '6']),
    # update is False
    ([(['1', '2', '3', '4', '5'], False, False, None),
      (['6', '7'], False, False, None),
      (['8'], False, True, None)], ['6', '7', '8']),
    # starts with an update, and specifies an index
    ([(['1', '2', '3', '4', '5'], False, True, 2),
      (['6', '7'], False, True, None)],
     [None, None, '1', '2', '3', '4', '5', '6', '7']),

    # raw strings
    # appending
    ([('12', None, False, None),
      ('34', None, True, None),
      ('567', None, True, None)], '1234567'),
    # using indexes
    ([('12', None, False, None),
      ('3', None, True, 0),
      ('456', None, True, 1),
      ('7', None, True, -1)], '3457'),
    ([('12345', None, False, None),
      ('1011121314', None, True, 2),
      ('3', None, True, -2)], '121011121334'),
    ([('123', None, False, None),
      ('456', None, True, 6),
      ('78', None, True, 3)], '12378\x00456'),
    # update is False
    ([('12345', None, False, None),
      ('67', None, False, None),
      ('8', None, True, None)], '678'),
    # starts with an update, and specifies an index
    ([('12345', None, True, 2),
      ('67', None, True, None)], '\x00\x001234567'),

    # hashes
    # adding to a hash
    ([({'a': 'z'}, None, False, None),
      ({'b': 'y', 'c': 'x'}, None, True, None),
      ({'d': 'w'}, None, True, None)],
      {'a': 'z', 'b': 'y', 'c': 'x', 'd': 'w'}),
    # overriding existing fields
    ([({'a': 'z'}, None, False, None),
      ({'a': 1, 'b': 2}, None, True, None),
      ({'b': 3}, None, True, None)], {'a': 1, 'b': 3}),
    # update is False
    ([({'a': 'z'}, None, False, None),
      ({'b': 'y'}, None, False, None),
      ({'c': 'x'}, None, True, None)], {'b': 'y', 'c': 'x'}),
    # starts with an update
    ([({'a': 'z'}, None, True, None),
      ({'b': 'y'}, None, True, None)], {'a': 'z', 'b': 'y'}),

])
def test_redisobject_set_and_defer_multiple_updates(updates, expected):
    """
    When performing multiple deferred 'set' operations that update a
    key, the length of data the key stores varies per operation. But,
    if operations are deferred, then operations n+1, n+2, ... may
    depend on the future length of the data, not the current length.
    This is to test and make sure this works correctly.
    """
    r = redisobjs.RedisObject('test', 'defer-multi-update', defer=True)
    for args in updates:
        r.set(*args)
    r.pipe.execute()
    r.defer = False
    assert r.get() == expected


def test_redisobject_setfield_calls_set(mocker):
    """
    The RedisObject.set_field method is just a convenience method that
    calls 'set' with 'update=True' to set fields on an existing hash.
    """
    r = redisobjs.RedisObject('test', 'item')
    r.set = mocker.Mock()
    r.set_field({'a': 'z'})
    r.set.assert_called_with({'a': 'z'}, update=True)


def test_redisobject_setvalue_single_value(mocker):
    """
    The RedisObject.set_value method is just a convenience method that
    calls 'set' with 'update=True' to set data values on an existing
    list or zset. When a single value is provided, it returns only that
    value (not in a list).
    """
    r = redisobjs.RedisObject('test', 'item')
    r.set = mocker.Mock(return_value=['a'])
    assert r.set_value(1, 'a') == 'a'
    r.set.assert_called_with(('a',), update=True, index=1)


def test_redisobject_setvalue_multiple_values(mocker):
    """
    The RedisObject.set_value method is just a convenience method that
    calls 'set' with 'update=True' to set data values on an existing
    list or zset. When multiple values are provided, it returns them as
    a list.
    """
    r = redisobjs.RedisObject('test', 'item')
    r.set = mocker.Mock(return_value=['a', 'b'])
    assert r.set_value(1, 'a', 'b') == ['a', 'b']
    r.set.assert_called_with(('a', 'b'), update=True, index=1)


def test_redisobject_get_nonexistent_key_returns_none():
    """
    For ease of use, trying to get a key that doesn't exist returns
    None. This behavior is patterned after the dict `get` method.
    """
    r = redisobjs.RedisObject('not set', 'not set')
    assert r.conn.keys() == []
    assert r.get() == None


@pytest.mark.parametrize('init, field, expected', [
    ({'a': 'z', 'b': 'y', 'c': 'x'}, tuple(), None),
    ({'a': 'z', 'b': 'y', 'c': 'x'}, 'b', 'y'),
    ({'a': 'z', 'b': 'y', 'c': 'x'}, ('b', 'a', 'c'), ['y', 'z', 'x']),
    ({'a': 'z', 'b': 'y', 'c': 'x'}, 'd', None),
    ({'a': 'z', 'b': 'y', 'c': 'x'}, ('d', 'e'), [None, None]),
    ({'a': 'z', 'b': 'y', 'c': 'x'}, ('a', 'd', 'c'), ['z', None, 'x']),
    ({'a': [1, 2, 3], 'b': 'y', 'c': 'x'}, 'a', [1, 2, 3]),
    ({'a': {'a1': 'z1', 'a2': 'z2'}, 'b': 'y', 'c': 'x'}, 'a',
     {'a1': 'z1', 'a2': 'z2'}),
    ('a string', 'a', None),
    ('a string', ['a', 'b'], None),
    (['a', 'list'], 'a', None),
    ({'a', 'set'}, 'a', None)
])
def test_redisobject_getfield(init, field, expected):
    """
    For a hash type object, the RedisObject.get_field method returns
    the value(s) in the requested field(s). If one field arg is
    provided, it returns the one field; if multiple field args are
    provided, it returns the values as a list. For everything else, or
    if a field does not exist, it returns None.
    """
    redisobjs.RedisObject('test', 'hash').set(init)
    args = field if isinstance(field, tuple) else (field,)
    assert redisobjs.RedisObject('test', 'hash').get_field(*args) == expected


def test_redisobject_getfield_with_defer():
    """
    When a RedisObject instance has defer set to True, a 'get_field'
    operation wil queue the operation on the instance's 'pipe' object
    and return the pipe. When executed, the pipe will include the
    expected value(s) from Redis in its list of results.
    """
    redisobjs.RedisObject('test', 'hash').set({'a': 'z', 'b': 'y', 'c': 'x'})
    r = redisobjs.RedisObject('test', 'hash', defer=True)
    pipe = r.get_field('a', 'b', 'c')
    assert pipe.execute() == [['z', 'y', 'x']]


@pytest.mark.parametrize('init, force_unique, value, expected', [
    (['a', 'b', 'c'], True, tuple(), None),
    (['a', 'b', 'c'], True, 'a', 0),
    (['a', 'b', 'c'], True, 'b', 1),
    (['a', 'b', 'c'], True, 'c', 2),
    (['a', 'b', 'c'], True, 'd', None),
    (['a', 'b', 'c'], True, ('a', 'b'), [0, 1]),
    (['a', 'b', 'c'], True, ('d', 'e'), [None, None]),
    (['a', 'b', 'c'], True, ('a', 'd'), [0, None]),
    (['a', 'b', 'c'], False, tuple(), None),
    (['a', 'b', 'c'], False, 'a', 0),
    (['a', 'b', 'c'], False, 'b', 1),
    (['a', 'b', 'c'], False, 'c', 2),
    (['a', 'b', 'c'], False, 'd', None),
    (['a', 'b', 'c'], False, ('a', 'b'), [0, 1]),
    (['a', 'b', 'c'], False, ('d', 'e'), [None, None]),
    (['a', 'b', 'c'], False, ('a', 'd'), [0, None]),
    (['a', 'c', 'b', 'c'], False, 'c', [1, 3]),
    (['a', 'c', 'b', 'c'], False, ('c', 'b'), [[1, 3], 2]),
    ('a string', None, 'a', None),
    ('a string', None, ('a', 'b'), None),
    ({'a': 'hash'}, None, 'a', None),
    ({'a', 'set'}, None, 'a', None)
])
def test_redisobject_getindex(init, force_unique, value, expected):
    """
    For a list or zset type object, the RedisObject.get_index method
    returns the index position of the provided value. If one value arg
    is provided, it returns the one value; if multiple value args are
    provided, it returns the index positions as a list. For everything
    else, or if a value does not exist, it returns None.
    """
    redisobjs.RedisObject('test', 'list').set(init, force_unique=force_unique)
    args = value if isinstance(value, tuple) else (value,)
    assert redisobjs.RedisObject('test', 'list').get_index(*args) == expected


def test_redisobject_getindex_with_defer():
    """
    When a RedisObject instance has defer set to True, a 'get_index'
    operation wil queue the operation on the instance's 'pipe' object
    and return the pipe. When executed, the pipe will include the
    expected value(s) from Redis in its list of results.
    """
    redisobjs.RedisObject('test', 'list').set(
        ['a', 'c', 'b', 'c'], force_unique=False
    )
    r = redisobjs.RedisObject('test', 'list', defer=True)
    pipe = r.get_index('a', 'b', 'c')
    assert pipe.execute() == [[0, 2, [1, 3]]]


@pytest.mark.parametrize('start, force_unique, index, end, expected', [
    (['a', 'b', 'c'], True, 0, None, 'a'),
    (['a', 'b', 'c'], True, 1, None, 'b'),
    (['a', 'b', 'c'], True, 2, None, 'c'),
    (['a', 'b', 'c'], True, 3, None, None),
    (['a', 'b', 'c'], True, 1, 1, ['b']),
    (['a', 'b', 'c'], True, 1, 2, ['b', 'c']),
    (['a', 'b', 'c'], True, 1, 3, ['b', 'c']),
    (['a', 'b', 'c'], True, 3, 4, None),
    (['a', 'b', 'c'], True, -1, None, 'c'),
    (['a', 'b', 'c'], True, -2, None, 'b'),
    (['a', 'b', 'c'], True, -3, None, 'a'),
    (['a', 'b', 'c'], True, -4, None, None),
    (['a', 'b', 'c'], True, -2, -1, ['b', 'c']),
    (['a', 'b', 'c'], True, 1, -1, ['b', 'c']),
    (['a', 'b', 'c'], True, -5, -4, None),
    (['a', 'b', 'c'], False, 0, None, 'a'),
    (['a', 'b', 'c'], False, 1, None, 'b'),
    (['a', 'b', 'c'], False, 2, None, 'c'),
    (['a', 'b', 'c'], False, 3, None, None),
    (['a', 'b', 'c'], False, 1, 1, ['b']),
    (['a', 'b', 'c'], False, 1, 2, ['b', 'c']),
    (['a', 'b', 'c'], False, 1, 3, ['b', 'c']),
    (['a', 'b', 'c'], False, 3, 4, None),
    (['a', 'b', 'c'], False, -1, None, 'c'),
    (['a', 'b', 'c'], False, -2, None, 'b'),
    (['a', 'b', 'c'], False, -3, None, 'a'),
    (['a', 'b', 'c'], False, -4, None, None),
    (['a', 'b', 'c'], False, -2, -1, ['b', 'c']),
    (['a', 'b', 'c'], False, 1, -1, ['b', 'c']),
    (['a', 'b', 'c'], True, -5, -4, None),
    ('a string', None, 0, None, None),
    ('a string', None, 0, 2, None),
    ({'a': 'hash'}, None, 0, None, None),
    ({'a', 'set'}, None, 0, None, None)
])
def test_redisobject_getvalue(start, force_unique, index, end, expected):
    """
    For a list or zset type object, the RedisObject.get_value method
    returns the value at the provided index. If an 'end' index is
    supplied, it returns a list of values between 'index' and 'end'. If
    'end' is out of range, then it returns values up to the end of the
    data structure. For everything else, or if the starting index is
    out of range, it returns None. Negative index numbers count from
    the end of data structure.
    """
    redisobjs.RedisObject('test', 'list').set(start, force_unique=force_unique)
    result = redisobjs.RedisObject('test', 'list').get_value(index, end)
    assert result == expected


def test_redisobject_getvalue_with_defer():
    """
    When a RedisObject instance has defer set to True, a 'get_value'
    operation wil queue the operation on the instance's 'pipe' object
    and return the pipe. When executed, the pipe will include the
    expected value(s) from Redis in its list of results.
    """
    redisobjs.RedisObject('test', 'list').set(
        ['a', 'c', 'b', 'c'], force_unique=False
    )
    r = redisobjs.RedisObject('test', 'list', defer=True)
    pipe = r.get_value(0, 5)
    assert pipe.execute() == [['a', 'c', 'b', 'c']]


def test_redisobject_one_pipeline_multiple_operations():
    """
    This is just to show that you can use RedisObjects to queue up
    multiple operations on the same pipeline object and then execute
    them all at once.
    """
    conn = redisobjs.REDIS_CONNECTION
    pipe = redisobjs.Pipeline()
    r_header = redisobjs.RedisObject('test', 'header', defer=True, pipe=pipe)
    r_colors = redisobjs.RedisObject('test', 'colors', defer=True, pipe=pipe)
    r_tastes = redisobjs.RedisObject('test', 'tastes', defer=True, pipe=pipe)
    r_feels = redisobjs.RedisObject('test', 'feelings', defer=True, pipe=pipe)
    r_things = redisobjs.RedisObject('test', 'things', defer=True, pipe=pipe)
    assert conn.keys() == []
    r_header.set('Testing Whatever')
    r_colors.set({'r': 'red', 'g': 'green', 'b': 'blue', 'y': 'yellow'})
    r_tastes.set(['bitter', 'sour', 'sweet', 'salty', 'umami'])
    r_feels.set(['happy', 'sad', 'frantic', 'content', 'exciting', 'enui'])
    r_things.set({
        'sea water': {'color': 'b', 'taste': 'salty', 'feeling': 'sad'},
        'peas': {'color': 'g', 'taste': 'sweet', 'feeling': 'content'},
        'lemon': {'color': 'y', 'taste': 'sour', 'feeling': 'frantic'}
    })
    assert conn.keys() == []
    pipe.execute()
    assert set(conn.keys()) == set([
        'test:header', 'test:colors', 'test:tastes', 'test:feelings',
        'test:things'
    ])
    r_tastes.get_index('umami', 'sweet')
    r_feels.get_value(-1)
    r_header.get()
    r_things.get_field('lemon', 'peas')
    r_things.set({
        'lemon': {'color': 'y', 'taste': 'sour', 'feeling': 'happy'}
    }, update=True)
    r_things.get_field('lemon')
    t, f, h, t1, _, t2 = pipe.execute()
    assert t == [4, 2]
    assert f == 'enui'
    assert h == 'Testing Whatever'
    assert t1 == [
        {'color': 'y', 'taste': 'sour', 'feeling': 'frantic'},
        {'color': 'g', 'taste': 'sweet', 'feeling': 'content'}
    ]
    assert t2 == {'color': 'y', 'taste': 'sour', 'feeling': 'happy'}
