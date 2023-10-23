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
    if isinstance(value, tuple):
        assert r.get() == list(value)
    else:
        assert r.get() == value


@pytest.mark.parametrize('old_val, new_val', [
    (None, 'test val'),
    ('test val', None),
    ('test val', ''),
    ('test val', [1, 2, 3, 4, 5]),
    ([1, 2, 3, 4, 5], 'test val'),
    ([1, 2, 3, 4, 5], [5, 4, 3, 2, 1]),
    ({1, 2, 3}, [1, 2, 3]),
    ({'test1': [1, 2, 3], 'test2': [4, 5, 6]}, {'test3': [7, 8, 9]})
])
def test_redisobject_set_overwrites_existing_key(old_val, new_val):
    """
    The RedisObject.set method should silently and automatically
    overwrite the existing key, whatever it may be.
    """
    redisobjs.RedisObject('test_table', 'test_item').set(old_val)
    redisobjs.RedisObject('test_table', 'test_item').set(new_val)
    assert redisobjs.RedisObject('test_table', 'test_item').get() == new_val


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
    When RedisObject.set is used to save a list, the `force_unique`
    option determines whether Redis stores only unique values or not.
    """
    r_set = redisobjs.RedisObject('test_table', 'test_item')
    r_set.set(value, force_unique=force_unique)
    r_get = redisobjs.RedisObject('test_table', 'test_item')
    assert r_get.get() == exp


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
    assert pipe.execute() == [[0, True], [1, True]]
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
    assert pipe.execute() == [[0, True], [0, True]]
    assert redisobjs.RedisObject('test', 'item').get() == 'my_value'
    assert redisobjs.RedisObject('test', 'item2').get() == 'other_value' 


def test_redisobject_get_nonexistent_key_returns_none():
    """
    For ease of use, trying to get a key that doesn't exist returns
    None. This behavior is patterned after the dict `get` method.
    """
    r = redisobjs.RedisObject('not set', 'not set')
    assert r.conn.keys() == []
    assert r.get() == None


@pytest.mark.parametrize('init, mapping, expected', [
    ({'a': 'z'}, {'b': 'y'}, {'a': 'z', 'b': 'y'}),
    ({'a': 'z'}, {'b': 'y', 'c': 'x'}, {'a': 'z', 'b': 'y', 'c': 'x'}),
    ({'a': 'z', 'b': 'y'}, {'a': 1, 'b': 2}, {'a': 1, 'b': 2}),
    ({'a': 'z'}, {'a': 1, 'b': 2}, {'a': 1, 'b': 2}),
    ({'a': 'z'}, {'b': {'test': 'value'}}, {'a': 'z', 'b': {'test': 'value'}}),
    ({'a': 'z', 'b': 'y'}, {'a': [1, 2, 3]}, {'a': [1, 2, 3], 'b': 'y'})
])
def test_redisobject_setfield_sets_values(init, mapping, expected):
    """
    The RedisObject.set_field method sets one or more fields on an
    existing hash using the given mapping. It returns the mapping.
    Getting the full object produces the expected dict.
    """
    redisobjs.RedisObject('test', 'hash').set(init)
    assert redisobjs.RedisObject('test', 'hash').set_field(mapping) == mapping
    assert redisobjs.RedisObject('test', 'hash').get() == expected


@pytest.mark.parametrize('value, force_unique', [
    ('test', None),
    ([1, 2, 3], False),
    ([1, 2, 3], True),
    ({1, 2, 3}, None)
])
def test_redisobject_setfield_type_errors(value, force_unique):
    """
    The RedisObject.set_field method requires a Redis 'hash' type,
    otherwise it raises a TypeError.
    """
    r = redisobjs.RedisObject('test', 'hash-error')
    r.set(value, force_unique=force_unique)
    with pytest.raises(TypeError):
        redisobjs.RedisObject('test', 'hash-error').set_field({'a': 'z'})


@pytest.mark.parametrize('init, mapping, exp_return, exp_dict', [
    ({'a': 'z'}, {'b': 'y'}, [1], {'a': 'z', 'b': 'y'}),
    ({'a': 'z'}, {'b': 'y', 'c': 'x'}, [2], {'a': 'z', 'b': 'y', 'c': 'x'}),
])
def test_redisobject_setfield_with_defer(init, mapping, exp_return, exp_dict):
    """
    When a RedisObject instance has defer set to True, a 'set_field'
    operation wil queue the operation on the instance's 'pipe' object
    and return the pipe. When executed, the pipe will return the
    expected value(s) from Redis. Getting the full object will produce
    the expected dict, but only after the pipeline is executed.
    """
    r_norm = redisobjs.RedisObject('test', 'hash-defer')
    r_norm.set(init)
    r_defer = redisobjs.RedisObject('test', 'hash-defer', defer=True)
    pipe = r_defer.set_field(mapping)
    assert pipe == r_defer.pipe
    assert r_norm.get() == init
    assert pipe.execute() == exp_return
    assert r_norm.get() == exp_dict


@pytest.mark.parametrize('init, force_unique, index, val, expected', [
    (['a', 'b'], True, 0, 'c', ['c', 'b']),
    (['a', 'b'], True, 1, 'c', ['a', 'c']),
    (['a', 'b'], False, 0, 'c', ['c', 'b']),
    (['a', 'b'], False, 1, 'c', ['a', 'c']),

    # If the start index is out of range, it pushes values onto the end
    # of the list.
    (['a', 'b'], True, 2, 'c', ['a', 'b', 'c']),
    (['a', 'b'], True, 10, 'c', ['a', 'b', 'c']),
    (['a', 'b'], False, 2, 'c', ['a', 'b', 'c']),
    (['a', 'b'], False, 10, 'c', ['a', 'b', 'c']),

    # If a negative index is used, it replaces values starting at the
    # end of the list.
    (['a', 'b'], True, -1, 'c', ['a', 'c']),
    (['a', 'b'], True, -2, 'c', ['c', 'b']),
    (['a', 'b'], False, -1, 'c', ['a', 'c']),
    (['a', 'b'], False, -2, 'c', ['c', 'b']),
])
def test_redisobject_setvalue_sets_one_value(init, force_unique, index, val,
                                             expected):
    """
    The RedisObject.set_value method sets a value at the given index
    position for an existing list or zset in Redis and returns the
    value. Getting the full object produces the expected list.
    """
    redisobjs.RedisObject('test', 'item').set(init, force_unique=force_unique)
    assert redisobjs.RedisObject('test', 'item').set_value(index, val) == val
    assert redisobjs.RedisObject('test', 'item').get() == expected


@pytest.mark.parametrize('init, force_unique, i, vals, expected', [
    (['a', 'b'], True, 0, ('c', 'b', 'd'), ['c', 'b', 'd']),
    (['a', 'b'], True, 1, (['b1', 'b2'], ['c1']), ['a', ['b1', 'b2'], ['c1']]),
    (['a', 'b', 'c'], True, 1, ('1', '2'), ['a', '1', '2']),
    (['a', 'b'], False, 0, ('c', 'b', 'd',), ['c', 'b', 'd']),
    (['a', 'b'], False, 1, ({'b': 'z'}, 'c'), ['a', {'b': 'z'}, 'c']),
    (['a', 'b', 'c'], False, 1, ('1', '2'), ['a', '1', '2']),

    # If the start index is out of range, it pushes values onto the end
    # of the list.
    (['a', 'b'], True, 2, ('c', 'd'), ['a', 'b', 'c', 'd']),
    (['a', 'b'], True, 10, ('c', 'd'), ['a', 'b', 'c', 'd']),
    (['a', 'b'], False, 2, ('c', 'd'), ['a', 'b', 'c', 'd']),
    (['a', 'b'], False, 10, ('c', 'd'), ['a', 'b', 'c', 'd']),

    # If a negative index is used, it replaces values starting at the
    # end of the list.
    (['a', 'b'], True, -1, ('c', 'd'), ['a', 'c', 'd']),
    (['a', 'b', 'c'], True, -2, ('1', '2'), ['a', '1', '2']),
    (['a', 'b'], False, -1, ('c', 'd'), ['a', 'c', 'd']),
    (['a', 'b', 'c'], False, -2, ('1', '2'), ['a', '1', '2']),
])
def test_redisobject_setvalue_sets_multi_values(init, force_unique, i, vals,
                                                expected):
    """
    When multiple values are provided, the RedisObject.set_value method
    sets a range of values starting at the given index position for an
    existing list or zset in Redis and returns the values. Getting the
    full object produces the expected list.
    """
    redisobjs.RedisObject('test', 'item').set(init, force_unique=force_unique)
    assert redisobjs.RedisObject('test', 'item').set_value(i, *vals) == vals
    assert redisobjs.RedisObject('test', 'item').get() == expected


@pytest.mark.parametrize('value', [
    'test',
    {1, 2, 3},
    {'a': 'z', 'b': 'y'}
])
def test_redisobject_setvalue_type_errors(value):
    """
    The RedisObject.set_value method requires a list or zset type in
    Redis, otherwise it raises a TypeError.
    """
    redisobjs.RedisObject('test', 'list-error').set(value)
    with pytest.raises(TypeError):
        redisobjs.RedisObject('test', 'list-error').set_value(0, ['a', 'b'])


@pytest.mark.parametrize('init, f_unq, i, vals, exp_return, exp_list', [
    (['a', 'b'], True, 0, ('c',), [[1, 1]], ['c', 'b']),
    (['a', 'b'], True, 0, ('c', 'b', 'd'), [[2, 3]], ['c', 'b', 'd']),
    (['a', 'b'], False, 0, ('c',), [1], ['c', 'b']),
    (['a', 'b'], False, 0, ('c', 'b', 'd'), [[True, True, 3]], ['c', 'b', 'd']),
])
def test_redisobject_setvalue_with_defer(init, f_unq, i, vals, exp_return,
                                         exp_list):
    """
    When a RedisObject instance has defer set to True, a 'set_value'
    operation wil queue the operation on the instance's 'pipe' object
    and return the pipe. When executed, the pipe will return the
    expected value(s) from Redis. Getting the full object will produce
    the expected list, but only after the pipeline is executed.
    """
    r_norm = redisobjs.RedisObject('test', 'list-zset-defer')
    r_norm.set(init, force_unique=f_unq)
    r_defer = redisobjs.RedisObject('test', 'list-zset-defer', defer=True)
    pipe = r_defer.set_value(i, *vals)
    assert pipe == r_defer.pipe
    assert r_norm.get() == init
    assert pipe.execute() == exp_return
    assert r_norm.get() == exp_list


@pytest.mark.parametrize('init, field, expected', [
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
    (['a', 'b', 'c'], True, 'a', 0),
    (['a', 'b', 'c'], True, 'b', 1),
    (['a', 'b', 'c'], True, 'c', 2),
    (['a', 'b', 'c'], True, 'd', None),
    (['a', 'b', 'c'], True, ('a', 'b'), [0, 1]),
    (['a', 'b', 'c'], True, ('d', 'e'), [None, None]),
    (['a', 'b', 'c'], True, ('a', 'd'), [0, None]),
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
    r_things.set_field({
        'lemon': {'color': 'y', 'taste': 'sour', 'feeling': 'happy'}
    })
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
