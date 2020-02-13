"""
`ruleset` module for catalog-api `base` app.

This provides classes to help implement logic for local business rules
based on Sierra-specific codes and behavior not easily extracted from
the Sierra DB. For things like: Collections (based on scope rules,
which aren't held in the Sierra DB); request rules (also not held in
the Sierra DB); hierarchy for location codes; etc.

I've put implementations of these classes in `local_rulesets.py`; if
you are implementing this at your institution, you will want to
override that locally.
"""

from __future__ import unicode_literals
import re

from utils import helpers as h


class Ruleset(object):
    """
    Map one or more of an object's attribute values to an output value.

    Initialize a Ruleset by providing a list of `rules` and an optional
    `default` value. Each rule is a 2-length tuple, where the first
    item is an attribute name or tuple of attribute names, and the
    second item is an object that maps the values from the object to
    result values. The mapping object may or may not be a dict; it only
    must implement a dict-like `get` method to be used in a Ruleset.

    Trigger the Ruleset by calling the `evaluate` method, providing an
    object instance of the intended type (`obj`). It evaluates rules in
    order and does NOT stop when a matching rule is found. Later rules
    acting on the same values for the same attributes will override
    earlier ones. This allows you to place more general rules first and
    more specific rules (i.e. overrides) later.

    An example ruleset could look like this:

    >>> requestable_ruleset = Ruleset([
    >>>     ('location_id', {'czm': False, 'law': False, 'w': False}),
    >>>     ('itype_id', {7: False, 8: False, 22: False}),
    >>>     (('itype_id', 'location_id'), {(7, 'xmus'): True})
    >>> ], default=True)

    An object with `location_id` 'czm', 'law', or 'w' would return
    False. An object with `itype_id` 7 would return False unless it
    also had `location_id` 'xmus', in which case it would return True.
    Objects with `itype_id` 8 or 22 would return False. Everything else
    would return the default value of True.
    """
    def __init__(self, rules, default=None):
        self.rules = rules
        self.default = default

    def evaluate(self, obj):
        result = self.default
        for fields, mapping in self.rules:
            if isinstance(fields, (tuple, list)):
                val_from_obj = tuple(getattr(obj, f, None) for f in fields)
            else:
                val_from_obj = getattr(obj, fields, None)
            result = mapping.get(val_from_obj, result)
        return result


def reverse_mapping(forward_mapping, multi=True):
    """
    Generate the reverse of the provided `forward_mapping`.
    This is a utility function for helping define mappings to use in
    Rulesets to make them more concise.

    Values in `forward_mapping` can be any iterable: sets, lists,
    tuples, etc. During the reversal they will be converted to dict key
    values (and will thus be made unique).

    Use `multi` to control whether or not you want values in the return
    dict to be sets (if True) or not (if False); if True, then a value
    from the original dict that appears in multiple entries will have
    all entries preserved.

    For example:

    >>> forward_mapping = {
    >>>     'Collection1': set(['code1', 'code2']),
    >>>     'Collection2': set(['code2', 'code3'])
    >>> }
    >>> reverse_mapping(forward_mapping, multi=True)
    {
        'code1': set(['Collection1']),
        'code2': set(['Collection1', 'Collection2']),
        'code3': set(['Collection2']),
    }
    >>> reverse_mapping(forward_mapping, multi=False)
    {
        'code1': 'Collection1',
        'code2': 'Collection2',
        'code3': 'Collection2'
    }

    Be careful when using multi=False, if you have overlapping values
    (such as 'code2' in the above example). There's no way to gaurantee
    the order `forward_mapping` will be evaluated in, so it could get
    either value. You can use an OrderedDict to work around this.
    """
    reverse_mapping = {}
    for key, vals in forward_mapping.items():
        for val in list(vals):
            if multi:
                reverse_mapping[val] = reverse_mapping.get(val, set())
                reverse_mapping[val].add(key)
            else:
                reverse_mapping[val] = key
    return reverse_mapping


class StrPatternMap(object):
    """
    Map strings (i.e., codes) to labels based on regex patterns.
    StrPatternMap objects use a similar `get` method interface used by
    dicts; however they default to returning 'None' rather than raising
    a KeyError for non-matching values.

    This is a utility class intended for use as an alternative-style
    mapping (rather than a dict) in Rulesets. Essentially, instead of
    mapping strings to strings, you're mapping string patterns to
    strings (or any other value).

    For example:

    >>> pmap = StrPatternMap({
    >>>     r'^w': 'Willis Library',
    >>>     r'^s': 'Eagle Commons Library'}, exclude=['wx'])
    >>> pmap.get('w3')
    'Willis Library'

    >>> pmap.get('sdus')
    'Eagle Commons Library'

    >>> pmap.get('wx')
    None

    >>> pmap.get('abcd')
    None

    >>> pmap.get('wx', 'Error')
    'Error'
    """
    def __init__(self, patterns, exclude=None):
        self.patterns = patterns or {}
        self.exclude = tuple() if exclude is None else tuple(exclude)

    def get(self, code, default=None):
        """
        Map the given string (`code`) to the appropriate value based on
        the initialized pattern settings.
        """
        if code not in self.exclude:
            for pattern, val in self.patterns.items():
                if bool(re.search(pattern, code)):
                    return val
        return default
