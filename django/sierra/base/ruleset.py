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
