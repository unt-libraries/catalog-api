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

    Trigger the Ruleset by calling the `apply` method, providing an
    object instance of the intended type (`obj`). Rules are read in
    order and do NOT stop when a matching rule is found. Later rules
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

    def apply(self, obj):
        result = self.default
        for fields, mapping in self.rules:
            if isinstance(fields, (tuple, list)):
                val_from_obj = tuple(getattr(obj, f, None) for f in fields)
            else:
                val_from_obj = getattr(obj, fields, None)
            result = mapping.get(val_from_obj, result)
        return result


class RulesetCollection(object):
    """
    Use multiple Rulesets to generate a dict describing an object.

    Initialize a RulesetCollection by providing a `labeled_rulesets`
    dict; values are Ruleset objects, and keys label what aspect of an
    object each Ruleset describes.

    Use method `apply_rules` to run the `Ruleset.apply` method on the
    provided object (`obj`) for all rules in `labeled_rulesets`,
    generating a dict matching keys to the values returned by applying
    each Ruleset.

    For example:

    >>> item_rules = RulesetCollection({
    >>>     'is_requestable': Ruleset([
    >>>         ('location_id', {'czm': False, 'w': False}),
    >>>         ('itype_id', {7: False, 8: False, 22: False}),
    >>>         (('itype_id', 'location_id'), {(7, 'xmus'): True})
    >>>     ], default=True),
    >>>     'building_location': Ruleset([
    >>>         ('location_id', {'czmrf': 'Chilton Hall',
    >>>                          'czm': 'Chilton Hall'})
    >>>     ], default='Willis Library')
    >>> })

    >>> test_item.location_id = 'czmrf'
    >>> test_item.itype_id = 2
    >>> item_rules.apply_rules(test_item)
    { 'is_requestable': True, 'building_location': 'Chilton Hall' }
    """
    def __init__(self, labeled_rulesets):
        self.labeled_rulesets = labeled_rulesets

    def apply_rules(self, obj):
        obj_facts = {}
        for label, ruleset in self.labeled_rulesets.items():
            obj_facts[label] = ruleset.apply(obj)
        return obj_facts
