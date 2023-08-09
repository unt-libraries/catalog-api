from __future__ import absolute_import

import importlib
from collections.abc import Mapping, Sequence

from six import iteritems


def load_class(class_str):
    '''
    Dynamically loads a class from a full class path specified in a
    string.
    '''
    class_data = class_str.split('.')
    module_path = '.'.join(class_data[:-1])
    return getattr(importlib.import_module(module_path), class_data[-1])


def dict_merge(orig_d, updt_d, list_merge=True):
    '''
    Performs a recursive update on nested dictionaries of arbitrary
    depths so that all levels are combined instead of overwritten.
    If list_append is True, then lists at the deepest level are merged,
    otherwise they're overwritten. Example, with list_merge = True:
        orig_d = {'outer_key': {'inner_key': {'a': [0], 'b': 1 }}}
        updt_d = {'outer_key': {'inner_key': {'a': [1] }}}
        result = {'outer_key': {'inner_key': {'a': [0, 1], 'b': 1 }}}
    '''
    for k, v in iteritems(updt_d):
        if isinstance(v, Mapping):
            temp_d = dict_merge(orig_d.get(k, {}), v, list_merge)
            orig_d[k] = temp_d
        else:
            if isinstance(v, Sequence) and list_merge:
                orig_d[k] = orig_d.get(k, [])
                orig_d[k].extend(v)
            else:
                orig_d[k] = v

    return orig_d
