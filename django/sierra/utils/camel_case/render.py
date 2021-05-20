# -*- coding: utf-8 -*-
from __future__ import absolute_import
from collections import OrderedDict
from rest_framework.renderers import JSONRenderer
import six
from six.moves import map
from six.moves import range

def underscoreToCamel(s):
  parts = s.split('_')
  return ''.join([parts[i].capitalize() if i !=0 else parts[i] for i in range(0, len(parts))])

def camelize(data):
    if isinstance(data, dict):
        new_dict = OrderedDict()
        for key, value in six.iteritems(data):
            new_dict[underscoreToCamel(key)] = camelize(value)
        return new_dict
    if isinstance(data, (list, tuple)):
        data = list(map(camelize, data))
        return data
    return data

class CamelCaseJSONRenderer(JSONRenderer):

    def render(self, data, *args, **kwargs):
        return super(CamelCaseJSONRenderer, self).render(camelize(data), *args, **kwargs)