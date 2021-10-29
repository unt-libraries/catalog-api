# -*- coding: utf-8 -*-

"""
Sierra2Marc module for catalog-api `blacklight` app.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import re

try:
    # Python 3
    from re import ASCII
except ImportError:
    # Python 2
    ASCII = 0
from collections import OrderedDict

from django.conf import settings

from blacklight import parsers as p
from utils import toascii

