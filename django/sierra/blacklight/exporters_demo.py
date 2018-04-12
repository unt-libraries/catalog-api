"""
Exporters module for catalog-api `blacklight` app, dev01 version.
"""

from __future__ import unicode_literals
import logging
import subprocess
import os
import re
import shlex

import pysolr

from django.conf import settings

from .exporters import BaseSolrMarcBibsToSolr


class BibsToBlacklightDemo(BaseSolrMarcBibsToSolr):
    
    cores = {'bibs': 'bl-demo'}
