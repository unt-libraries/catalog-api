"""
This contains a custom Solr backend for haystack. It fixes a small
issue with the way pysolr converts Solr data to Python, and it helps
consolidate how Solr commits happen. (I.e., it uses the 'commit'
function from utils.solr, which also triggers Solr replication when
manual replication is configured.)
"""
from __future__ import absolute_import

import os
import re
import shlex
import subprocess

from django.apps import apps
from django.conf import settings
from haystack import connections
from haystack.backends import solr_backend, BaseEngine
from haystack.constants import ID, DJANGO_CT, DJANGO_ID
from haystack.models import SearchResult
from haystack.utils import get_model_ct
from pysolr import Solr, SolrError
from six.moves import zip

from utils import solr


class CustomSolr(Solr):
    """
    Custom pysolr.Solr class that patches the _to_python method.
    """

    def _to_python(self, value):
        # The current pysolr (3.9.0) Solr._to_python method only
        # returns the first value in a sequence; we want to return all
        # values.
        if isinstance(value, (list, tuple)):
            return [self._to_python(v) for v in value]
        return super()._to_python(value)


class CustomSolrSearchBackend(solr_backend.SolrSearchBackend):
    """
    Custom haystack SolrSearchBackend class. Does a few things.

    1. Uses the CustomSolr class to replace pysolr.Solr, to fix the
       _to_python method and ensure multi-valued fields are rendered
       correctly.
    2. Adds a 'commit' method that directs to the 'commit' function in
       utils.solr, to ensure manual replication is handled (if it's
       configured).
    3. Overrides methods 'update', 'remove', and 'clear' to intercept
       commits and run them through the new 'commit' method.
    4. Adds a 'delete' method that lets you delete a batch of records
       (by Solr query or by id).
    """

    def __init__(self, connection_alias, **connection_options):
        super().__init__(connection_alias, **connection_options)
        self.conn = CustomSolr(
            connection_options["URL"],
            timeout=self.timeout,
            **connection_options.get("KWARGS", {})
        )

    def update(self, index, iterable, commit=True):
        super().update(index, iterable, commit=False)
        if commit:
            self.commit()

    def remove(self, obj_or_string, commit=True):
        super().remove(obj_or_string, commit=False)
        if commit:
            self.commit()

    def clear(self, models=None, commit=True):
        super().clear(models, commit=False)
        if commit:
            self.commit()

    def delete(self, q=None, ids=None, commit=True):
        self.conn.delete(q=q, id=ids, commit=False)
        if commit:
            self.commit()

    def commit(self):
        solr.commit(self.conn, self.connection_alias)


class CustomSolrEngine(BaseEngine):
    backend = CustomSolrSearchBackend
    query = solr_backend.SolrSearchQuery

