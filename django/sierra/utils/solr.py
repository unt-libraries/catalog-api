"""
Provides Django queryset functionality on top of Solr search results.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import logging
import re
from datetime import datetime

import pysolr
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from six import iteritems, text_type

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


def connect(url=None, using='default', **kwargs):
    if not url:
        try:
            url = settings.HAYSTACK_CONNECTIONS[using]['URL']
        except KeyError:
            raise ImproperlyConfigured('Haystack connection {} does not '
                                       'exist.'.format(using))
    return pysolr.Solr(url, always_commit=True, **kwargs)


def format_datetime_for_solr(dt_obj):
    """
    Format a Python datetime object (UTC) in Solr datetime format.
    """
    s_time = dt_obj.utctimetuple()
    date_str = '{}-{:02d}-{:02d}'.format(*s_time[0:3])
    time_str = '{:02d}:{:02d}:{:02d}'.format(*s_time[3:6])
    return '{}T{}Z'.format(date_str, time_str)


class MultipleObjectsReturned(Exception):
    pass


class Result(dict):
    """
    Simple Result class that provides Solr fields as object attributes
    but is instantiated using a dict. Can be updated by manipulating
    the object attributes / dict values directly. Can be saved back to
    Solr using save().
    """

    def __init__(self, *args, **kwargs):
        super(Result, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def save(self, url=None, using='default', **kwargs):
        conn = connect(url, using)
        try:
            del(self['_version_'])
        except KeyError:
            pass
        conn.add([self], **kwargs)


class Queryset(object):
    def __init__(self, url=None, using='default', page_by=100, conn=None,
                 **kwargs):
        self._conn = conn or connect(url=url, using=using, **kwargs)
        self._result_set = []
        self._result_offset = 0
        self._hits = None
        self._search_params = {'q': '*:*', 'fq': []}
        self._full_response = None
        self.page_by = page_by
        kwargs['conn'] = self._conn
        kwargs['page_by'] = page_by
        self._kwargs = kwargs

    def __getitem__(self, key):
        if isinstance(key, slice):
            start = key.start
            new_key = key.start - self._result_offset
            rows = key.stop - key.start
            r = self._search(start=start, rows=rows)
            return [Result(i) for i in r]

        if key < 0:
            hits = len(self)
            key = hits + key

        try:
            new_key = key - self._result_offset
            if new_key < 0:
                raise IndexError()
            return self._result_set[new_key]
        except IndexError:
            if self._hits is None or key < self._hits:
                r = self._search(start=key, rows=self.page_by)
                self._set_cache(r, offset=key)
            if key >= self._hits or not self._result_set:
                raise IndexError('index out of range')
            return self.__getitem__(key)

    def __len__(self):
        if self._hits is None:
            self._search(rows=0)
        return self._hits

    def _search(self, *args, **kwargs):
        kwargs = kwargs or {}
        kwargs.update(self._search_params)
        response = self._conn.search(*args, **kwargs)
        self._full_response = response
        self._hits = response.hits
        return response

    def _set_cache(self, result, offset=0):
        self._result_offset = offset
        if result:
            self._result_set = [Result(i) for i in result]
        else:
            self._result_set = []

    def _clone(self):
        clone = Queryset(**self._kwargs)
        clone._search_params = copy.deepcopy(self._search_params)
        clone._set_cache(None)
        clone._full_response = None
        return clone

    def count(self):
        return len(self)

    @property
    def full_response(self):
        if self._full_response is None:
            len(self)
        return self._full_response

    def _add_contains_parameter(self, field, val):
        val = self._conn._from_python(val)
        return u'{}:*{}*'.format(field, val)

    def _add_startswith_parameter(self, field, val):
        val = self._conn._from_python(val)
        return u'{}:{}*'.format(field, val)

    def _add_endswith_parameter(self, field, val):
        val = self._conn._from_python(val)
        return u'{}:*{}'.format(field, val)

    def _add_exact_parameter(self, field, val):
        val = self._conn._from_python(val)
        return u'{}:"{}"'.format(field, val)

    def _add_keywords_parameter(self, field, val):
        val = self._conn._from_python(val)
        return u'{}:({})'.format(field, val)

    def _add_gt_parameter(self, field, val):
        val = self._conn._from_python(val)
        return u'{}:{{"{}" TO *}}'.format(field, val)

    def _add_gte_parameter(self, field, val):
        val = self._conn._from_python(val)
        return u'{}:["{}" TO *]'.format(field, val)

    def _add_lt_parameter(self, field, val):
        val = self._conn._from_python(val)
        return u'{}:{{* TO "{}"}}'.format(field, val)

    def _add_lte_parameter(self, field, val):
        val = self._conn._from_python(val)
        return u'{}:[* TO "{}"]'.format(field, val)

    def _add_in_parameter(self, field, val):
        return u'{}:({})'.format(field, u' OR '.join(['"{}"'.format(
                                 self._conn._from_python(v)) for v in val]))

    def _add_range_parameter(self, field, val):
        return u'{}:["{}" TO "{}"]'.format(field,
                                           self._conn._from_python(val[0]),
                                           self._conn._from_python(val[1]))

    def _add_matches_parameter(self, field, val):
        start, end = ('.*', '.*')

        if re.search(r'^\^', val):
            start = ''
            val = re.sub(r'^\^', r'', val)

        if re.search(r'\$$', val):
            end = ''
            val = re.sub(r'\$$', r'', val)

        return u'{}:/{}{}{}/'.format(field, start, val, end)

    def _add_isnull_parameter(self, field, val):
        if val == 'True':
            ret_val = u'-{}:*'.format(field)
        else:
            ret_val = u'{}:*'.format(field)
        return ret_val

    def _val_to_solr_str(self, val):
        if isinstance(val, datetime):
            return format_datetime_for_solr(val)
        return re.sub(r'([ +\-!(){}\[\]\^"~*?:\\/]|&&|\|\|)', r'\\\1',
                      text_type(val))

    def _compile_filter_args(self, filter_args):
        fq = []
        for key, val in iteritems(filter_args):
            try:
                field, filter_type = key.split('__')
            except ValueError:
                field, filter_type = key, 'exact'
            f_method = getattr(self, '_add_{}_parameter'.format(filter_type))
            if filter_type not in ('matches', 'keywords'):
                if isinstance(val, (list, tuple)):
                    val = [self._val_to_solr_str(v) for v in val]
                else:
                    val = self._val_to_solr_str(val)
            fq.append(f_method(field, val))
        return fq

    def _apply_filters(self, filter_args, exclude=False):
        fq = self._compile_filter_args(filter_args)
        if exclude:
            fq = [q[1:] if (q[0] == '-') else ('-' + q) for q in fq]
        clone = self._clone()
        clone._search_params['fq'].extend(fq)
        return clone

    def filter(self, **kwargs):
        return self._apply_filters(kwargs, exclude=False)

    def exclude(self, **kwargs):
        return self._apply_filters(kwargs, exclude=True)

    def get_one(self, **kwargs):
        """
        Like filter, but fetches and returns a single result based on
        the supplied kwargs search parameters. Note that, like filter
        and exclude, it will apply any search parameters already set on
        this Queryset object first before applying additional
        parameters specified via this method.

        Raises a MultipleObjectsReturned exception if the filter
        returns multiple objects.
        """
        result = self.filter(**kwargs)
        try:
            ret_value = result[0]
        except IndexError:
            ret_value = None
        else:
            if len(result) > 1:
                msg = ('Multiple objects returned for query {} '
                       ''.format(result._search_params))
                raise MultipleObjectsReturned(msg)
        return ret_value

    def set_raw_params(self, params):
        """
        Set any raw `params` to send to Solr, overriding existing ones.

        `params` should be a dict of [additional] keyword parameters to
        send to Solr next time a search is triggered. Query params set
        via previous methods (such as filter or order by) are kept and
        are only overridden if you explicitly override the applicable
        params (like `q`, `fq`, `sort`, etc.). This lets you set things
        like facet parameters in conjunction with a filtered queryset
        that are otherwise not supported directly by other methods.
        """
        clone = self._clone()
        clone._search_params.update(params)
        return clone

    def search(self, raw_query, params=None):
        q = self._search_params.get('q', '*')

        if q not in ('*', '*:*'):
            q = '({}) AND ({})'.format(q, raw_query)
        else:
            q = raw_query

        params = params or {}
        params['q'] = q
        return self.set_raw_params(params)

    def order_by(self, *fields):
        clone = self._clone()
        sort = []
        for field in fields:
            direction = 'asc'
            if field.startswith('-'):
                field = field[1:]
                direction = 'desc'
            sort.append('{} {}'.format(field, direction))
        sort = ', '.join(sort)
        clone._search_params['sort'] = sort
        return clone

    def only(self, *fields):
        clone = self._clone()
        clone._search_params['fl'] = fields
        return clone
