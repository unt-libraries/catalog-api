from __future__ import absolute_import

import logging
import re

from django.conf import settings
from pysolr import SolrError
from rest_framework.filters import BaseFilterBackend

from . import exceptions
from .simpleserializers import SimpleField

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class FilterValidationError(Exception):
    pass


class SimpleQSetFilterBackend(BaseFilterBackend):
    """
    Filter backend for filtering QuerySets.
    """
    paginate_by_param = settings.REST_FRAMEWORK['PAGINATE_BY_PARAM']
    paginate_param = settings.REST_FRAMEWORK['PAGINATE_PARAM']
    order_by_param = settings.REST_FRAMEWORK['ORDER_BY_PARAM']
    search_param = settings.REST_FRAMEWORK['SEARCH_PARAM']
    searchtype_param = settings.REST_FRAMEWORK['SEARCHTYPE_PARAM']
    reserved_params = [paginate_by_param, paginate_param, order_by_param,
                       search_param, searchtype_param, 'format']
    valid_operators = ['exact', 'contains', 'in', 'gt', 'gte', 'lt', 'lte',
                       'startswith', 'endswith', 'range', 'matches', 'isnull',
                       'keywords']

    # searchtypes contains Solr search parameters for various types of
    # searches you want to allow to be performed from the API filters.
    searchtypes = {}
    default_searchtype = None

    def parse_array_vals(self, val_string):
        """
        Parse a string of array values into a Py list.

        Arrays are comma-delimited lists of values, where double-quotes
        MAY be used to wrap a value that includes a comma, and a
        backslash character preceding either a double-quote or a comma
        escapes it. Thus: "a,b",c,d and a\\,b,c,d both become
        ['a,b', 'c', 'd'].
        """
        vals, this_val, in_quotes, escape = [], [], False, False
        for ch in val_string:
            if ch == '\\':
                escape = True
            else:
                if ch == ',' and not escape and not in_quotes:
                    vals.append(''.join(this_val))
                    this_val = []
                elif ch == '"' and not escape:
                    in_quotes = not in_quotes
                else:
                    this_val.append(ch)
                escape = False
        if this_val:
            vals.append(''.join(this_val))
        return vals

    def parse_filter_param_name(self, client_pname):
        """
        Parse a client filter param name into field, op, and negate.
        
        A client parameter for a filter may look like:
            field
            field[operator]
            field[-operator]
        
        The first implies an exact-match operator; the third applies
        negation (not). This method parses the client param name and
        return a tuple: (fieldname, operator, negate). `negate` is a
        boolean value.
        """
        negate = False
        fieldname, _, op = client_pname.partition('[')
        op = op.rstrip(']') or 'exact'
        if op.startswith('-'):
            op = op.lstrip('-')
            negate = True
        return fieldname, op, negate

    def field_is_not_filterable(self, field, view):
        """
        True if the given field is not filterable, given the view.

        Normally filterability is set on the serializer field object,
        but a view may override that to disable it.
        """
        disabled_filters = getattr(view, 'disabled_filters', set())
        return (field.name in disabled_filters) or not field.filterable

    def field_is_not_orderable(self, field, view):
        """
        True if the given field is not orderable, given the view.

        Normally orderability is set on the serializer field object,
        but a view may override that to disable it.
        """
        disabled_orderby = getattr(view, 'disabled_orderby', set())
        return (field.name in disabled_orderby) or not field.orderable

    def prep_filter_val_for_op(self, op, client_pval):
        """
        Prep and validate a filter value for a given operator.

        Currently the only thing to do here is to parse the client
        value for the `in` and `range` operators as a list/array. Note
        that the client may (or may not) enclose the value in square
        brackets; if present they are stripped. Either way, the client
        value for these operators is interpreted as a comma-delimited
        list.
        """
        if op in ('in', 'range'):
            if client_pval.startswith('[') and client_pval.endswith(']'):
                client_pval = client_pval[1:-1]
            return self.parse_array_vals(client_pval)
        return client_pval

    def apply_filter(self, qset, client_pname, client_pval, view, serializer):
        """
        Filter the qset, given a client param/val & view/serializer.

        This uses the provided args to apply a Django-queryset-style
        filter to the givent queryset. All appropriate validation and
        preparation are applied to the value first.
        """
        fname, op, negate = self.parse_filter_param_name(client_pname)
        field = serializer.field_lookup.get(fname)
        if field is None:
            msg = "'{}' is not a valid parameter.".format(fname)
            raise FilterValidationError(msg)

        if self.field_is_not_filterable(field, view):
            msg = ("Field '{}' cannot be used for filtering this "
                   "resource.".format(fname))
            raise FilterValidationError(msg)

        if op not in self.valid_operators:
            valid_ops = ', '.join(self.valid_operators)
            msg = ("'{}' is not a valid operator. Valid operators "
                   "include: {}.".format(op, valid_ops))
            raise FilterValidationError(msg)

        validation_errors = []        
        for val in client_pval:
            try:
                val = self.prep_filter_val_for_op(op, val)
            except FilterValidationError as e:
                validation_errors.append(str(e))
            try:
                qset = serializer.do_apply_field_filter_to_qset(
                    field, val, op, negate, qset
                )
            except field.ValidationError as e:
                validation_errors.append(str(e))
        if len(validation_errors) > 0:
            msg = "Field '{}': {}".format(fname, ' '.join(validation_errors))
            raise FilterValidationError(msg)
        return qset

    def apply_orderby(self, qset, fieldnames, view, serializer):
        """
        Order the qset, given a list of fieldnames & view/serializer.

        This uses the provided args to apply a Django-queryset-style
        `order_by` to the given queryset.
        """
        validation_errors = []
        criteria = []
        for fname in fieldnames:
            desc = False
            if fname.startswith('-'):
                desc, fname = True, fname[1:]
            field = serializer.field_lookup.get(fname)
            if field is None:
                msg = ("Cannot order by field '{}': it is not a field on this "
                       "resource.".format(fname))
                raise FilterValidationError(msg)
            if self.field_is_not_orderable(field, view):
                msg = ("Field '{}' cannot be used for ordering this "
                       "resource.".format(fname))
                validation_errors.append(msg)
            try:
                criteria.extend(
                    serializer.do_emit_field_orderby_criteria(field, desc)
                )
            except field.ValidationError as e:
                msg = 'Field `{}`: {}'.format(fname, str(e))
                validation_errors.append(msg)
        if len(validation_errors) > 0:
            raise FilterValidationError(' '.join(validation_errors))
        return qset.order_by(*criteria)

    def apply_search(self, qset, client_pval, searchtype):
        """
        Apply a search filter to the given qset.
        """
        q_settings = self.searchtypes.get(searchtype)
        if searchtype and q_settings is None:
            valid_stypes = ', '.join(list(self.searchtypes.keys()))
            msg = ("The 'searchtype' parameter must be one of the "
                   "following values: {}".format(valid_stypes))
            raise FilterValidationError(msg)
        return qset.search(client_pval[0], params=q_settings)

    def filter_queryset(self, request, qset, view):
        """
        Apply appropriate filtering to the qset for the given request.
        """
        # Casting the request.query_params QueryDict to a plain dict
        # forces each value to a list. I.e.:
        #   - /?test=foo => {'test': ['foo']}
        #   - /?test=foo&test=bar => {'test': ['foo', 'bar']}
        req_params = dict(request.query_params)
        validation_errors = []
        ser = view.get_serializer()
        for param, pval in req_params.items():
            if param not in self.reserved_params:
                try:
                    qset = self.apply_filter(qset, param, pval, view, ser)
                except FilterValidationError as e:
                    validation_errors.append(str(e))
            elif param == self.search_param:
                default = self.default_searchtype
                searchtype = req_params.get(self.searchtype_param, [default])[0]
                try:
                    qset = self.apply_search(qset, pval, searchtype)
                except FilterValidationError as e:
                    validation_errors.append(str(e))
            elif param == self.order_by_param:
                fnames = [fn for pstr in pval for fn in pstr.split(',')]
                try:
                    qset = self.apply_orderby(qset, fnames, view, ser)
                except FilterValidationError as e:
                    validation_errors.append(str(e))

        if len(validation_errors) > 0:
            err_strs = ['<<<{}>>>'.format(err) for err in validation_errors]
            msg = ('Query filter criteria specified for this resource is '
                   'invalid. The following errors were raised. ... {}'
                   ''.format(' ... '.join(err_strs)))
            raise exceptions.BadQuery(detail=msg)

        try:
            view.paginate_queryset(qset, request)
        except SolrError as e:
            msg = ('Query raised Solr error. {}'.format(e))
            raise exceptions.BadQuery(detail=msg)
        return qset


class EResourcesFilter(SimpleQSetFilterBackend):
    searchtypes = {
        'databases': {
            'defType': 'edismax',
            'qf': 'title^10 alternate_titles^10 subjects^5 summary^2 '
                  'holdings^2',
            'pf': 'title^10 alternate_titles^10 subjects^5 summary^5 '
                  'holdings^5',
            'pf2': 'title^10 alternate_titles^10 subjects^5 summary^5 '
                   'holdings^5',
            'qs': '2',
            'ps': '2',
            'ps2': '1',
            'mm': '2<80%',
            'stopwords': 'true',
            'synonyms': 'true'
        }
    }
    default_searchtype = 'databases'

