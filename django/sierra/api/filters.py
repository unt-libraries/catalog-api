import re
import logging
from datetime import datetime

import ujson
from dateutil import parser as dateparser
from pysolr import SolrError

from django.conf import settings

from rest_framework.filters import BaseFilterBackend
from haystack.query import SearchQuerySet

from . import exceptions
from utils import helpers

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class FilterValidationError(Exception):
    pass


class HaystackFilter(BaseFilterBackend):
    '''
    Filter to let us filter Haystack SearchQuerySets.
    '''
    reserved_params = [settings.REST_FRAMEWORK['PAGINATE_BY_PARAM'], 
                       settings.REST_FRAMEWORK['PAGINATE_PARAM'],
                       settings.REST_FRAMEWORK['ORDER_BY_PARAM'], 
                       settings.REST_FRAMEWORK['SEARCH_PARAM'], 
                       settings.REST_FRAMEWORK['SEARCHTYPE_PARAM'], 'format']
    valid_operators = ['exact', 'contains', 'in', 'gt', 'gte', 'lt', 'lte',
                       'startswith', 'endswith', 'range', 'matches', 'isnull',
                       'keywords']
    order_field_mapping = {'call_number': 'call_number_sort',
                           'volume': 'volume_sort'}
    # searchtypes contains Solr search parameters for various types of
    # searches you want to allow to be performed from the API filters.
    searchtypes = {
        'journals': {
            'defType': 'synonym_edismax',
            'qf': 'full_title^10 alternate_titles^10 full_subjects^5 '
                  'related_titles^5 creator^1 contributors^1 '
                  'series_creators^1 publishers^0.5 toc_notes^0.5 '
                  'context_notes^0.5 summary_notes^0.5',
            'pf': 'full_title^10 alternate_titles^10 full_subjects^5 '
                  'related_titles^3 toc_notes^2 context_notes^2 '
                  'summary_notes^2',
            'pf2': 'full_title^10 alternate_titles^10 full_subjects^5 '
                   'related_titles^3 toc_notes^2 context_notes^2 '
                   'summary_notes^2',
            'qs': '2',
            'ps': '2',
            'ps2': '1',
            'stopwords': 'true',
            'synonyms': 'true'
        },
        'databases': {
            'defType': 'synonym_edismax',
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

    def _validate_parameter(self, orig_p_name, p_name, operator, p_val, view):
        '''
        Runs validation and normalization routines and returns the
        parameter name (p_name), operator (op), and parameter value
        (p_val) to use in the final query sent to Solr. orig_p_name is
        the original parameter name sent by the client, including the
        operator, as in title[contains]. p_name is just the parameter
        name, minus the operator, and operator is just the operator,
        without the brackets. p_val is the parameter value and view is
        the object.

        You can subclass HaystackFilter and create your own validation
        and normalization routines using the patterns:
        _type_normalize_datetime, replace datetime with a custom type
        _normalize_callnumber, replace callnumber with a field
        These routines return the parameter name, operator, and
        parameter value. So they each have the opportunity to modify
        each of these, if you need that level of customization.
        '''
        try:
            filters = view.filter_fields
        except AttributeError:
            filters = []
        for f in filters:
            if (p_name == f or (re.match(r'^\/.*\/$', f)
                and re.match(re.sub(r'^\/(.*)\/$', r'\1', f), p_name))):
                break
        else:
            raise FilterValidationError('\'{}\' is not a valid field for '
                                        'filtering.'.format(orig_p_name))
        if operator not in self.valid_operators:
            raise FilterValidationError('\'{}\' is not a valid operator. '
                                        'Valid operators include: {}.'
                                        ''.format(operator,
                                            ', '.join(self.valid_operators)))
        list_m = re.match(r'\[(.+)\]$', p_val)
        if list_m and operator != 'matches':
            if operator not in ('in', 'range'):
                raise FilterValidationError('Arrays of values are only used '
                                            'with the \'in\' and \'range\' '
                                            'operators.')
            else:
                p_val = list_m.group(1).split(',')
        elif operator in ('in', 'range'):
            raise FilterValidationError('The \'in\' and \'range\' operators '
                                        'require an array of values. Use a '
                                        'comma-delimited list within square '
                                        'brackets [] to pass an array.')

        if operator == 'isnull':
            field_type = 'bool'
        else:
            try:
                field_type = view.get_serializer().fields[p_name]['type']
            except KeyError:
                field_type = 'default'

        type_norm = getattr(self, '_type_normalize_{}'.format(field_type),
                            None)
        if type_norm is not None:
            if hasattr(p_val, '__iter__'):
                new_p_val = []
                for val in p_val:
                    p_name, operator, val = type_norm(orig_p_name, p_name,
                                                      operator, val)
                    new_p_val.append(val)
                p_val = new_p_val
            else:
                p_name, operator, p_val = type_norm(orig_p_name, p_name,
                                                    operator, p_val)
        norm = getattr(self, '_normalize_{}'.format(p_name), None)
        if norm is not None:
            if hasattr(p_val, '__iter__'):
                new_p_val = []
                for val in p_val:
                    p_name, operator, val = norm(orig_p_name, p_name, operator,
                                                 val)
                    new_p_val.append(val)
                p_val = new_p_val
            else:
                p_name, operator, p_val = norm(orig_p_name, p_name, operator,
                                               p_val)

        return p_name, operator, p_val

    def _normalize_call_number(self, orig_name, name, op, val):
        '''
        Searches for call numbers should be compared to
        call_number_search and normalized using the same algorithm.
        '''
        val = helpers.NormalizedCallNumber(val, 'search').normalize()
        return ('call_number_search', op, val)

    def _type_normalize_datetime(self, orig_name, name, op, val):
        '''
        Base normalization for datetime filter fields.
        '''
        date_m = re.search(r'^\d{4}\-\d{2}\-\d{2}T(\d{2}:){2}\d{2}Z$', val)
        if date_m:
            try:
                val = dateparser.parse(val)
            except Exception as e:
                raise FilterValidationError('There was a problem '
                    'parsing the value \'{}\' in your query: {}'
                    ''.format(val, e))

        else:
            raise FilterValidationError('There was a problem filtering on '
                'the {} field: the datetime was formatted incorrectly. '
                'Dates and times are expected to be full ISO 8601-formatted '
                'strings in UTC time; e.g.: 2014-06-13T12:00:00Z would '
                'indicate June 13, 2014 at 12:00 UTC time.'
                ''.format(orig_name))
        return (name, op, val)

    def _type_normalize_bool(self, orig_name, name, op, val):
        '''
        Base normalization for boolean filter fields.
        '''
        bool_m = re.match(r'(true)|(false)', val, re.IGNORECASE)
        val = True if bool_m.group(1) else False
        return (name, op, val)

    def _get_order_param(self, order_by, view):
        ordering = getattr(view, 'ordering', [])
        order_params = order_by.split(',')
        new_order_params = []
        for orig_p in order_params:
            direction = ''
            if orig_p.startswith('-'):
                orig_p = orig_p[1:]
                direction = '-'
            p = view.get_serializer().restore_field_name(orig_p)
            if ordering is None or p not in ordering:
                raise FilterValidationError('\'{}\' is not a valid field for '
                                            'ordering results.'.format(orig_p))
            new_param = '{}{}'.format(direction, 
                                      self.order_field_mapping.get(p, p))
            new_order_params.append(new_param)

        return new_order_params

    def _prep_params(self, params, view):
        '''
        Prepares a QUERY_PARAMS dictionary to be passed to
        get_django_style_filters(). The QUERY_PARAMS dict is params.
        This also validates parameters and throws a BadQuery()
        exception if any do not validate.
        '''
        param_data = {'data': {}, 'search': '', 'order_by': ''}
        validation_errors = []
        for orig_p_name, p_val in dict(params).iteritems():
            p_name = view.get_serializer().restore_field_name(orig_p_name)

            if p_name not in self.reserved_params:
                parameter = {}
                negate = False
                m = re.search(r'(.+)\[([\-a-z]+)\]$', p_name)
                try:
                    p_name = m.group(1)
                    operator = m.group(2)
                except AttributeError:
                    operator = 'exact'

                if operator[0] == '-':
                    operator = operator.lstrip('-')
                    negate = True

                # validate and normalize this parameter
                if not isinstance(p_val, (list, tuple)):
                    p_val = [p_val]

                for pv in p_val:
                    try:
                        p_name, operator, pv = self._validate_parameter(
                            orig_p_name, p_name, operator, pv, view)
                    except FilterValidationError as e:
                        validation_errors.append(str(e))

                    p = param_data['data'].get(p_name, {})
                    if negate:
                        operator = '-{}'.format(operator)
                    p_list = p.get(operator, [])
                    p_list.append(pv)
                    p.update({operator: p_list})
                    param_data['data'][p_name] = p

            elif p_name == settings.REST_FRAMEWORK.get('SEARCH_PARAM',
                                                       'search'):
                param_data['search'] = p_val[0]

            elif p_name == settings.REST_FRAMEWORK.get('SEARCHTYPE_PARAM',
                                                       'searchtype'):
                if p_val[0] in self.searchtypes.keys():
                    param_data['searchtype'] = p_val[0]
                else:
                    msg = ('Query filter criteria specified for this resource '
                           'is invalid. The searchtype parameter must be one '
                           'of the following values: {}'.format(
                                ', '.join(self.searchtypes.keys())))
                    raise exceptions.BadQuery(detail=msg)

            elif p_name == settings.REST_FRAMEWORK.get('ORDER_BY_PARAM', 
                                                       'order_by'):
                try:
                    order_param = self._get_order_param(p_val[0], view)
                except FilterValidationError as e:
                    validation_errors.append(str(e))
                else:
                    param_data['order_by'] = order_param

        if validation_errors:
            msg = ('Query filter criteria specified for this resource is '
                   'invalid. The following errors were raised. {}'.format(
                        ' '.join(validation_errors)))
            raise exceptions.BadQuery(detail=msg)
        return param_data

    def _apply_django_style_filters(self, queryset, params):
        for p_name, p_val in params.iteritems():
            for operator, op_vals in p_val.iteritems():
                for op_val in op_vals:
                    negate = False
                    if operator[0] == '-':
                        operator = operator.lstrip('-')
                        negate = True

                    field = ''.join([p_name, '__', operator])

                    if negate:
                        queryset = queryset.exclude(**{field: op_val})
                    else:
                        queryset = queryset.filter(**{field: op_val})
        return queryset

    def filter_queryset(self, request, queryset, view):
        request_params = self._prep_params(request.query_params, view)
        try:
            queryset = self._apply_django_style_filters(queryset, 
                                                        request_params['data'])
        except KeyError:
            pass
        if request_params.get('search', None):
            q_settings = None
            if request_params.get('searchtype', None):
                q_settings = self.searchtypes[request_params['searchtype']]
            try:
                queryset = queryset.search(request_params['search'],
                                           params=q_settings)
            except SolrError as e:
                err = ujson.loads(e.message.split('\n')[1])
                msg = ('Query filter "search" parameter is invalid. The '
                       'following errors were raised. {}'.format(
                            err['error']['msg']))
                raise exceptions.BadQuery(detail=msg)
        if request_params['order_by']:
            queryset = queryset.order_by(*request_params['order_by'])
        return queryset


class MarcFilter(HaystackFilter):
    '''
    Lets us filter "marc" resources on MARC fields, like
    245 for MARC Field 245 or 245a for MARC subfield 245a.
    '''

    def _type_normalize_default(self, orig_name, name, op, val):
        '''
        Our solr index uses mf_{marc tag} and sf_{marc and subfield
        tag} to store MARC field/subfield data. We want to accept the
        MARC tag/subfields specified without "mf_" and "sf_", so we add
        these prefixes here if needed so that they match the Solr
        fields.
        '''
        if re.match(r'^\d{3}$', name):
            name = 'mf_{}'.format(name)

        if re.match(r'^\d{3}[a-z0-9]$', name):
            name = 'sf_{}'.format(name)

        return (name, op, val)
