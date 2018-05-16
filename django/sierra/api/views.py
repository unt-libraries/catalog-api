from datetime import datetime
from collections import OrderedDict

from django.conf import settings
from django.http import Http404
from django.utils import timezone as tz
from django.contrib.auth.models import User

from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import permissions

from .simpleviews import SimpleView, SimpleGetMixin
from utils import solr
from . import serializers
from . import filters
from .uris import APIUris

import logging

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


@api_view(('GET',))
def api_root(request):
    utc_offset = tz.get_default_timezone().utcoffset(datetime.now())
    utc_offset = utc_offset.total_seconds() / (60*60)
    links = {
        'self': {
            'href': APIUris.get_uri('api-root', req=request,
                                    absolute=True)
        },
        'apiusers': {
            'href': APIUris.get_uri('apiusers-list', req=request,
                                    absolute=True)
        },
        'bibs': {
            'href': APIUris.get_uri('bibs-list', req=request,
                                    absolute=True)
        },
        'marc': {
            'href': APIUris.get_uri('marc-list', req=request,
                                    absolute=True)
        },
        'items': {
            'href': APIUris.get_uri('items-list', req=request,
                                    absolute=True)
        },
        'eresources': {
            'href': APIUris.get_uri('eresources-list', req=request,
                                    absolute=True)
        },
        'locations': {
            'href': APIUris.get_uri('locations-list', req=request,
                                    absolute=True)
        },
        'itemtypes': {
            'href': APIUris.get_uri('itemtypes-list', req=request,
                                    absolute=True)
        },
        'itemstatuses': {
            'href': APIUris.get_uri('itemstatuses-list', req=request,
                                    absolute=True)
        },
        'callnumbermatches': {
            'href': APIUris.get_uri('callnumbermatches-list', req=request,
                                    absolute=True)
        },
        'firstitemperlocation': {
            'href': APIUris.get_uri('firstitemperlocation-list', req=request,
                                    absolute=True)
        },
    }
    ret_val = OrderedDict()
    ret_val['catalogApi'] = OrderedDict()
    ret_val['catalogApi']['version'] = '1'
    ret_val['catalogApi']['_links'] = OrderedDict(sorted(links.items()))
    ret_val['serverTime'] = {
        'currentTime': tz.now(),
        'timezone': tz.get_default_timezone_name(),
        'utcOffset': utc_offset
    }

    return Response(ret_val)


class APIUserList(SimpleGetMixin, SimpleView):
    '''
    Paginated list of API Users permissions. Requires authorization to
    view.
    '''
    queryset = User.objects.exclude(apiuser__exact=None)
    serializer_class = serializers.APIUserSerializer
    resource_name = 'apiusers'
    permission_classes = (permissions.IsAuthenticated,)


class APIUserDetail(SimpleGetMixin, SimpleView):
    '''
    View one API User. Requires authorization to view.
    '''
    queryset = User.objects.exclude(apiuser__exact=None)
    serializer_class = serializers.APIUserSerializer
    multi = False
    permission_classes = (permissions.IsAuthenticated,)

    def get_object(self):
        queryset = self.get_queryset()
        try:
            obj = queryset.filter(username=self.kwargs['id'])[0]
        except IndexError:
            raise Http404
        else:
            return obj


class ItemList(SimpleGetMixin, SimpleView):
    '''
    Paginated list of items. Use the 'limit' and 'offset' query
    parameters for paging.
    '''
    queryset = solr.Queryset().filter(type='Item')
    serializer_class = serializers.ItemSerializer
    ordering = ['call_number', 'barcode', 'id', 'record_number',
                'parent_bib_id', 'parent_bib_record_number', 'volume',
                'copy_number', 'checkout_date']
    filter_fields = ['record_number', 'call_number', 'volume', 'volume_sort',
        'copy_number', 'barcode', 'long_messages', 'internal_notes',
        'public_notes', 'local_code1', 'number_of_renewals', 'item_type_code',
        'price', 'internal_use_count', 'iuse3_count', 'total_checkout_count',
        'total_renewal_count', 'year_to_date_checkout_count',
        'last_year_to_date_checkout_count', 'location_code', 'status_code',
        'due_date', 'checkout_date', 'last_checkin_date', 'overdue_date',
        'recall_date', 'record_creation_date', 'record_last_updated_date',
        'record_revision_number', 'suppressed', 'parent_bib_record_number',
        'parent_bib_title', 'parent_bib_main_author',
        'parent_bib_publication_year', 'call_number_type']
    resource_name = 'items'


class ItemDetail(SimpleGetMixin, SimpleView):
    '''
    Retrieve one item.
    '''
    queryset = solr.Queryset().filter(type='Item')
    serializer_class = serializers.ItemSerializer
    multi = False
    
    def get_object(self):
        queryset = self.get_queryset()
        try:
            obj = queryset.filter(id=self.kwargs['id'])[0]
        except IndexError:
            raise Http404
        else:
            return obj


class BibList(SimpleGetMixin, SimpleView):
    '''
    Paginated list of bibs. Use the 'limit' and 'offset' query
    parameters for paging.
    '''
    queryset = solr.Queryset(using=
                 settings.REST_VIEWS_HAYSTACK_CONNECTIONS['Bibs'])
    serializer_class = serializers.BibSerializer
    ordering = ['call_number', 'id', 'record_number', 'material_type',
                'timestamp', 'main_call_number_sort']
    filter_fields = ['record_number', 'call_number', 'id', 'suppressed',
                     'material_type', 'issn_numbers', 'timestamp', 
                     'full_title', 'main_title', 'subtitle', 
                     'statement_of_responsibility', 'uniform_title',
                     'alternate_titles', 'related_titles', 'series', 'creator',
                     'contributors', 'series_creators', 'people',
                     'corporations', 'meetings', 'imprints', 
                     'publication_country', 'publication_places', 'publishers',
                     'publication_dates', 'full_subjects', 'general_terms',
                     'topic_terms', 'genre_terms', 'era_terms', 'form_terms',
                     'other_terms', 'physical_characteristics', 'toc_notes',
                     'context_notes', 'summary_notes', 'main_call_number',
                     'loc_call_numbers', 'dewey_call_numbers', 
                     'other_call_numbers', 'sudoc_numbers', 'isbn_numbers',
                     'lccn_numbers', 'oclc_numbers']
    resource_name = 'bibs'


class BibDetail(SimpleGetMixin, SimpleView):
    '''
    Retrieve one bib.
    '''
    queryset = solr.Queryset(using=
                 settings.REST_VIEWS_HAYSTACK_CONNECTIONS['Bibs'])
    serializer_class = serializers.BibSerializer
    multi = False

    def get_object(self):
        queryset = self.get_queryset()
        try:
            obj = queryset.filter(id=self.kwargs['id'])[0]
        except IndexError:
            raise Http404
        else:
            return obj


class MarcList(SimpleGetMixin, SimpleView):
    '''
    Paginated list of MARC records. Use the 'limit' and 'offset' query
    parameters for paging.
    '''
    queryset = solr.Queryset(using=
                 settings.REST_VIEWS_HAYSTACK_CONNECTIONS['Marc'])
    serializer_class = serializers.MarcSerializer
    resource_name = 'marc'
    filter_fields = ['record_number', '/^(mf_)?\\d{3}$/',
        '/^(sf_)?\\d{3}[a-z0-9]$/']
    filter_class = filters.MarcFilter


class MarcDetail(SimpleGetMixin, SimpleView):
    '''
    Retrieve one MARC record.
    '''
    queryset = solr.Queryset(using=
                 settings.REST_VIEWS_HAYSTACK_CONNECTIONS['Marc'])
    serializer_class = serializers.MarcSerializer
    multi = False

    def get_object(self):
        queryset = self.get_queryset()
        try:
            obj = queryset.filter(id=self.kwargs['id'])[0]
        except IndexError:
            raise Http404
        else:
            return obj


class EResourceList(SimpleGetMixin, SimpleView):
    '''
    Paginated list of eresources. Use the 'limit' and 'offset' query
    parameters for paging.
    '''
    queryset = solr.Queryset().filter(type='eResource')
    serializer_class = serializers.EResourceSerializer
    ordering = ['record_number', 'parent_bib_record_number', 'eresource_type',
                'publisher', 'title', 'alert']
    filter_fields = ['record_number', 'parent_bib_record_number',
                     'eresource_type', 'publisher', 'title',
                     'alternate_titles', 'subjects', 'summary',
                     'internal_notes', 'public_notes', 'alert', 'holdings',
                     'suppressed']
    resource_name = 'eresources'


class EResourceDetail(SimpleGetMixin, SimpleView):
    '''
    Retrieve one eresource.
    '''
    queryset = solr.Queryset().filter(type='eResource')
    serializer_class = serializers.EResourceSerializer
    multi = False
    
    def get_object(self):
        queryset = self.get_queryset()
        try:
            obj = queryset.filter(id=self.kwargs['id'])[0]
        except IndexError:
            raise Http404
        else:
            return obj


class LocationList(SimpleGetMixin, SimpleView):
    '''
    Paginated list of bibs. Use the 'limit' and 'offset' query
    parameters for paging.
    '''
    queryset = solr.Queryset().filter(type='Location')
    serializer_class = serializers.LocationSerializer
    resource_name = 'locations'
    ordering = ['code', 'label']
    filter_fields = ['code', 'label']


class LocationDetail(SimpleGetMixin, SimpleView):
    '''
    Retrieve one Location.
    '''
    queryset = solr.Queryset().filter(type='Location')
    serializer_class = serializers.LocationSerializer
    multi = False

    def get_object(self):
        queryset = self.get_queryset()
        try:
            obj = queryset.filter(code=self.kwargs['code'])[0]
        except IndexError:
            raise Http404
        else:
            return obj


class ItemTypesList(SimpleGetMixin, SimpleView):
    '''
    Paginated list of bibs. Use the 'limit' and 'offset' query
    parameters for paging.
    '''
    queryset = solr.Queryset().filter(type='Itype')
    serializer_class = serializers.ItemTypeSerializer
    resource_name = 'itemtypes'
    ordering = ['code', 'label']
    filter_fields = ['code', 'label']


class ItemTypesDetail(SimpleGetMixin, SimpleView):
    '''
    Retrieve one Location.
    '''
    queryset = solr.Queryset().filter(type='Itype')
    serializer_class = serializers.ItemTypeSerializer
    multi = False

    def get_object(self):
        queryset = self.get_queryset()
        try:
            obj = queryset.filter(code=self.kwargs['code'])[0]
        except IndexError:
            raise Http404
        else:
            return obj


class ItemStatusesList(SimpleGetMixin, SimpleView):
    '''
    Paginated list of bibs. Use the 'limit' and 'offset' query
    parameters for paging.
    '''
    queryset = solr.Queryset().filter(type='ItemStatus')
    serializer_class = serializers.ItemStatusSerializer
    resource_name = 'itemstatuses'
    ordering = ['code', 'label']
    filter_fields = ['code', 'label']


class ItemStatusesDetail(SimpleGetMixin, SimpleView):
    '''
    Retrieve one Item Status.
    '''
    queryset = solr.Queryset().filter(type='ItemStatus')
    serializer_class = serializers.ItemStatusSerializer
    multi = False

    def get_object(self):
        queryset = self.get_queryset()
        try:
            obj = queryset.filter(code=self.kwargs['code'])[0]
        except IndexError:
            raise Http404
        else:
            return obj


class CallnumbermatchesList(SimpleGetMixin, SimpleView):
    '''
    Returns the first X matching call numbers, where X is the supplied
    limit. Pagination (offset) is not supported.
    You can filter using the
    following fields: callNumber, locationCode, and callNumberType.
    '''
    queryset = solr.Queryset().filter(type='Item').only(
                'call_number').order_by('call_number_sort')
    serializer_class = serializers.ItemSerializer
    resource_name = 'callnumber_matches'
    filter_fields = ['call_number', 'location_code', 'call_number_type']

    def paginate(self, queryset, request):
        # for paging, we only use the 'limit'
        limit_p = settings.REST_FRAMEWORK.get('PAGINATE_BY_PARAM', 'limit')
        max_limit = settings.REST_FRAMEWORK.get('MAX_PAGINATE_BY', 500)
        default_limit = settings.REST_FRAMEWORK.get('PAGINATE_BY', 10)
        limit = int(request.query_params.get(limit_p, default_limit))
        limit = max_limit if limit > max_limit else limit
        data, i, count = [], 0, queryset.count()
        while len(data) < limit and i < count:
            call_number = queryset[i].get('call_number', None)
            if call_number is not None and call_number not in data:
                data.append(call_number)
            i += 1

        return data


class FirstItemPerLocationList(SimpleGetMixin, SimpleView):
    '''
    Returns the first item (by call number) for each location within a
    filtered result set.
    '''
    facet_field = 'location_code'
    queryset = solr.Queryset().filter(type='Item').search('*:*', 
        params={'facet': 'true', 'facet.field': facet_field, 
                'facet.sort': 'index', 'facet.mincount': 1})
    serializer_class = serializers.ItemSerializer
    filter_fields = ['call_number', 'call_number_type', 'barcode']

    def paginate(self, queryset, request):
        ff = self.facet_field
        facets = queryset.full_response.facets['facet_fields'][ff]
        fields = ['id', 'parent_bib_title', 'parent_bib_record_number',
                  'call_number', 'barcode', 'record_number', 'call_number_type']
        total_count = len(facets) / 2
        items = []

        for key in facets[0:len(facets):2]:
            facet_qs = solr.Queryset()
            facet_qs._search_params['fq'] = queryset._search_params['fq']
            facet_qs = facet_qs.filter(**{ff: key})
            facet_qs = facet_qs.order_by('call_number_sort').only(*fields)
            item_uri = APIUris.get_uri('items-detail', id=facet_qs[0]['id'],
                                       req=request, absolute=True)
            items.append({
                '_links': { 'self': { 'href': item_uri } },
                'id': facet_qs[0].get('id', None),
                'parentBibRecordNumber': 
                    facet_qs[0].get('parent_bib_record_number', None),
                'parentBibTitle': facet_qs[0].get('parent_bib_title', None),
                'recordNumber':
                    facet_qs[0].get('record_number', None),
                'callNumber': facet_qs[0].get('call_number', None),
                'callNumberType': facet_qs[0].get('call_number_type', None),
                'barcode': facet_qs[0].get('barcode', None),
                'locationCode': key,
            })

        data = OrderedDict()
        data['totalCount'] = total_count
        data['_links'] = {'self': request.build_absolute_uri()}
        data['_embedded'] = {'items': items}

        return data
