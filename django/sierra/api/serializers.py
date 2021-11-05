"""
Contains DRF serializer classes for the API.
"""

from __future__ import absolute_import

import re
import logging
from collections import OrderedDict
from datetime import datetime

import ujson
from dateutil import parser as dateparser
from utils import solr, helpers
from utils.camel_case import render

from .simpleserializers import SimpleField, SimpleObjectInterface,\
                               SimpleSerializer, SimpleSerializerWithLookups
from .models import APIUser
from .uris import APIUris

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class SimpleBoolField(SimpleField):
    data_type = bool
    false_strings = set(['false', 'f', '0', 'null', 'n', 'no', 'not'])

    @classmethod
    def cast_one_to_python(cls, val):
        return helpers.cast_to_boolean(val, cls.false_strings)


class SimpleStrField(SimpleField):
    data_type = str


class SimpleIntField(SimpleField):
    data_type = int

    @classmethod
    def cast_one_to_python(cls, val):
        return int(float(val))


class SimpleDateTimeField(SimpleField):
    data_type = datetime

    @classmethod
    def cast_one_to_python(cls, val):
        if isinstance(val, datetime):
            return val
        if isinstance(val, str):
            date_re = r'^\d{4}\-\d{2}\-\d{2}T(\d{2}:){2}\d{2}(\.\d+)?Z$'
            date_match = re.search(date_re, val)
            if date_match:
                try:
                    return dateparser.parse(val)
                except Exception as e:
                    raise ValueError(e)
            msg = ('The datetime is formatted incorrectly. Dates and times are '
                   'expected to be full ISO 8601-formatted strings in UTC '
                   'time; e.g.: 2014-06-13T12:00:00Z would indicate June 13, '
                   '2014 at 12:00 UTC time.')
            raise ValueError(msg)
        raise ValueError('Unknown datetime conversion.')


class SimpleJSONField(SimpleField):
    data_type = dict

    @classmethod
    def cast_one_to_python(cls, val):
        return ujson.loads(val)


class APIUserSerializer(SimpleSerializer):
    class APIUserObjInterface(SimpleObjectInterface):
        obj_type = APIUser

        def get_obj_data(self, obj):
            obj_data = super().get_obj_data(obj)
            obj_data['apiuser'] = obj.apiuser
            return obj_data

    fields = [
        SimpleStrField('username'),
        SimpleStrField('first_name'),
        SimpleStrField('last_name'),
        SimpleStrField('email'),
        SimpleStrField('permissions')
    ]
    obj_interface = APIUserObjInterface()

    def present_permissions(self, obj_data):
        permissions = ujson.decode(obj_data['apiuser'].permissions)
        new_permissions = {}
        for key, val in permissions.items():
            new_permissions[render.underscoreToCamel(key)] = val
        return new_permissions


class ItemSerializer(SimpleSerializerWithLookups):
    obj_interface = SimpleObjectInterface(solr.Result)
    fields = [
        SimpleField('_links', derived=True),
        SimpleStrField('id', orderable=True, filterable=True),
        SimpleStrField('record_number', source='id', orderable=True,
                       filterable=True),
        SimpleStrField('parent_bib_id', orderable=True, filterable=True),
        SimpleStrField('parent_bib_record_number', source='parent_bib_id',
                       orderable=True, filterable=True),
        SimpleStrField('parent_bib_title', filterable=True),
        SimpleStrField('parent_bib_main_author', filterable=True),
        SimpleStrField('parent_bib_publication_year', filterable=True),
        SimpleStrField('call_number', filterable=True, orderable=True,
                       filter_source='call_number_search',
                       keyword_source='call_number_search',
                       order_source='call_number_sort'),
        SimpleStrField('call_number_type', filterable=True),
        SimpleStrField('call_number_sort', orderable=True),
        SimpleStrField('call_number_search'),
        SimpleStrField('volume', orderable=True, filterable=True,
                       order_source='volume_sort'),
        SimpleStrField('volume_sort', orderable=True, filterable=True),
        SimpleIntField('copy_number', orderable=True, filterable=True),
        SimpleStrField('barcode', orderable=True, filterable=True),
        SimpleStrField('long_messages', filterable=True),
        SimpleStrField('internal_notes', filterable=True),
        SimpleStrField('public_notes', filterable=True),
        SimpleIntField('local_code1', filterable=True),
        SimpleIntField('number_of_renewals', filterable=True),
        SimpleStrField('item_type_code', filterable=True),
        SimpleStrField('item_type'),
        SimpleStrField('price', filterable=True),
        SimpleIntField('internal_use_count', filterable=True),
        SimpleIntField('copy_use_count'),
        SimpleIntField('iuse3_count', filterable=True),
        SimpleIntField('total_checkout_count', filterable=True),
        SimpleIntField('total_renewal_count', filterable=True),
        SimpleIntField('year_to_date_checkout_count', filterable=True),
        SimpleIntField('last_year_to_date_checkout_count', filterable=True),
        SimpleStrField('location_code', filterable=True),
        SimpleStrField('location'),
        SimpleStrField('status_code', filterable=True),
        SimpleStrField('status'),
        SimpleDateTimeField('due_date', filterable=True),
        SimpleDateTimeField('checkout_date', orderable=True, filterable=True),
        SimpleDateTimeField('last_checkin_date', filterable=True),
        SimpleDateTimeField('overdue_date', filterable=True),
        SimpleDateTimeField('recall_date', filterable=True),
        SimpleDateTimeField('record_creation_date', filterable=True),
        SimpleDateTimeField('record_last_updated_date', filterable=True),
        SimpleIntField('record_revision_number', filterable=True),
        SimpleBoolField('suppressed', filterable=True)
    ]

    def cache_all_lookups(self):
        types = ['Location', 'ItemStatus', 'Itype']
        lookups = {t: {} for t in types}
        qs = solr.Queryset(page_by=1000).filter(type__in=types)
        for r in qs.only('type', 'code', 'label'):
            try:
                lookups[r['type']][r['code']] = r['label']
            except KeyError:
                try:
                    lookups[r['type']][r['code']] = None
                except KeyError:
                    pass
        self.cache_lookup('location', lookups['Location'])
        self.cache_lookup('status', lookups['ItemStatus'])
        self.cache_lookup('item_type', lookups['Itype'])

    def present_location(self, obj_data):
        """
        Returns a location's label based on the obj's location_code.
        """
        return self.get_lookup_value('location', obj_data.get('location_code'))

    def present_status(self, obj_data):
        """
        Returns a status label based on the status_code.
        """
        value = obj_data.get('status_code')
        if value == '-' and obj_data.get('due_date') is not None:
            return 'CHECKED OUT'
        else:
            return self.get_lookup_value('status', value)

    def present_item_type(self, obj_data):
        """
        Returns item_type label based on item_type_code.
        """
        item_type_code = obj_data.get('item_type_code')
        return self.get_lookup_value('item_type', item_type_code)

    def present__links(self, obj_data):
        """
        Generates links for each item. Doesn't use reverse URL lookups
        because those get really slow when you have lots of objects.
        I implemented my own reverse URL lookup (sort of) in api.urls,
        which is much faster.
        """
        req = self.context.get('request', None)
        view = self.context.get('view', None)

        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri('items-detail', req=req, absolute=True,
                                        v=view.api_version,
                                        id=obj_data.get('id'))
            }
            ret['parentBib'] = {
                'href': APIUris.get_uri('bibs-detail', req=req, absolute=True,
                                        v=view.api_version,
                                        id=obj_data.get('parent_bib_id'))
            }
            ret['location'] = {
                'href': APIUris.get_uri('locations-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=obj_data.get('location_code'))
            }
            ret['itemtype'] = {
                'href': APIUris.get_uri('itemtypes-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=obj_data.get('item_type_code'))
            }
            ret['itemstatus'] = {
                'href': APIUris.get_uri('itemstatuses-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=obj_data.get('status_code'))
            }
        return ret

    def apply_call_number_filter_to_qset(self, qval, op, negate, qset):
        if op != 'isnull':
            qval = helpers.NormalizedCallNumber(qval, 'search').normalize()
        f = self.field_lookup['call_number']
        return f.apply_filter_to_qset(qval, op, negate, qset)


class BibSerializer(SimpleSerializer):
    obj_interface = SimpleObjectInterface(solr.Result)
    fields = [
        SimpleField('_links', derived=True),
        SimpleStrField('id', orderable=True, filterable=True),
        SimpleStrField('record_number', source='id', orderable=True,
                       filterable=True),
        SimpleBoolField('suppressed', orderable=True, filterable=True),
        SimpleDateTimeField('date_added', orderable=True, filterable=True),
        SimpleIntField('record_boost', orderable=True, filterable=True),
        SimpleStrField('access_facet', filterable=True),
        SimpleDateTimeField('timestamp_of_last_solr_update', orderable=True),
        SimpleStrField('resource_type', orderable=True, filterable=True),
        SimpleStrField('resource_type_facet', filterable=True),
        SimpleStrField('media_type_facet', filterable=True),
        SimpleStrField('building_locations'),
        SimpleStrField('building_facet', filterable=True),
        SimpleStrField('shelf_facet', filterable=True),
        SimpleStrField('collection_facet', filterable=True),
        SimpleStrField('languages', filterable=True),
        SimpleStrField('language_notes'),
        SimpleStrField('games_ages_facet', filterable=True),
        SimpleStrField('games_duration_facet', filterable=True),
        SimpleStrField('games_players_facet', filterable=True),
        SimpleStrField('thumbnail_url'),
        SimpleJSONField('urls_json'),
        SimpleJSONField('items_json'),
        SimpleBoolField('has_more_items', orderable=True, filterable=True),
        SimpleJSONField('more_items_json'),
        SimpleStrField('call_numbers_display'),
        SimpleStrField('sudocs_display'),
        SimpleStrField('lccns_display'),
        SimpleStrField('oclc_numbers_display'),
        SimpleStrField('other_control_numbers_display'),
        SimpleStrField('isbns_display'),
        SimpleStrField('issns_display'),
        SimpleStrField('other_standard_numbers_display'),
        SimpleStrField('isbn_numbers'),
        SimpleStrField('lccn_number'),
        SimpleStrField('issn_numbers'),
        SimpleStrField('oclc_numbers'),
        SimpleStrField('all_standard_numbers'),
        SimpleStrField('all_control_numbers'),
        SimpleStrField('publication_year_display'),
        SimpleIntField('publication_year_range_facet', filterable=True),
        SimpleStrField('current_publication_frequency'),
        SimpleStrField('former_publication_frequency'),
        SimpleStrField('creation_display'),
        SimpleStrField('publication_display'),
        SimpleStrField('distribution_display'),
        SimpleStrField('manufacture_display'),
        SimpleStrField('copyright_display'),
        SimpleStrField('publication_date_notes'),
        SimpleStrField('publication_sort', orderable=True),
        SimpleJSONField('author_json'),
        SimpleJSONField('contributors_json'),
        SimpleStrField('author_contributor_facet', filterable=True),
        SimpleJSONField('meetings_json'),
        SimpleStrField('meeting_facet', filterable=True),
        SimpleStrField('responsibility_display'),
        SimpleStrField('author_sort', orderable=True, filterable=True),
        SimpleStrField('title_display'),
        SimpleStrField('non_truncated_title_display'),
        SimpleStrField('main_work_title_json'),
        SimpleStrField('included_work_titles_json'),
        SimpleStrField('related_work_titles_json'),
        SimpleStrField('related_series_titles_json'),
        SimpleStrField('title_series_facet', filterable=True),
        SimpleStrField('variant_titles_notes'),
        SimpleStrField('title_sort', orderable=True, filterable=True),
        SimpleStrField('editions_display'),
        SimpleJSONField('subject_headings_json'),
        SimpleJSONField('genre_headings_json'),
        SimpleStrField('subject_heading_facet', filterable=True),
        SimpleStrField('genre_heading_facet', filterable=True),
        SimpleStrField('topic_facet', filterable=True),
        SimpleStrField('era_facet', filterable=True),
        SimpleStrField('region_facet', filterable=True),
        SimpleStrField('genre_facet', filterable=True),
        SimpleJSONField('serial_continuity_linking_json'),
        SimpleJSONField('related_resources_linking_json'),
        SimpleStrField('toc_notes'),
        SimpleStrField('summary_notes'),
        SimpleStrField('arrangement_of_materials'),
        SimpleStrField('physical_description'),
        SimpleStrField('physical_medium'),
        SimpleStrField('geospatial_data'),
        SimpleStrField('audio_characteristics'),
        SimpleStrField('projection_characteristics'),
        SimpleStrField('video_characteristics'),
        SimpleStrField('digital_file_characteristics'),
        SimpleStrField('graphic_representation'),
        SimpleStrField('performance_medium'),
        SimpleStrField('production_credits'),
        SimpleStrField('performers'),
        SimpleStrField('dissertation_notes'),
        SimpleStrField('audience'),
        SimpleStrField('creator_demographics'),
        SimpleStrField('curriculum_objectives'),
        SimpleStrField('system_details'),
        SimpleStrField('notes'),
        SimpleStrField('library_has_display'),
    ]

    def __init__(self, instance=None, data=None, context=None):
        self.reset_cached_json()
        super().__init__(instance, data, context)

    def reset_cached_json(self):
        self.items_from_json = None
        self.more_items_from_json = None

    def prepare_for_serialization(self, obj_data):
        self.reset_cached_json()
        return obj_data

    def present_items_json(self, obj_data):
        if self.items_from_json is None:
            f = self.field_lookup['items_json']
            self.items_from_json = f.present(obj_data)
        return self.items_from_json

    def present_more_items_json(self, obj_data):
        if self.more_items_from_json is None:
            f = self.field_lookup['more_items_json']
            self.more_items_from_json = f.present(obj_data)
        return self.more_items_from_json

    def present__links(self, obj_data):
        req = self.context.get('request')
        view = self.context.get('view')
        obj_id = obj_data.get('id')
        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri('bibs-detail', req=req, absolute=True,
                                        v=view.api_version, id=obj_id)
            }
            items = self.present_items_json(obj_data) or []
            items.extend(self.present_more_items_json(obj_data) or [])
            if len(items) > 0:
                ret['items'] = [{
                    'href': APIUris.get_uri(
                        'items-detail', req=req, absolute=True,
                        v=view.api_version, id=item['i']
                    )
                } for item in items]
        return ret


class EResourceSerializer(SimpleSerializerWithLookups):
    obj_interface = SimpleObjectInterface(solr.Result)
    fields = [
        SimpleField('_links', derived=True),
        SimpleStrField('id', orderable=True, filterable=True),
        SimpleStrField('record_number', source='id',
                       orderable=True, filterable=True),
        SimpleStrField('title', orderable=True, filterable=True),
        SimpleStrField('alternate_titles', orderable=True, filterable=True),
        SimpleStrField('eresource_type', orderable=True, filterable=True),
        SimpleStrField('publisher', orderable=True, filterable=True),
        SimpleStrField('subjects', filterable=True),
        SimpleStrField('summary', filterable=True),
        SimpleStrField('internal_notes', filterable=True),
        SimpleStrField('public_notes', filterable=True),
        SimpleStrField('alert', orderable=True, filterable=True),
        SimpleStrField('holdings', filterable=True),
        SimpleStrField('external_url'),
        SimpleDateTimeField('record_creation_date'),
        SimpleDateTimeField('record_last_updated_date'),
        SimpleIntField('record_revision_number'),
        SimpleBoolField('suppressed', filterable=True),
    ]

    def present__links(self, obj_data):
        req = self.context.get('request', None)
        view = self.context.get('view', None)
        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri(
                    'eresources-detail', req=req, absolute=True,
                    v=view.api_version, id=obj_data.get('id')
                )
            }
        return ret


class ItemCodeSerializer(SimpleSerializer):
    foreign_key_field_name = None
    detail_uri_str = ''
    fields = [
        SimpleField('_links', derived=True),
        SimpleStrField('code', orderable=True, filterable=True),
        SimpleStrField('label', orderable=True, filterable=True)
    ]

    def present__links(self, obj_data):
        req = self.context.get('request', None)
        view = self.context.get('view', None)
        code = obj_data.get('code')
        fk = self.foreign_key_field_name
        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri(
                    self.detail_uri_str, req=req, absolute=True,
                    v=view.api_version, code=code
                )
            }
            items_url = APIUris.get_uri('items-list', req=req, absolute=True,
                                        v=view.api_version)
            ret['items'] = {'href': '{}?{}={}'.format(items_url, fk, code)}
        return ret


class LocationSerializer(ItemCodeSerializer):
    obj_interface = SimpleObjectInterface(solr.Result)
    foreign_key_field_name = 'locationCode'
    detail_uri_str = 'locations-detail'


class ItemTypeSerializer(ItemCodeSerializer):
    obj_interface = SimpleObjectInterface(solr.Result)
    foreign_key_field_name = 'itemTypeCode'
    detail_uri_str = 'itemtypes-detail'


class ItemStatusSerializer(ItemCodeSerializer):
    obj_interface = SimpleObjectInterface(solr.Result)
    foreign_key_field_name = 'statusCode'
    detail_uri_str = 'itemstatuses-detail'

