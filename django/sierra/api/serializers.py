from collections import OrderedDict
import ujson

from django.conf import settings

from utils import solr
from utils.camel_case import render, parser
from .uris import APIUris
from .simpleserializers import SimpleSerializer, SimpleSerializerWithLookups

import logging

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class APIUserSerializer(SimpleSerializer):
    fields = OrderedDict()
    fields['username'] = {'type': 'str'}
    fields['first_name'] = {'type': 'str'}
    fields['last_name'] = {'type': 'str'}
    fields['email'] = {'type': 'str'}
    fields['permissions'] = {'type': 'compound'}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def process_permissions(self, value, obj):
        permissions = ujson.decode(obj.apiuser.permissions)
        new_permissions = {}
        for key, val in permissions.iteritems():
            new_permissions[self.render_field_name(key)] = val
        return new_permissions


class ItemSerializer(SimpleSerializerWithLookups):
    fields = OrderedDict()
    fields['_links'] = {'type': 'compound'}
    fields['id'] = {'type': 'int'}
    fields['parent_bib_record_number'] = {'type': 'int'}
    fields['parent_bib_title'] = {'type': 'str'}
    fields['parent_bib_main_author'] = {'type': 'str'}
    fields['parent_bib_publication_year'] = {'type': 'str'}
    fields['record_number'] = {'type': 'str'}
    fields['call_number'] = {'type': 'str'}
    fields['call_number_type'] = {'type': 'str'}
    fields['call_number_sort'] = {'type': 'str'}
    fields['call_number_search'] = {'type': 'str'}
    fields['volume'] = {'type': 'str'}
    fields['volume_sort'] = {'type': 'str'}
    fields['copy_number'] = {'type': 'int'}
    fields['barcode'] = {'type': 'str'}
    fields['long_messages'] = {'type': 'str'}
    fields['internal_notes'] = {'type': 'str'}
    fields['public_notes'] = {'type': 'str'}
    fields['local_code1'] = {'type': 'int'}
    fields['number_of_renewals'] = {'type': 'int'}
    fields['item_type_code'] = {'type': 'str'}
    fields['item_type'] = {'type': 'str'}
    fields['price'] = {'type': 'str'}
    fields['internal_use_count'] = {'type': 'int'}
    fields['copy_use_count'] = {'type': 'int'}
    fields['iuse3_count'] = {'type': 'int'}
    fields['total_checkout_count'] = {'type': 'int'}
    fields['total_renewal_count'] = {'type': 'int'}
    fields['year_to_date_checkout_count'] = {'type': 'int'}
    fields['last_year_to_date_checkout_count'] = {'type': 'int'}
    fields['location_code'] = {'type': 'str'}
    fields['location'] = {'type': 'str'}
    fields['status_code'] = {'type': 'str'}
    fields['status'] = {'type': 'str'}
    fields['due_date'] = {'type': 'datetime'}
    fields['checkout_date'] = {'type': 'datetime'}
    fields['last_checkin_date'] = {'type': 'datetime'}
    fields['overdue_date'] = {'type': 'datetime'}
    fields['recall_date'] = {'type': 'datetime'}
    fields['record_creation_date'] = {'type': 'datetime'}
    fields['record_last_updated_date'] = {'type': 'datetime'}
    fields['record_revision_number'] = {'type': 'datetime'}
    fields['suppressed'] = {'type': 'bool'}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def restore_field_name(self, field_name):
        return parser.camel_to_underscore(field_name)

    def cache_all_lookups(self):
        types = ['Location', 'ItemStatus', 'Itype']
        qs = solr.Queryset(page_by=1000).filter(type__in=types)
        qs = qs.only('type', 'code', 'label')
        results = [i for i in qs]
        
        lookups = {'Location': {}, 'ItemStatus': {}, 'Itype': {}}
        for r in results:
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

    def process_location(self, value, obj):
        '''
        Returns a location's label based on the obj's location_code.
        '''
        return self.get_lookup_value('location', getattr(obj, 'location_code',
                                                         None))

    def process_status(self, value, obj):
        '''
        Returns a status label based on the status_code.
        '''
        value = getattr(obj, 'status_code', None)
        if value == '-' and getattr(obj, 'due_date', None) is not None:
            return 'CHECKED OUT'
        else:
            return self.get_lookup_value('status', value)
    
    def process_item_type(self, value, obj):
        '''
        Returns item_type label based on item_type_code.
        '''
        return self.get_lookup_value('item_type', getattr(obj,
                                                          'item_type_code',
                                                          None))

    def process__links(self, value, obj):
        '''
        Generates links for each item. Doesn't use reverse URL lookups
        because those get really slow when you have lots of objects.
        I implemented my own reverse URL lookup (sort of) in api.urls,
        which is much faster.
        '''
        req = self.context.get('request', None)
        view = self.context.get('view', None)

        obj_id = getattr(obj, 'id', None)
        p_bib_id = getattr(obj, 'parent_bib_id', None)
        l_code = getattr(obj, 'location_code', None)
        itype_code = getattr(obj, 'item_type_code', None)
        istatus_code = getattr(obj, 'status_code', None)

        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri('items-detail', req=req, absolute=True,
                                                 v=view.api_version,
                                                 id=obj_id)
            }
            ret['parentBib'] = {
                'href': APIUris.get_uri('bibs-detail', req=req, absolute=True,
                                                 v=view.api_version,
                                                 id=p_bib_id)
            }
            ret['location'] = {
                'href': APIUris.get_uri('locations-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=l_code)
            }
            ret['itemtype'] = {
                'href': APIUris.get_uri('itemtypes-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=itype_code)
            }
            ret['itemstatus'] = {
                'href': APIUris.get_uri('itemstatuses-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=istatus_code)
            }

        return ret


class BibSerializer(SimpleSerializer):
    fields = OrderedDict()
    fields['_links'] = {'type': 'compound'}
    fields['id'] = {'type': 'int'}
    fields['record_number'] = {'type': 'str'}
    fields['timestamp'] = {'type': 'datetime'}
    fields['suppressed'] = {'type': 'bool'}
    fields['language'] = {'type': 'str'}
    fields['format'] = {'type': 'str'}
    fields['material_type'] = {'type': 'str'}
    fields['main_call_number'] = {'type': 'str'}
    fields['main_call_number_sort'] = {'type': 'str'}
    fields['loc_call_numbers'] = {'type': 'str'}
    fields['dewey_call_numbers'] = {'type': 'str'}
    fields['other_call_numbers'] = {'type': 'str'}
    fields['sudoc_numbers'] = {'type': 'str'}
    fields['isbn_numbers'] = {'type': 'str'}
    fields['issn_numbers'] = {'type': 'str'}
    fields['lccn_numbers'] = {'type': 'str'}
    fields['oclc_numbers'] = {'type': 'str'}
    fields['full_title'] = {'type': 'str'}
    fields['main_title'] = {'type': 'str'}
    fields['subtitle'] = {'type': 'str'}
    fields['statement_of_responsibility'] = {'type': 'str'}
    fields['uniform_title'] = {'type': 'str'}
    fields['alternate_titles'] = {'type': 'str'}
    fields['related_titles'] = {'type': 'str'}
    fields['series'] = {'type': 'str'}
    fields['series_exact'] = {'type': 'str'}
    fields['creator'] = {'type': 'str'}
    fields['contributors'] = {'type': 'str'}
    fields['series_creators'] = {'type': 'str'}
    fields['people'] = {'type': 'str'}
    fields['corporations'] = {'type': 'str'}
    fields['meetings'] = {'type': 'str'}
    fields['imprints'] = {'type': 'str'}
    fields['publication_country'] = {'type': 'str'}
    fields['publication_places'] = {'type': 'str'}
    fields['publishers'] = {'type': 'str'}
    fields['publication_dates'] = {'type': 'str'}
    fields['full_subjects'] = {'type': 'str'}
    fields['general_terms'] = {'type': 'str'}
    fields['topic_terms'] = {'type': 'str'}
    fields['genre_terms'] = {'type': 'str'}
    fields['geographic_terms'] = {'type': 'str'}
    fields['era_terms'] = {'type': 'str'}
    fields['form_terms'] = {'type': 'str'}
    fields['other_terms'] = {'type': 'str'}
    fields['physical_characteristics'] = {'type': 'str'}
    fields['toc_notes'] = {'type': 'str'}
    fields['context_notes'] = {'type': 'str'}
    fields['summary_notes'] = {'type': 'str'}
    fields['urls'] = {'type': 'str'}
    fields['url_labels'] = {'type': 'str'}
    fields['people_facet'] = {'type': 'str'}
    fields['corporations_facet'] = {'type': 'str'}
    fields['meetings_facet'] = {'type': 'str'}
    fields['topic_terms_facet'] = {'type': 'str'}
    fields['general_terms_facet'] = {'type': 'str'}
    fields['genre_terms_facet'] = {'type': 'str'}
    fields['geographic_terms_facet'] = {'type': 'str'}
    fields['era_terms_facet'] = {'type': 'str'}
    fields['form_terms_facet'] = {'type': 'str'}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def restore_field_name(self, field_name):
        return parser.camel_to_underscore(field_name)

    def process__links(self, value, obj):
        req = self.context.get('request', None)
        view = self.context.get('view', None)

        obj_id = getattr(obj, 'id', None)
        item_ids = getattr(obj, 'item_ids', None)

        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri('bibs-detail', req=req, absolute=True,
                                        v=view.api_version, id=obj_id)
            }
            ret['marc'] = {
                'href': APIUris.get_uri('marc-detail', req=req, absolute=True,
                                        v=view.api_version, id=obj_id)
            }
            if item_ids is not None:
                items = []
                for item_id in item_ids:
                    items.append({
                        'href': APIUris.get_uri('items-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        id=item_id)
                    })
                ret['items'] = items

        return ret


class MarcSerializer(SimpleSerializer):
    fields = OrderedDict()
    fields['_links'] = {'type': 'compound'}
    fields['id'] = {'type': 'int'}
    fields['record_number'] = {'type': 'str'}
    fields['timestamp'] = {'type': 'datetime'}
    fields['record'] = {'type': 'compound'}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def restore_field_name(self, field_name):
        return parser.camel_to_underscore(field_name)

    def process_record(self, value, obj):
        return ujson.loads(obj.json)

    def process__links(self, value, obj):
        req = self.context.get('request', None)
        view = self.context.get('view', None)

        obj_id = getattr(obj, 'id', None)

        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri('marc-detail', req=req, absolute=True,
                                        v=view.api_version, id=obj_id)
            }
            ret['bib'] = {
                'href': APIUris.get_uri('bibs-detail', req=req, absolute=True,
                                        v=view.api_version, id=obj_id)
            }

        return ret


class EResourceSerializer(SimpleSerializerWithLookups):
    fields = OrderedDict()
    fields['_links'] = {'type': 'compound'}
    fields['id'] = {'type': 'int'}
    fields['record_number'] = {'type': 'str'}
    fields['title'] = {'type': 'str'}
    fields['alternate_titles'] = {'type': 'str'}
    fields['eresource_type'] = {'type': 'str'}
    fields['publisher'] = {'type': 'str'}
    fields['subjects'] = {'type': 'str'}
    fields['summary'] = {'type': 'str'}
    fields['internal_notes'] = {'type': 'str'}
    fields['public_notes'] = {'type': 'str'}
    fields['alert'] = {'type': 'str'}
    fields['holdings'] = {'type': 'str'}
    fields['external_url'] = {'type': 'str'}
    fields['record_creation_date'] = {'type': 'datetime'}
    fields['record_last_updated_date'] = {'type': 'datetime'}
    fields['record_revision_number'] = {'type': 'datetime'}
    fields['suppressed'] = {'type': 'bool'}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def restore_field_name(self, field_name):
        return parser.camel_to_underscore(field_name)

    def process__links(self, value, obj):
        req = self.context.get('request', None)
        view = self.context.get('view', None)

        obj_id = getattr(obj, 'id', None)

        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri('eresources-detail', req=req, absolute=True,
                                        v=view.api_version, id=obj_id)
            }

        return ret


class LocationSerializer(SimpleSerializer):
    foreign_key_field = 'location_code'
    fields = OrderedDict()
    fields['_links'] = {'type': 'compound'}
    fields['code'] = {'type': 'str'}
    fields['label'] = {'type': 'str'}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def restore_field_name(self, field_name):
        return parser.camel_to_underscore(field_name)

    def process__links(self, value, obj):
        req = self.context.get('request', None)
        view = self.context.get('view', None)
        code = getattr(obj, 'code', None)
        fk = self.render_field_name(self.foreign_key_field)

        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri('locations-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=code)
            }
            ret['items'] = {
                'href': '{}?{}={}'.format(
                    APIUris.get_uri('items-list', req=req, absolute=True,
                    v=view.api_version), fk, code
                )
            }
        return ret


class ItemTypeSerializer(SimpleSerializer):
    foreign_key_field = 'item_type_code'
    fields = OrderedDict()
    fields['_links'] = {'type': 'compound'}
    fields['code'] = {'type': 'str'}
    fields['label'] = {'type': 'str'}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def restore_field_name(self, field_name):
        return parser.camel_to_underscore(field_name)

    def process__links(self, value, obj):
        req = self.context.get('request', None)
        view = self.context.get('view', None)
        code = getattr(obj, 'code', None)
        fk = self.render_field_name(self.foreign_key_field)

        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri('itemtypes-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=code)
            }
            ret['items'] = {
                'href': '{}?{}={}'.format(
                    APIUris.get_uri('items-list', req=req, absolute=True,
                    v=view.api_version), fk, code
                )
            }

        return ret


class ItemStatusSerializer(SimpleSerializer):
    foreign_key_field = 'status_code'
    fields = OrderedDict()
    fields['_links'] = {'type': 'compound'}
    fields['code'] = {'type': 'str'}
    fields['label'] = {'type': 'str'}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def restore_field_name(self, field_name):
        return parser.camel_to_underscore(field_name)

    def process__links(self, value, obj):
        req = self.context.get('request', None)
        view = self.context.get('view', None)
        code = getattr(obj, 'code', None)
        fk = self.render_field_name(self.foreign_key_field)

        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': APIUris.get_uri('itemstatuses-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=code)
            }
            ret['items'] = {
                'href': '{}?{}={}'.format(
                    APIUris.get_uri('items-list', req=req, absolute=True,
                    v=view.api_version), fk, code
                )
            }

        return ret
