from collections import OrderedDict
from copy import copy

from django.conf import settings

from api.simpleserializers import SimpleSerializerWithLookups
from api import serializers as api_serializers
from api.uris import APIUris
from .uris import ShelflistAPIUris
from utils.redisobjs import RedisObject
from utils import solr
from utils.camel_case import render, parser

import logging

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')

class ShelflistItemSerializer(SimpleSerializerWithLookups):
    fields = OrderedDict()
    fields['_links'] = {'type': 'str', 'derived': True}
    fields['id'] = {'type': 'int'}
    fields['record_number'] = {'type': 'str'}
    fields['row_number'] = {'type': 'int', 'derived': True}
    fields['call_number'] = {'type': 'str'}
    fields['call_number_type'] = {'type': 'str'}
    fields['volume'] = {'type': 'str'}
    fields['copy_number'] = {'type': 'int'}
    fields['barcode'] = {'type': 'str'}
    fields['status'] = {'type': 'str', 'derived': True}
    fields['due_date'] = {'type': 'datetime'}
    fields['suppressed'] = {'type': 'bool'}
    fields['datetime_created'] = {'type': 'datetime',
                                  'source': 'record_creation_date'}
    fields['datetime_updated'] = {'type': 'datetime',
                                  'source': 'record_last_updated_date'}
    fields['shelf_status'] = {'type': 'str', 'writable': True}
    fields['inventory_notes'] = {'type': 'str', 'writable': True}
    fields['inventory_date'] = {'type': 'datetime', 'writable': True}
    fields['flags'] = {'type': 'str', 'writable': True}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def restore_field_name(self, field_name):
        return parser.camel_to_underscore(field_name)

    def cache_all_lookups(self):
        qs = solr.Queryset().filter(type='ItemStatus').only('code', 'label')
        results = [i for i in qs]

        lookup = {}
        for r in results:
            try:
                lookup[r['code']] = r['label']
            except KeyError:
                try:
                    lookup[r['code']] = None
                except KeyError:
                    pass
        self.cache_lookup('status', lookup)

    def process_row_number(self, value, obj):
        r = RedisObject('shelflistitem_manifest', obj.location_code)
        return r.get_index(obj.id)

    def process_status(self, value, obj):
        '''
        Returns a status label based on the status_code.
        '''
        value = getattr(obj, 'status_code', None)
        if value == '-' and getattr(obj, 'due_date', None) is not None:
            return 'CHECKED OUT'
        else:
            return self.get_lookup_value('status', value)

    def process__links(self, value, obj):
        req = self.context.get('request', None)
        view = self.context.get('view', None)
        obj_id = getattr(obj, 'id', None)
        l_code = getattr(obj, 'location_code', None)

        ret = OrderedDict()
        if req is not None and view is not None:
            ret['self'] = {
                'href': ShelflistAPIUris.get_uri('shelflistitems-detail',
                                                 req=req, absolute=True,
                                                 v=view.api_version,
                                                 code=l_code, id=obj_id)
            }
            ret['item'] = {
                'href': APIUris.get_uri('items-detail', req=req, absolute=True,
                                        v=view.api_version, id=obj_id)
            }
            ret['location'] = {
                'href': APIUris.get_uri('locations-detail', req=req,
                                        absolute=True, v=view.api_version,
                                        code=l_code)
            }

        return ret


class ItemSerializer(api_serializers.ItemSerializer):
    def process__links(self, value, obj):
        req = self.context.get('request', None)
        view = self.context.get('view', None)
        obj_id = getattr(obj, 'id', None)
        l_code = getattr(obj, 'location_code', None)

        ret = super(ItemSerializer, self).process__links(value, obj)
        if req is not None and view is not None:
            ret['shelflistItem'] = {
                'href': ShelflistAPIUris.get_uri('shelflistitems-detail',
                                                 req=req, absolute=True,
                                                 v=view.api_version,
                                                 code=l_code, id=obj_id)
            }

        return ret


class LocationSerializer(api_serializers.LocationSerializer):
    def process__links(self, value, obj):
        req = self.context.get('request', None)
        view = self.context.get('view', None)
        code = getattr(obj, 'code', None)

        ret = super(LocationSerializer, self).process__links(value, obj)
        if req is not None and view is not None:
            ret['shelflist'] = {
                'href': ShelflistAPIUris.get_uri('shelflistitems-list',
                                                 req=req, absolute=True,
                                                 v=view.api_version, code=code)
            }

        return ret

