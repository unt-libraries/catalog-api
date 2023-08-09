"""
Contains DRF serializer classes for the shelflist app.
"""

from __future__ import absolute_import

import logging
from collections import OrderedDict

from api import serializers as api_serializers
from api.simpleserializers import SimpleField, SimpleObjectInterface,\
                                  SimpleSerializerWithLookups
from api.uris import APIUris
from django.conf import settings
from utils import solr
from utils.redisobjs import RedisObject

from .uris import ShelflistAPIUris

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class ShelflistItemSerializer(SimpleSerializerWithLookups):
    _save_conn = settings.REST_VIEWS_HAYSTACK_CONNECTIONS['ShelflistItems']
    _lookup_conn = settings.REST_VIEWS_HAYSTACK_CONNECTIONS['ItemStatuses']

    class LinksField(SimpleField):
        def present(self, obj_data):
            req = obj_data['__context'].get('request', None)
            view = obj_data['__context'].get('view', None)

            ret = OrderedDict()
            if req is not None and view is not None:
                ret['self'] = {
                    'href': ShelflistAPIUris.get_uri(
                        'shelflistitems-detail', req=req, absolute=True,
                        v=view.api_version, code=obj_data.get('location_code'),
                        id=obj_data.get('id')
                    )
                }
                ret['item'] = {
                    'href': APIUris.get_uri(
                        'items-detail', req=req, absolute=True,
                        v=view.api_version, id=obj_data.get('id')
                    )
                }
                ret['location'] = {
                    'href': APIUris.get_uri(
                        'locations-detail', req=req, absolute=True,
                        v=view.api_version, code=obj_data.get('location_code')
                    )
                }
            return ret

    class RowNumberField(api_serializers.SimpleIntField):
        def present(self, obj_data):
            r = RedisObject('shelflistitem_manifest',
                            obj_data.get('location_code'))
            return r.get_index(obj_data.get('id'))

    obj_interface = SimpleObjectInterface(solr.Result)
    fields = [
        LinksField('_links', derived=True),
        api_serializers.SimpleStrField('id', filterable=True),
        api_serializers.SimpleStrField('record_number', source='id',
                                       filterable=True),
        RowNumberField('row_number', derived=True), 
        api_serializers.CallNumberField('call_number', filterable=True,
                                        filter_source='call_number_search'),
        api_serializers.SimpleStrField('call_number_type', filterable=True),
        api_serializers.SimpleStrField('volume', filterable=True),
        api_serializers.SimpleIntField('copy_number', filterable=True),
        api_serializers.SimpleStrField('barcode', filterable=True),
        api_serializers.SimpleStrField('status'),
        api_serializers.SimpleStrField('status_code', filterable=True),
        api_serializers.SimpleDateTimeField('due_date', filterable=True),
        api_serializers.SimpleBoolField('suppressed', filterable=True),
        api_serializers.SimpleDateTimeField('datetime_created',
                                            source='record_creation_date'),
        api_serializers.SimpleDateTimeField('datetime_updated',
                                            source='record_last_updated_date'),
        api_serializers.SimpleStrField('shelf_status', writeable=True,
                                       filterable=True),
        api_serializers.SimpleStrField('inventory_notes', writeable=True,
                                       filterable=True),
        api_serializers.SimpleDateTimeField('inventory_date', writeable=True,
                                            filterable=True),
        api_serializers.SimpleStrField('flags', writeable=True,
                                       filterable=True),
    ]

    def cache_all_lookups(self):
        self.refresh_status()

    def refresh_status(self):
        qs = solr.Queryset(
            using=self._lookup_conn
        ).filter(type='ItemStatus').only('code', 'label')
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

    def prepare_for_serialization(self, obj_data):
        obj_data['__context'] = self.context
        st_code = obj_data.get('status_code')
        if st_code == '-' and obj_data.get('due_date') is not None:
            obj_data['status'] = 'CHECKED OUT'
        else:
            obj_data['status'] = self.get_lookup_value('status', st_code)
        return obj_data

    def save(self, *args, **kwargs):
        kwargs['using'] = self._save_conn
        super().save(*args, **kwargs)


class ItemSerializer(api_serializers.ItemSerializer):
    class LinksField(api_serializers.ItemSerializer.LinksField):
        def present(self, obj_data):
            req = obj_data['__context'].get('request')
            view = obj_data['__context'].get('view')
            ret = super().present(obj_data)
            if req is not None and view is not None:
                ret['shelflistItem'] = {
                    'href': ShelflistAPIUris.get_uri(
                        'shelflistitems-detail', req=req, absolute=True,
                        v=view.api_version, code=obj_data.get('location_code'),
                        id=obj_data.get('id')
                    )
                }
            return ret

    @classmethod
    def set_up_field_lookup(cls):
        if not hasattr(cls, 'field_lookup'):
            new_fields = []
            for f in cls.fields:
                if f.name == '_links':
                    new_fields.append(cls.LinksField('_links', derived=True))
                else:
                    new_fields.append(f)
            cls.fields = new_fields
            super().set_up_field_lookup()


class LocationSerializer(api_serializers.LocationSerializer):
    class LinksField(api_serializers.LocationSerializer.LinksField):
        def present(self, obj_data):
            req = obj_data['__context'].get('request')
            view = obj_data['__context'].get('view')
            ret = super().present(obj_data)
            if req is not None and view is not None:
                ret['shelflist'] = {
                    'href': ShelflistAPIUris.get_uri(
                        'shelflistitems-list', req=req, absolute=True,
                        v=view.api_version, code=obj_data.get('code')
                    )
                }
            return ret

    @classmethod
    def set_up_field_lookup(cls):
        if not hasattr(cls, 'field_lookup'):
            new_fields = []
            for f in cls.fields:
                if f.name == '_links':
                    new_fields.append(cls.LinksField('_links', derived=True))
                else:
                    new_fields.append(f)
            cls.fields = new_fields
            super().set_up_field_lookup()

