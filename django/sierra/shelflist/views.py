from __future__ import absolute_import
from django.http import Http404
from collections import OrderedDict

from rest_framework.parsers import JSONParser
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import permissions

from utils import solr
from utils.redisobjs import RedisObject
from api import views as api_views
from api.simpleviews import SimpleView, SimpleGetMixin, SimplePatchMixin, SimplePutMixin
from .uris import ShelflistAPIUris
from . import serializers
from .parsers import JSONPatchParser

import logging

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


@api_view(('GET',))
def api_root(request):
    # ._request is a protected member of DRFRequest, but don't know how else to
    # get what we need here.
    resp = api_views.get_api_root(request._request)
    links = resp.data['catalogApi']['_links']
    links['shelflistitems'] = {
        'href': ShelflistAPIUris.get_uri('shelflistitems-list', req=request,
                                        template=True, absolute=True),
        'templated': True
    }
    resp.data['catalogApi']['_links'] = OrderedDict(sorted(links.items()))
    return Response(resp.data)


class ShelflistItemList(SimpleGetMixin, SimpleView):
    """
    Paginated list of items. Use the 'page' query parameter to specify
    the page number.
    """
    serializer_class = serializers.ShelflistItemSerializer
    ordering = None
    filter_fields = ['call_number', 'call_number_type', 'barcode', 
                     'status_code', 'shelf_status', 'suppressed',
                     'inventory_notes', 'flags', 'due_date', 'inventory_date']
    resource_name = 'shelflistItems'

    def get_queryset(self):
        return solr.Queryset().filter(type='Item', 
                    location_code=self.kwargs['code']).order_by('call_number_type', 'call_number_sort', 'volume_sort', 'copy_number')


# Add SimplePutMixin, SimplePatchMixin before SimpleGetMixin to enable
# Put/Patch behavior. Disabled for now for security in production.
class ShelflistItemDetail(SimplePutMixin, SimplePatchMixin, SimpleGetMixin, 
                          SimpleView):
    """
    Retrieve one item.
    """
    queryset = solr.Queryset().filter(type='Item')
    serializer_class = serializers.ShelflistItemSerializer
    multi = False
    parser_classes = (JSONPatchParser, JSONParser)
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)
    resource_name = 'shelflistItems'
    
    def get_object(self):
        queryset = self.get_queryset()
        try:
            obj = queryset.filter(id=self.kwargs['shelflistitem_id'])[0]
        except IndexError:
            raise Http404
        else:
            return obj


class LocationList(api_views.LocationList):
    serializer_class = serializers.LocationSerializer


class LocationDetail(api_views.LocationDetail):
    serializer_class = serializers.LocationSerializer


class ItemList(api_views.ItemList):
    serializer_class = serializers.ItemSerializer


class ItemDetail(api_views.ItemDetail):
    serializer_class = serializers.ItemSerializer


class FirstItemPerLocationList(api_views.FirstItemPerLocationList):
    def get_page_data(self, queryset, request):
        data = super(FirstItemPerLocationList, self).get_page_data(queryset,
                                                                   request)
        for item in data['_embedded']['items']:
            l_code = item['locationCode']
            this_id = item['id']
            item['_links']['shelflistItem'] = {
                'href': ShelflistAPIUris.get_uri(
                'shelflistitems-detail', req=request, absolute=True,
                v=self.api_version, code=l_code, id=this_id)
            }
            r = RedisObject('shelflistitem_manifest', l_code)
            item['rowNumber'] = r.get_index(this_id)

        return data

