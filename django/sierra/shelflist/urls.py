from __future__ import absolute_import

from api.uris import APIUris
from django.urls import re_path, reverse_lazy
from django.views.generic import RedirectView
from rest_framework.urlpatterns import format_suffix_patterns

from . import views
from .uris import ShelflistAPIUris

urlpatterns = [
    re_path(r'^$', RedirectView.as_view(url=reverse_lazy('api-root'),
                                        permanent=True)),
    re_path(APIUris.get_urlpattern('api-root', v=r'1'), views.api_root,
            name='api-root'),
    re_path(APIUris.get_urlpattern('locations-list', v='1'),
            views.LocationList.as_view(), name='locations-list'),
    re_path(APIUris.get_urlpattern('locations-detail', v='1',
                                   code=r'(?P<code>[a-z\d]+)'),
            views.LocationDetail.as_view(), name='locations-detail'),
    re_path(ShelflistAPIUris.get_urlpattern('shelflistitems-list', v='1',
                                            code=r'(?P<code>[a-z\d]+)'),
            views.ShelflistItemList.as_view(), name='shelflistitems-list'),
    re_path(ShelflistAPIUris.get_urlpattern('shelflistitems-detail', v=r'1',
                                            code=r'(?P<code>[a-z\d]+)',
                                            id=r'(?P<shelflistitem_id>i\d+)'),
            views.ShelflistItemDetail.as_view(), name='shelflistitems-detail'),
    re_path(APIUris.get_urlpattern('items-list', v=r'1'),
            views.ItemList.as_view(), name='items-list'),
    re_path(APIUris.get_urlpattern('items-detail', v=r'1',
                                   id=r'(?P<id>i\d+)'),
            views.ItemDetail.as_view(), name='items-detail'),
    re_path(APIUris.get_urlpattern('firstitemperlocation-list', v=r'1'),
            views.FirstItemPerLocationList.as_view(),
            name='firstitemperlocation-list')
]

urlpatterns = format_suffix_patterns(urlpatterns, allowed=['json', 'html'])
