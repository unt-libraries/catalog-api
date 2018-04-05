from django.conf.urls import patterns, url
from django.conf import settings
from django.core.urlresolvers import reverse_lazy
from django.views.generic import RedirectView

from rest_framework.urlpatterns import format_suffix_patterns
from . import views
from api.uris import APIUris
from .uris import ShelflistAPIUris

urlpatterns = patterns('',
    url(r'^$', RedirectView.as_view(url=reverse_lazy('api-root'), permanent=True)),
    url(APIUris.get_urlpattern('api-root', v=r'1'), views.api_root, 
        name='api-root'),
    url(APIUris.get_urlpattern('locations-list', v='1'),
        views.LocationList.as_view(), name='locations-list'),
    url(APIUris.get_urlpattern('locations-detail', v='1',
                                code=r'(?P<code>[a-z0-9]+)'),
        views.LocationDetail.as_view(), name='locations-detail'),
    url(ShelflistAPIUris.get_urlpattern('shelflistitems-list', v='1',
                                        code=r'(?P<code>[a-z0-9]+)'),
        views.ShelflistItemList.as_view(), name='shelflistitems-list'),
    url(ShelflistAPIUris.get_urlpattern('shelflistitems-detail', v=r'1',
                                        code=r'(?P<code>[a-z0-9]+)',
                                        id=r'(?P<shelflistitem_id>[0-9]+)'),
        views.ShelflistItemDetail.as_view(), name='shelflistitems-detail'),
    url(APIUris.get_urlpattern('items-list', v=r'1'), views.ItemList.as_view(),
        name='items-list'),
    url(APIUris.get_urlpattern('items-detail', v=r'1', id=r'(?P<id>[0-9]+)'),
        views.ItemDetail.as_view(), name='items-detail'),
    url(APIUris.get_urlpattern('firstitemperlocation-list', v=r'1'),
        views.FirstItemPerLocationList.as_view(),
        name='firstitemperlocation-list')
)

urlpatterns = format_suffix_patterns(urlpatterns, allowed=['json', 'html'])