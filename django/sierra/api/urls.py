from django.conf.urls import patterns, url
from django.conf import settings
from django.core.urlresolvers import reverse_lazy
from django.views.generic import RedirectView

from rest_framework.urlpatterns import format_suffix_patterns
from . import views
from .uris import APIUris

urlpatterns = patterns('',
    url(r'^$', RedirectView.as_view(url=reverse_lazy('api-root'), permanent=True)),
    url(APIUris.get_urlpattern('api-root', v=r'1'), views.api_root, 
        name='api-root'),
    url(APIUris.get_urlpattern('apiusers-list', v=r'1'),
        views.APIUserList.as_view(), name='apiusers-list'),
    url(APIUris.get_urlpattern('apiusers-detail', v=r'1',
                                id=r'(?P<id>[^?/]+)'),
        views.APIUserDetail.as_view(), name='apiusers-detail'),
    url(APIUris.get_urlpattern('items-list', v=r'1'), views.ItemList.as_view(),
        name='items-list'),
    url(APIUris.get_urlpattern('items-detail', v=r'1', id=r'(?P<id>[0-9]+)'),
        views.ItemDetail.as_view(), name='items-detail'),
    url(APIUris.get_urlpattern('bibs-list', v=r'1'), views.BibList.as_view(),
        name='bibs-list'),
    url(APIUris.get_urlpattern('bibs-detail', v=r'1', id=r'(?P<id>[0-9]+)'),
        views.BibDetail.as_view(), name='bibs-detail'),
    url(APIUris.get_urlpattern('marc-list', v=r'1'), views.MarcList.as_view(),
        name='marc-list'),
    url(APIUris.get_urlpattern('marc-detail', v=r'1', id=r'(?P<id>[0-9]+)'),
        views.MarcDetail.as_view(), name='marc-detail'),
    url(APIUris.get_urlpattern('eresources-list', v=r'1'), 
        views.EResourceList.as_view(), name='eresources-list'),
    url(APIUris.get_urlpattern('eresources-detail', v=r'1', 
                                id=r'(?P<id>[0-9]+)'), 
        views.EResourceDetail.as_view(), name='eresources-list'),
    url(APIUris.get_urlpattern('locations-list', v=r'1'),
        views.LocationList.as_view(), name='locations-list'),
    url(APIUris.get_urlpattern('locations-detail', v=r'1',
                                code=r'(?P<code>[a-z0-9]+)'),
        views.LocationDetail.as_view(), name='locations-detail'),
    url(APIUris.get_urlpattern('itemtypes-list', v=r'1'),
        views.ItemTypesList.as_view(), name='itemtypes-list'),
    url(APIUris.get_urlpattern('itemtypes-detail', v=r'1',
                                code=r'(?P<code>[a-z0-9]+)'),
        views.ItemTypesDetail.as_view(), name='itemtypes-detail'),
    url(APIUris.get_urlpattern('itemstatuses-list', v=r'1'),
        views.ItemStatusesList.as_view(), name='itemstatuses-list'),
    url(APIUris.get_urlpattern('itemstatuses-detail', v=r'1',
                                code=r'(?P<code>[a-z0-9]+)'),
        views.ItemStatusesDetail.as_view(), name='itemstatuses-detail'),
    url(APIUris.get_urlpattern('callnumbermatches-list', v=r'1'),
        views.CallnumbermatchesList.as_view(), name='callnumbermatches-list'),
    url(APIUris.get_urlpattern('firstitemperlocation-list', v=r'1'),
        views.FirstItemPerLocationList.as_view(), 
        name='firstitemperlocation-list'),
)

urlpatterns = format_suffix_patterns(urlpatterns, allowed=['json', 'html'])