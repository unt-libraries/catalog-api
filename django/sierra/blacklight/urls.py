"""
Add URL patterns for Django/DRF for `asm-search-suggestions` and
`asm-browse-suggestions` resources, for bl-suggest.
"""

from django.conf.urls import url
from django.core.urlresolvers import reverse_lazy
from django.views.generic import RedirectView

from rest_framework.urlpatterns import format_suffix_patterns
from api.uris import APIUris
from blacklight import views_alpha_solrmarc as views
from blacklight.uris_alpha_solrmarc import AsmUris

urlpatterns = [
    url(r'^$', RedirectView.as_view(url=reverse_lazy('api-root'),
                                    permanent=True)),
    url(APIUris.get_urlpattern('api-root', v=r'1'), views.api_root,
        name='api-root'),
    url(AsmUris.get_urlpattern('asm-search-suggestions-list', v=r'1'),
        views.AsmSearchSuggestionsList.as_view(),
        name='asm-search-suggestions-list'),
    url(AsmUris.get_urlpattern('asm-browse-suggestions-list', v=r'1'),
        views.AsmBrowseSuggestionsList.as_view(),
        name='asm-browse-suggestions-list')
]

urlpatterns = format_suffix_patterns(urlpatterns, allowed=['json', 'html'])
