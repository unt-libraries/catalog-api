"""
Add URL patterns for Django/DRF for `asm-search-suggestions` and
`asm-browse-suggestions` resources, for bl-suggest.
"""

from __future__ import absolute_import

from api.uris import APIUris
from blacklight import views_alpha_solrmarc as views
from blacklight.uris_alpha_solrmarc import AsmUris
from django.urls import re_path
from django.urls import reverse_lazy
from django.views.generic import RedirectView
from rest_framework.urlpatterns import format_suffix_patterns

urlpatterns = [
    re_path(r'^$', RedirectView.as_view(url=reverse_lazy('api-root'),
                                        permanent=True)),
    re_path(APIUris.get_urlpattern('api-root', v=r'1'), views.api_root,
            name='api-root'),
    re_path(AsmUris.get_urlpattern('asm-search-suggestions-list', v=r'1'),
            views.AsmSearchSuggestionsList.as_view(),
            name='asm-search-suggestions-list'),
    re_path(AsmUris.get_urlpattern('asm-browse-suggestions-list', v=r'1'),
            views.AsmBrowseSuggestionsList.as_view(),
            name='asm-browse-suggestions-list')
]

urlpatterns = format_suffix_patterns(urlpatterns, allowed=['json', 'html'])
