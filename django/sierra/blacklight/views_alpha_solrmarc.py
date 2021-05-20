from __future__ import absolute_import
from collections import OrderedDict

from django.conf import settings

from rest_framework.decorators import api_view
from rest_framework.response import Response

from utils import solr
from shelflist import views as shelflist_views
from api.simpleviews import SimpleView, SimpleGetMixin
from blacklight.uris_alpha_solrmarc import AsmUris
from blacklight.serializers_alpha_solrmarc import AsmSuggestionsSerializer
from blacklight.filters_alpha_solrmarc import AsmSuggestionsFilter

import logging

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


@api_view(('GET',))
def api_root(request):
    resp = shelflist_views.api_root(request)
    links = resp.data['catalogApi']['_links']
    links['asm-search-suggestions'] = {
        'href': AsmUris.get_uri('asm-search-suggestions-list', req=request,
                                 template=False, absolute=True)
    }
    links['asm-browse-suggestions'] = {
        'href': AsmUris.get_uri('asm-browse-suggestions-list', req=request,
                                 template=False, absolute=True)
    }
    resp.data['catalogApi']['_links'] = OrderedDict(sorted(links.items()))
    return Response(resp.data)


class AsmSuggestionsList(SimpleGetMixin, SimpleView):
    """
    Base class to be used for both `browse` and `search` suggestion
    types. Super simplified in order to maximize response speed: you
    can include a limit parameter to specify how many results you want,
    but no pagination info is included otherwise. One query is made to
    Solr to bring back the entire set that's requested.
    """
    serializer_class = AsmSuggestionsSerializer
    ordering = None
    filter_fields = ['heading_type']
    filter_class = AsmSuggestionsFilter

    def get_page_data(self, queryset, request):
        # for paging, we only use the 'limit'
        limit_p = settings.REST_FRAMEWORK.get('PAGINATE_BY_PARAM', 'limit')
        max_limit = settings.REST_FRAMEWORK.get('MAX_PAGINATE_BY', 500)
        default_limit = settings.REST_FRAMEWORK.get('PAGINATE_BY', 5)
        limit = int(request.query_params.get(limit_p, default_limit))
        limit = max_limit if limit > max_limit else limit

        qs = queryset.set_raw_params({'rows': limit})

        resource_list = self.get_serializer(instance=queryset[0:limit],
                                            force_refresh=True,
                                            context={'request': request,
                                                     'view': self}).data
        return {
            '_embedded': {
                self.resource_name: resource_list
            }
        }


class AsmSearchSuggestionsList(AsmSuggestionsList):
    resource_name = 'asmSearchSuggestions'
    suggest_type = 'search'

    def get_queryset(self):
        return solr.Queryset(using='bl-suggest', search_handler='suggest')


class AsmBrowseSuggestionsList(AsmSuggestionsList):
    resource_name = 'asmBrowseSuggestions'
    suggest_type = 'browse'

    def get_queryset(self):
        return solr.Queryset(using='bl-suggest', search_handler='browse')
    
