from __future__ import absolute_import
import re
import logging

from pysolr import SolrError
from django.conf import settings
from rest_framework.filters import BaseFilterBackend

from api import exceptions
from blacklight import bl_suggest_alpha_solrmarc as suggest


# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class AsmSuggestionsFilter(BaseFilterBackend):
    """
    This is a majorly simplified filter backend for the alpha-solrmarc
    bl-suggest index.
    """
    reserved_params = [settings.REST_FRAMEWORK['PAGINATE_BY_PARAM'],
                       settings.REST_FRAMEWORK['SEARCH_PARAM']]

    def _normalize_search_string(self, query, for_browse=False):
        """
        This uses the `make_normalized_heading_string` suggest function
        to normalize the search query. BUT, quotes have to be handled
        such that they are included only if the person has entered a
        complete phrase in quotes. E.g.: "online audio" is sent to 
        Solr as-is, but -- "online aud -- is sent without the starting
        quotation mark. Or -- "online video" "romeo and -- is sent with
        the first phrase in quotes but not the second.
        """
        phrases, quote_phrase = [], ''
        for i, phrase in enumerate(query.split('"')):
            inside_quotes = i % 2
            phrase = suggest.make_normalized_heading_string(phrase, for_browse)
            if inside_quotes:
                quote_phrase = phrase
            else:
                if quote_phrase:
                    phrases.append('"{}"'.format(quote_phrase))
                    quote_phrase = ''
                if phrase:
                    phrases.append(phrase)
        if quote_phrase:
            phrases.append(quote_phrase)
        return ' '.join([p.strip() for p in phrases])

    def filter_queryset(self, request, queryset, view):
        q, fq = '*', []
        for key, val in dict(request.query_params).items():
            val = val if isinstance(val, (list, tuple)) else [val]
            if val:
                if key == settings.REST_FRAMEWORK['SEARCH_PARAM']:
                    browse = True if view.suggest_type == 'browse' else False
                    q = self._normalize_search_string(val[0], browse)
                    if q[-1] != '"':
                        q = '{}*'.format(q)
                    if browse:
                        q = '\ '.join(q.split(' '))
                elif key == 'fq':
                    fq.extend(val)
                elif key not in self.reserved_params:
                    fq.extend(['{}:{}'.format(key, v) for v in val])
        queryset = queryset.set_raw_params({'fq': fq, 'q': q})

        try:
            view.paginate_queryset(queryset, request)
        except SolrError as e:
            msg = ('Query raised Solr error. {}'.format(e))
            raise exceptions.BadQuery(detail=msg)
        return queryset
