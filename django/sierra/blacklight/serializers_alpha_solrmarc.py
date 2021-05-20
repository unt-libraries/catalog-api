"""
Contains DRF serializers for alpha-solrmarc bl-suggest resources.
"""

from __future__ import absolute_import
from collections import OrderedDict

from api.simpleserializers import SimpleSerializer
from utils.camel_case import render, parser

import logging

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')

class AsmSuggestionsSerializer(SimpleSerializer):
    fields = OrderedDict()
    fields['heading_display'] = {'type': 'str'}
    fields['thing_type'] = {'type': 'str'}
    fields['facet_values'] = {'type': 'str', 'derived': True}
    fields['search_string'] = {'type': 'str', 'derived': True}

    def render_field_name(self, field_name):
        ret_val = field_name
        if field_name[0] != '_':
            ret_val = render.underscoreToCamel(field_name)
        return ret_val

    def restore_field_name(self, field_name):
        return parser.camel_to_underscore(field_name)

    def process_facet_values(self, value, rec):
        """
        Comes from the `this_facet_values` field in the Solr doc,
        converted to a dict. Values are stored in the (multi-valued)
        Solr field as "field:Value".
        """
        ret = {}
        for facet in rec.get('this_facet_values', []):
            field, val = facet.split(':', 1)
            ret[field] = val
        return ret

    def process_search_string(self, value, rec):
        """
        The `search_string` is just a string that can be used as the
        search query for this suggestion. Comes from facet values, but
        doesn't include the facet field.
        """
        val = rec.get('this_facet_values', [])
        return ' '.join([f.split(':', 1)[1] for f in val])
