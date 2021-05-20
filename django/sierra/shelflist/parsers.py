from __future__ import absolute_import
import jsonpatch
import jsonpointer

from rest_framework import parsers
from rest_framework.exceptions import ParseError


class JSONPatchParser(parsers.JSONParser):
    '''
    Adds JSON Patch media-type to JSONParser.
    '''
    media_type = 'application/json-patch+json'

    def parse(self, stream, media_type=None, parser_context=None):
        data = super(JSONPatchParser, self).parse(stream, media_type, 
                                                  parser_context)
        if not isinstance(data, (list, tuple)):
            raise ParseError('JSON-Patch parse error: json-patch doc should '
                             'be an array type--received {}.'
                             ''.format(type(data)))
        return jsonpatch.JsonPatch(data)
