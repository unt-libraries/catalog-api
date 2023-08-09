from __future__ import absolute_import
from __future__ import unicode_literals

import json

import ujson
from django.http.multipartparser import parse_header
from rest_framework.renderers import BaseRenderer
from rest_framework.utils import encoders
from six import text_type


class FullJSONRenderer(BaseRenderer):
    media_type = 'application/json'
    format = 'json'
    encoder_class = encoders.JSONEncoder
    ensure_ascii = True
    charset = None

    def render(self, data, accepted_media_type=None, renderer_context=None):
        '''
        Render `data` into JSON, using the faster ujson library for
        serialization unless indentation is requested (ujson does not
        support indentation).
        '''
        if data is None:
            return bytes()

        # If 'indent' is provided in the context, then pretty print the result.
        # E.g. If we're being called by the BrowsableAPIRenderer.
        renderer_context = renderer_context or {}
        indent = renderer_context.get('indent', None)

        if accepted_media_type:
            # If the media type looks like 'application/json; indent=4',
            # then pretty print the result.
            base_media_type, params = parse_header(
                accepted_media_type.encode('ascii'))
            indent = params.get('indent', indent)
            try:
                indent = max(min(int(indent), 8), 0)
            except (ValueError, TypeError):
                indent = None

            ret = json.dumps(data, cls=self.encoder_class,
                             indent=indent, ensure_ascii=self.ensure_ascii)
        else:
            ret = ujson.dumps(data, ensure_ascii=self.ensure_ascii)

        # On python 2.x json.dumps() returns bytestrings if ensure_ascii=True,
        # but if ensure_ascii=False, the return type is underspecified,
        # and may (or may not) be unicode.
        # On python 3.x json.dumps() returns unicode strings.
        if isinstance(ret, text_type):
            return bytes(ret.encode('utf-8'))
        return ret


class UnicodeFullJSONRenderer(FullJSONRenderer):
    ensure_ascii = False


class HALJSONRenderer(FullJSONRenderer):
    media_type = 'application/hal+json'
