from __future__ import absolute_import
import six.moves.urllib.request
import six.moves.urllib.parse
import six.moves.urllib.error
from collections import OrderedDict
import jsonpatch
import jsonpointer

from django.http import Http404
from django.conf import settings

from rest_framework import views
from rest_framework.response import Response
from rest_framework.templatetags.rest_framework import replace_query_param
from rest_framework import status

from api import exceptions
from utils import load_class
from utils.camel_case import render

import logging

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class SimpleView(views.APIView):
    """
    My attempt at a base for a set of generic classes that provide
    super simplified methods compared to DRF's. When subclassing this,
    set multi = False for single-object views.
    """
    queryset = None
    filter_class = load_class(
        settings.REST_FRAMEWORK['DEFAULT_FILTER_BACKENDS'][0])
    serializer_class = None
    ordering = []
    filter_fields = []
    api_version = 1
    resource_name = 'resources'
    multi = True

    def get_queryset(self):
        return self.queryset

    def get_serializer(self, **kwargs):
        serializer = getattr(self, 'serializer', None)
        force_refresh = kwargs.pop('force_refresh', False)
        if force_refresh == True or serializer is None:
            self.serializer = self.serializer_class(**kwargs)
        return self.serializer

    def get_object_id(self, obj):
        return obj.id

    def update_object(self, request, new_data):
        obj = self.get_object()
        serializer = self.get_serializer(force_refresh=True, instance=obj,
                                         context={'request': request,
                                                  'view': self}, data=new_data)
        if serializer.is_valid():
            serializer.save()
        else:
            msg = ('Attempting to save the object produced the following '
                   'errors. {}'.format(' '.join(serializer.errors)))
            raise exceptions.BadUpdate(detail=msg)

        url = request.build_absolute_uri()

        content = {'status': 200,
                   'details': 'The resource at {} was updated with the '
                   'fields or sub-resource content provided in the request.'
                   ''.format(url),
                   'links': {
                       'self': {
                           'href': url,
                           'id': self.get_object_id(obj)
                       }
                   }
                   }

        return Response(content, status=status.HTTP_200_OK)


class SimpleGetMixin(object):
    """
    Simple mixin for a get view that paginates data. Instead of using a
    special serializer and Django Pagination objects, this uses the
    more standard offset/limit parameters.
    """

    def get_default_paging_params(self):
        """
        Get a dict containing default pagination settings.
        """
        return {
            'limit_qp': settings.REST_FRAMEWORK.get('PAGINATE_BY_PARAM',
                                                    'limit'),
            'offset_qp': settings.REST_FRAMEWORK.get('PAGINATE_PARAM',
                                                     'offset'),
            'max_limit': settings.REST_FRAMEWORK.get('MAX_PAGINATE_BY', 500),
            'default_limit': settings.REST_FRAMEWORK.get('PAGINATE_BY', 10)
        }

    def paginate_queryset(self, queryset, request):
        """
        Paginate the given queryset based on the given request.

        Uses the appropriate offset and limit values to return a page
        of results from the given queryset. The return value is a dict
        containing relevant data: total number of results (unpaged),
        active offset for this page (or None if the offset is greater
        than the total), limit used in calculating results for this
        page, the row number of the last valid result on this page
        (`end_row`), and a list of this page's results (`results`).
        """
        if getattr(self, '_cached_page', None) is None:
            offset, limit = self.get_offset_limit_from_request(request)
            results = queryset[offset:offset+limit]
            total = queryset.count()
            end_row = self.get_page_end_row(total, offset, limit)
            offset = None if end_row is None else offset
            self._cached_page = {
                'total': total,
                'offset': offset,
                'limit': limit,
                'end_row': end_row,
                'results': results
            }
        return self._cached_page

    def get_offset_limit_from_request(self, request):
        """
        Get (offset, limit) values from the given request, using
        defaults if necessary.
        """
        params = self.get_default_paging_params()
        offset = int(request.query_params.get(params['offset_qp'], 0))
        limit = int(request.query_params.get(params['limit_qp'],
                                             params['default_limit']))
        limit = params['max_limit'] if limit > params['max_limit'] else limit
        return (offset, limit)

    def get_page_end_row(self, total, offset, limit):
        """
        Determine the end row value for a page of results based on the
        given `total`, `offset`, and `limit` values.
        """
        if total <= 0:
            return None
        try_end_row = offset + limit - 1
        return total - 1 if try_end_row > total - 1 else try_end_row

    def get_prev_page_url(self, url, page):
        """
        Calculate the URL for the previous page of results, based on
        the current `page` and base `url`. (Where `page` is the output
        from the `paginate_queryset` method.) Returns None if this is
        the first page.
        """
        params = self.get_default_paging_params()
        offset, limit = page['offset'], page['limit']
        if offset is None or offset == 0:
            prev_offset = None
        else:
            prev_offset = offset - limit if offset - limit >= 0 else 0
        if prev_offset is None:
            return None
        return six.moves.urllib.parse.unquote(replace_query_param(url, params['offset_qp'],
                                                                  prev_offset))

    def get_next_page_url(self, url, page):
        """
        Calculate the URL for the next page of results, based on
        the current `page` and base `url`. (Where `page` is the output
        from the `paginate_queryset` method.) Returns None if this is
        the last page.
        """
        params = self.get_default_paging_params()
        if page['offset'] is None or page['end_row'] == page['total'] - 1:
            next_offset = None
        else:
            next_offset = page['offset'] + page['limit']
        if next_offset is None:
            return None
        return six.moves.urllib.parse.unquote(replace_query_param(url, params['offset_qp'],
                                                                  next_offset))

    def get_page_data(self, queryset, request):
        """
        Return data for an API page based on the given queryset and
        request.
        """
        page = self.paginate_queryset(queryset, request)
        self._cached_page = None
        page_data = OrderedDict()
        page_data['totalCount'] = page['total']
        if page['offset'] is not None:
            page_data['startRow'] = page['offset']
        if page['end_row'] is not None:
            page_data['endRow'] = page['end_row']

        url = request.build_absolute_uri()
        page_data['_links'] = OrderedDict()
        page_data['_links']['self'] = {'href': url}
        prev_page = self.get_prev_page_url(url, page)
        if prev_page is not None:
            page_data['_links']['previous'] = {'href': prev_page}
        next_page = self.get_next_page_url(url, page)
        if next_page is not None:
            page_data['_links']['next'] = {'href': next_page}

        resource_name = render.underscoreToCamel(self.resource_name)
        resource_list = self.get_serializer(instance=page['results'],
                                            force_refresh=True,
                                            context={'request': request,
                                                     'view': self}).data
        if resource_list:
            page_data['_embedded'] = {resource_name: resource_list}
        return page_data

    def get(self, request, *args, **kwargs):
        """
        HTTP `get` method for this view. Given the `request`, `args`,
        and `kwargs`, return an appropriately paginated Response obj. 
        """
        if self.multi:
            queryset = self.get_queryset()
            queryset = self.filter_class().filter_queryset(request, queryset,
                                                           self)
            data = self.get_page_data(queryset, request)
        else:
            obj = self.get_object()
            data = self.get_serializer(instance=obj, force_refresh=True,
                                       context={'request': request, 'view': self}).data
        return Response(data)


class SimplePutMixin(object):
    """
    Simple mixin to provide a PUT method for a SimpleView-based object.
    """

    def put(self, request, *args, **kwargs):
        ret_val = self.update_object(request, request.data)
        return ret_val


class SimplePatchMixin(object):
    """
    Simple mixin to provide a PATCH method, which requires requests to
    be sent using a valid json-patch document. (See IETF RFC 6902 for
    more info.)
    """

    def patch(self, request, *args, **kwargs):
        patch = request.data
        obj = self.get_object()
        serializer = self.get_serializer(instance=obj,
                                         context={'request': request,
                                                  'view': self})
        try:
            new_data = patch.apply(serializer.data)
        except (jsonpatch.JsonPatchException,
                jsonpointer.JsonPointerException) as e:
            msg = 'Could not apply json-patch to object: {}'.format(e)
            raise exceptions.BadUpdate(detail=msg)

        ret_val = self.update_object(request, new_data)
        return ret_val
