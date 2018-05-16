import urllib
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
    '''
    My attempt at a base for a set of generic classes that provide
    super simplified methods compared to DRF's. When subclassing this,
    set multi = False for single-object views.
    '''
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
    '''
    Simple mixin for a get view that paginates data. Instead of using a
    special serializer and Django Pagination objects, this uses the
    more standard offset/limit parameters.
    '''
    def paginate(self, queryset, request):
        # first get paging parameters.
        limit_p = settings.REST_FRAMEWORK.get('PAGINATE_BY_PARAM', 'limit')
        offset_p = settings.REST_FRAMEWORK.get('PAGINATE_PARAM', 'offset')
        max_limit = settings.REST_FRAMEWORK.get('MAX_PAGINATE_BY', 500)
        default_limit = settings.REST_FRAMEWORK.get('PAGINATE_BY', 10)
        offset = int(request.query_params.get(offset_p, 0))
        limit = int(request.query_params.get(limit_p, default_limit))
        limit = max_limit if limit > max_limit else limit
        page = queryset[offset:offset+limit]

        # make sure the end row num is not > the total count of the queryset
        total_count = queryset.count()
        if total_count > 0:
            end_row = offset + limit - 1
            end_row = total_count - 1 if end_row > total_count - 1 else end_row
        else:
            end_row = None
            offset = None

        url = request.build_absolute_uri()

        # determine the previous and next offsets for the previous and next
        # pages of results
        if offset is None or offset == 0:
            prev_offset = None
        else:
            prev_offset = offset - limit if offset - limit >= 0 else 0

        if offset is None or end_row == total_count - 1:
            next_offset = None
        else:
            next_offset = offset + limit

        prev_page = None
        if prev_offset is not None:
            prev_page = urllib.unquote(replace_query_param(url, offset_p, 
                                                           prev_offset))
        next_page = None
        if next_offset is not None:
            next_page = urllib.unquote(replace_query_param(url, offset_p,
                                                           next_offset))

        resource_name = render.underscoreToCamel(self.resource_name)
        resource_list = self.get_serializer(instance=page, force_refresh=True,
                                            context={'request': request,
                                                     'view': self}).data

        # page_data elements dictate what shows up in the API for page-level
        # metadata
        page_data = OrderedDict()
        page_data['totalCount'] = total_count
        if offset is not None:
            page_data['startRow'] = offset
        if end_row is not None:
            page_data['endRow'] = end_row
        page_data['_links'] = OrderedDict()
        page_data['_links']['self'] = {'href': url}
        if prev_page is not None:
            page_data['_links']['previous'] = {'href': prev_page}
        if next_page is not None:
            page_data['_links']['next'] = {'href': next_page}
        if resource_list:
            page_data['_embedded'] = {resource_name: resource_list}

        return page_data

    def get(self, request, *args, **kwargs):
        if self.multi:
            queryset = self.get_queryset()
            queryset = self.filter_class().filter_queryset(request, queryset,
                                                           self)
            data = self.paginate(queryset, request)
        else:
            obj = self.get_object()
            data = self.get_serializer(instance=obj, force_refresh=True,
                    context={'request': request, 'view': self}).data
        return Response(data)


class SimplePutMixin(object):
    '''
    Simple mixin to provide a PUT method for a SimpleView-based object.
    '''
    def put(self, request, *args, **kwargs):
        ret_val = self.update_object(request, request.data)
        return ret_val


class SimplePatchMixin(object):
    '''
    Simple mixin to provide a PATCH method, which requires requests to
    be sent using a valid json-patch document. (See IETF RFC 6902 for
    more info.)
    '''
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
