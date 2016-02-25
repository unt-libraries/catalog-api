'''
Custom pagination for the Sierra API. This implements pages as resource
lists, uses JSON API conventions for links, and adds some metadata
about each page.
'''

from django.conf import settings

from rest_framework import serializers
from rest_framework import pagination
from rest_framework.templatetags.rest_framework import replace_query_param

from . import serializers as sierra_api_serializers

class NextPageField(serializers.Field):
    page_field = 'page'
    
    def to_native(self, value):
        if not value.has_next():
            return None
        page = value.next_page_number()
        request = self.context.get('request')
        url = request and request.build_absolute_uri() or ''
        return {'href': replace_query_param(url, self.page_field, page)}


class PreviousPageField(serializers.Field):
    page_field = 'page'

    def to_native(self, value):
        if not value.has_previous():
            return None
        page = value.previous_page_number()
        request = self.context.get('request')
        url = request and request.build_absolute_uri() or ''
        return {'href': replace_query_param(url, self.page_field, page)}


class PageLinksSerializer(sierra_api_serializers.LinksSerializer):
    next = NextPageField(source='*')
    prev = PreviousPageField(source='*')


class SierraApiPaginationSerializer(serializers.Serializer):
    totalCount = serializers.Field(source='paginator.count')
    countPerPage = serializers.SerializerMethodField('get_count_per_page')
    totalPages = serializers.Field(source='paginator.num_pages')
    page = serializers.Field(source='number')
    links = serializers.Field()
    
    _options_class = pagination.PaginationSerializerOptions
    results_field = 'results'
    
    def __init__(self, *args, **kwargs):
        super(SierraApiPaginationSerializer, self).__init__(*args, **kwargs)
        object_serializer = self.opts.object_serializer_class
        
        if object_serializer.root_label:
            results_field = object_serializer.root_label
        else:
            results_field = self.results_field

        if 'context' in kwargs:
            context_kwarg = {'context': kwargs['context']}
        else:
            context_kwarg = {}

        self.fields['links'] = PageLinksSerializer(source='*', 
                                instance=kwargs['instance'],
                                **context_kwarg)

        self.fields[results_field] = object_serializer(source='object_list', 
                                                       **context_kwarg)

    def get_count_per_page(self, obj):
        try:
            cpp = settings.REST_FRAMEWORK['PAGINATE_BY']
        except (AttributeError, KeyError):
            cpp = 10
        try:
            paginate_by_param = settings.REST_FRAMEWORK['PAGINATE_BY_PARAM']
        except (AttributeError, KeyError):
            pass
        else:
            params = self.context['request'].QUERY_PARAMS
            try:
                cpp = int(params[paginate_by_param])
            except KeyError:
                pass
        return cpp