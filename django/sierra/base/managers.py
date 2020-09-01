"""
Custom Managers for sierra base app models.
"""
from datetime import date, time, datetime

from django.db import models
from django.utils import timezone as tz

from utils import helpers


class CustomFilterManager(models.Manager):
    """
    A generic models.Manager class that provides the ability to set
    custom filters easily. Just create a child class, and then create a
    method with the same name as the filter that you pass to filter_by.
    Filter methods should return a dictionary with elements 'filter'
    and 'order_by.' Filter should be a list of kwarg dicts, such that
    each list element can be ORed together to create the final result
    set. For instance: [{a AND b AND c} OR {d} OR {e AND f}]. Order_by
    should be an array that can be passed as arguments to
    queryset.order_by().
    """
    options = {}

    def _apply_filter(self, filter_method):
        """
        Applies the filter_method and returns the filtered queryset.
        """
        filter_params = filter_method()
        filter = filter_params['filter']
        order_by = filter_params['order_by']
        set = self
        if filter:
            set = set.filter(helpers.reduce_filter_kwargs(filter))
        else:
            set = set.all()
        if order_by:
            set = set.order_by(*order_by)
        return set

    def filter_by(self, filter_method, options=None):
        """
        Fetches a set of records based on a filter string and any
        options you specify. Options should be a dictionary.
        """
        self.options = options or {}
        filter_method = getattr(self, filter_method)
        return self._apply_filter(filter_method)


class RecordManager(CustomFilterManager):
    """
    Defines some common filters that apply across multiple types of
    records from the Sierra database, such as the base record types
    (item, bib, patron, etc.)
    """

    def updated_date_range(self):
        """
        Filter by a date range for last_updated. Options should contain
        date_range_from and date_range_to, each of which are simply
        date objects. Options *may* contain `is_deletion`, which is a
        boolean that indicates whether or not this requires "last
        deleted" rather than "last updated".

        For things that are not deletions, Options may also contain a
        list, `other_updated_rtype_paths`, listing the paths to other
        record types where, if a record of that type that's attached to
        the main type was updated within the given range, then the main
        record should be included in the filtered queryset. For
        instance, updating an Item record attached to a Bib doesn't
        update the Bib's `record_last_updated_gmt` date, so if you need
        to get a list of Bibs where either the Bib or any attached item
        was updated within a certain date range, then include an
        `other_updated_rtype_paths` entry pointing to the item_record
        table.
        """
        def _make_fpath(prefix, is_del=False):
            fname = 'deletion_date_gmt' if is_del else 'record_last_updated_gmt'
            return '__'.join(([prefix] if prefix else []) + [fname])

        def _make_filter(prefix, date_from, date_to, is_del=False):
            fpath = _make_fpath(prefix, is_del)
            return {'__'.join([fpath, 'gte']): date_from,
                    '__'.join([fpath, 'lte']): date_to}

        options = self.options
        is_del = options.get('is_deletion', False)
        model_is_rec_md = self.model._meta.object_name == 'RecordMetadata'
        other_rt_paths = options.get('other_updated_rtype_paths', [])

        date_from = datetime.combine(options['date_range_from'], time(0, 0))
        date_from = tz.make_aware(date_from, tz.get_default_timezone())
        date_from = date_from.astimezone(tz.utc)
        date_to = datetime.combine(options['date_range_to'], 
                                   time(23, 59, 59, 99))
        date_to = tz.make_aware(date_to, tz.get_default_timezone())
        date_to = date_to.astimezone(tz.utc)

        prefix = '' if model_is_rec_md else 'record_metadata'
        filter_ = [_make_filter(prefix, date_from, date_to, is_del)]
        order_by = [_make_fpath(prefix, is_del)]

        if not is_del:
            for rt_path in options.get('other_updated_rtype_paths', []):
                prefix = '{}__record_metadata'.format(rt_path)
                filter_.append(_make_filter(prefix, date_from, date_to))
                order_by.append(_make_fpath(prefix))
        return {'filter': filter_, 'order_by': order_by}

    def record_range(self):
        """
        Filter by a III record number range. Options should contain
        record_range_from and record_range_to.
        """
        options = self.options
        record_from = options['record_range_from']
        record_to = options['record_range_to']
        if self.model._meta.object_name != 'RecordMetadata':
            prefix = 'record_metadata__'
        else:
            prefix = ''
        filter = [{
            '{}record_type__code'.format(prefix): record_from[0],
            '{}record_num__gte'.format(prefix): record_from[1:],
            '{}record_num__lte'.format(prefix): record_to[1:]
        }]
        order_by = ['{}record_num'.format(prefix)]
        return {'filter': filter, 'order_by': order_by}

    def last_export(self):
        """
        Filter by a latest updated datetime in options['latest_time'].
        """
        options = self.options
        latest_time = options['latest_time']
        if self.model._meta.object_name != 'RecordMetadata':
            prefix = 'record_metadata__'
        else:
            prefix = ''
        if options.get('is_deletion', False):
            filter = [{
                '{}deletion_date_gmt__gte'.format(prefix): latest_time
            }]
            order_by = ['{}record_last_updated_gmt'.format(prefix)]
        else:
            filter = [{
                '{}record_last_updated_gmt__gte'.format(prefix): latest_time
            }]
            order_by = ['{}deletion_date_gmt'.format(prefix)]
        return {'filter': filter, 'order_by': order_by}

    def full_export(self):
        """
        No filter.
        """
        if self.model._meta.object_name != 'RecordMetadata':
            prefix = 'record_metadata__'
        else:
            prefix = ''
        filter = None
        order_by = ['{}record_num'.format(prefix)]
        return {'filter': filter, 'order_by': order_by}

    def location(self):
        """
        Filters item records by location (code).
        """
        options = self.options
        location_code = self.options['location_code']
        if self.model._meta.object_name == 'RecordMetadata':
            f_prefix = 'itemrecord__'
            o_prefix = ''
        else:
            f_prefix = ''
            o_prefix = 'record_metadata__'
        filter = [{'{}location__code'.format(f_prefix): location_code}]
        order_by = ['{}__record_num'.format(o_prefix)]
        return {'filter': filter, 'order_by': order_by}
