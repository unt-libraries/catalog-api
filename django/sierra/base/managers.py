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
        filter_ = filter_params['filter']
        order_by = filter_params['order_by']
        distinct = filter_params.get('distinct', False)
        qset = self.all()
        if filter_:
            qset = qset.filter(helpers.reduce_filter_kwargs(filter_))
        if order_by:
            qset = qset.order_by(*order_by)
        if distinct:
            qset = qset.distinct()
        return qset

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
    def _do_date_range(self, start, end):
        def _make_fpath(prefix, is_del=False):
            fname = 'deletion_date_gmt' if is_del else 'record_last_updated_gmt'
            return '__'.join(([prefix] if prefix else []) + [fname])

        def _make_filter(prefix, start, end, is_del=False):
            fparts, fpath = {}, _make_fpath(prefix, is_del)
            if start:
                fparts['__'.join([fpath, 'gte'])] = start
            if end:
                fparts['__'.join([fpath, 'lte'])] = end
            return fparts

        options = self.options
        distinct = False
        is_del = options.get('is_deletion', False)
        model_is_rec_md = self.model._meta.object_name == 'RecordMetadata'
        other_rt_paths = options.get('other_updated_rtype_paths', [])

        prefix = '' if model_is_rec_md else 'record_metadata'
        filter_ = [_make_filter(prefix, start, end, is_del)]
        order_by = [_make_fpath(prefix, is_del)]

        if not is_del:
            for rt_path in options.get('other_updated_rtype_paths', []):
                distinct = True
                prefix = '{}__record_metadata'.format(rt_path)
                filter_.append(_make_filter(prefix, start, end))
        return {'filter': filter_, 'order_by': order_by, 'distinct': distinct}

    def updated_date_range(self):
        """
        Filter by a date range for last_updated. Options should contain
        datetime objects date_range_from and date_range_to. Options
        *may* contain `is_deletion`, which is a boolean that indicates
        whether or not this requires "last deleted" rather than
        "last updated".

        Note: This filter forces time-boundaries at midnight on the
        given start date and 11:59:59 PM on the end date. Even if you
        provide different times. The TZ will also be forced to UTC.

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
        def _prep_date(date, combine_time):
            dt = datetime.combine(date, combine_time)
            dt = tz.make_aware(dt, tz.get_default_timezone())
            return dt.astimezone(tz.utc)

        options = self.options
        midnight = time(0, 0)
        pre_midnight = time(23, 59, 59, 99)
        date_from = _prep_date(options['date_range_from'], midnight)
        date_to = _prep_date(options['date_range_to'], pre_midnight)
        return self._do_date_range(date_from, date_to)

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
        This value should be a timezone aware datetime obj.
        """
        return self._do_date_range(self.options['latest_time'], None)

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
        Filters records by item location (code).
        """
        options = self.options
        locations = self.options['location_code']
        f_prefix, distinct = '', False
        if self.model._meta.object_name == 'RecordMetadata':
            f_prefix = 'itemrecord__'
        elif self.model._meta.object_name == 'BibRecord':
            f_prefix = 'bibrecorditemrecordlink__item_record__'
            distinct = True
        filter_ = [{'{}location_id__in'.format(f_prefix): locations}]
        return {'filter': filter_, 'order_by': ['pk'], 'distinct': distinct}
