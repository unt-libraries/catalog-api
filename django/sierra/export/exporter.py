"""
Exporter module. Contains the class definition for the Exporter class
and a few subclasses to help you create your own Exporters to export
data out of Sierra.

Define your subclasses in a separate module and then hook it into your
project using the EXPORTER_MODULE_REGISTRY Django setting.
"""

from __future__ import unicode_literals

import logging
import sys
import traceback
from collections import OrderedDict, namedtuple

from django.db.models import F
from django.utils import timezone as tz
from django.conf import settings

from utils import helpers
from utils import dict_merge
from base import models as sierra_models
from .models import ExportInstance, ExportType, Status


class ExportError(Exception):
    pass


class Exporter(object):
    """
    Exporter class. Subclass this to define your export jobs. Your
    class names should match exactly 1:1 with the ExportType.codes
    you've defined.
    
    Your Exporter subclasses should at the very LEAST override the
    export_records() method to define how the export works.

    If your exports are syncing Sierra with an external index or data
    store, such as Solr, then each time you load data you want to make
    sure you're also deleting records from the index that were deleted
    from Sierra (or--if you're excluding records from your external
    store based on other criteria, like record suppression, you want
    to make sure those get deleted correctly as well). In this case,
    you should override delete_records() to define how to do those
    deletions. You should also make sure you define the deletion_filter
    attribute, which provides the filter for deletions. By default, we
    get deletions from RecordMetadata--this is because, when a record
    is deleted from Sierra, all data gets removed except for the row in
    RecordMetadata. So your deletion_filter should assume you're
    filtering from the POV of the RecordMetadata model. Alternatively,
    you can simply override the get_deletions method and do whatever
    you want.
    
    The deletion_filter attribute should contain a list of
    dictionaries, where each dict contains keyword filter criteria that
    should be ANDed together and each dict group then gets ORed to
    produce the final filter.
    
    If your export job doesn't need to handle deletions, simply don't
    specify a deletion_filter in your subclass--the get_deletions
    method will return None and you won't have to worry about it.
    
    In certain cases you might want to apply some sort of base filter
    when getting records, aside from whatever export filter is used.
    For instance, if you want to filter out suppressed records. To do
    this, you can set the record_filter attribute. It uses the same
    format as deletion_filter. If you do this, make sure your deletion
    filter will select the correct records for deletion--e.g., if you
    filter out suppressed records from getting loaded into Solr, then
    you'll want to check for newly suppressed records in your deletion
    filter along with deleted records.
    
    Use select_related and prefetch_related attributes to specify what
    related objects should be prefetched or preselected when records
    are fetched from the DB. These have a MASSIVE performance benefit
    if your job uses a lot of data in related tables, so use them!
    (See the Django docs on the Queryset API for info about
    prefetch_related and select_related.)
    
    Note about max_rec_chunk and max_del_chunk. These are used by
    the tasks.py module that breaks up record loads and delegates tasks
    out to Celery. The max_rec_chunk size is the size of the chunk used
    for record loads and the max_del_chunk size is the size of chunk
    used for deletions. Depending on your export, how much
    you're loading into memory at once (e.g. with prefetch_related),
    and how many parallel chunks you're allowing, chunks greater than
    5000 could use up all your memory. Keep an eye on it when you're
    testing your export jobs, and adjust those numbers accordingly.
    Needs will also likely vary by environment; if developing via the
    Docker env, your dev containers may lack power and memory that you
    have in production. You can override these settings for ANY
    Exporter class via the settings file or the .env file. The values
    set in the class end up serving as the defaults, which you can
    override if you want to on an env-specific basis. See the base
    settings module's EXPORTER_MAX_*_CONFIG settings for more info.
    """

    record_filter = []
    deletion_filter = []
    prefetch_related = []
    select_related = None
    max_rec_chunk = 3000
    max_del_chunk = 1000
    model = None
    app_name = 'export'

    def __init__(self, instance_pk, export_filter, export_type, options=None,
                 log_label=''):
        """
        Arguments: instance_pk is the pk for the export_instance
        attached to the export job; export_filter is the export_filter
        id string for this job; export_type is the export_type id
        string for this job; options is an optional dictionary
        containing specs for export_filter (date range, record range,
        etc.) Log_label is the label used in log messages to show the
        source of the message.
        """
        max_rc_override = settings.EXPORTER_MAX_RC_CONFIG.get(export_type, 0)
        max_dc_override = settings.EXPORTER_MAX_DC_CONFIG.get(export_type, 0)
        self.max_rec_chunk = max_rc_override or type(self).max_rec_chunk
        self.max_del_chunk = max_dc_override or type(self).max_del_chunk
        self.instance = ExportInstance.objects.get(pk=instance_pk)
        self.status = 'unknown'
        self.export_filter = export_filter
        self.export_type = export_type
        self.options = options or {}
        self.log_label = log_label if log_label else self.__class__.__name__
        if export_filter == 'last_export':
            try:
                latest = ExportInstance.objects.filter(
                    export_type=self.export_type, 
                    status__in=['success', 'done_with_errors']
                ).order_by(
                    '-timestamp'
                )[0]
            except IndexError:
                raise ExportError('This export type has never been run '
                                  'successfully before or does not exist. '
                                  'There is no last-updated date to use for '
                                  'this job.')
            self.options['latest_time'] = latest.timestamp
        # set up our loggers for this process
        self.logger = logging.getLogger('exporter.file')
        self.console_logger = logging.getLogger('sierra.custom')

    def _base_get_records(self, model, filters, is_deletion=False,
                          select_related=None, prefetch_related=None,
                          fail_on_zero=False):
        """
        This method is now deprecated; use `get_filtered_queryset`
        instead. A warning will be logged in the export log if you use
        this method.
        """
        msg = ('The `export.Exporter._base_get_records` method is deprecated '
               'and will be removed in a future update. Use '
               '`get_filtered_queryset` instead.')
        self.log('Warning', msg)
        options = self.options.copy()
        options['is_deletion'] = is_deletion
        return self.get_filtered_queryset(
            model, self.export_filter, options, added_filters=filters,
            select_related=select_related, prefetch_related=prefetch_related,
        )

    @staticmethod
    def get_filtered_queryset(model, export_filter, filter_options,
                              added_filters=None, select_related=None,
                              prefetch_related=None):
        """
        Utility method for fetching a filtered queryset based on the
        provided args and kwargs.
        """
        prefetch_related = prefetch_related or []
        qs = model.objects.filter_by(export_filter, options=filter_options)
        
        if added_filters:
            q_filter = helpers.reduce_filter_kwargs(added_filters)
            qs = qs.filter(q_filter)

        if select_related and select_related is not None:
            qs = qs.select_related(*select_related)
        qs = qs.prefetch_related(*prefetch_related)
        return qs

    def log(self, type, message, label=''):
        """
        Generates a log item for this export.
        Takes parameters: type and message. Type should be 'Error',
        'Warning', or 'Info'; message is the log message.
        """
        label = label if label else self.log_label
        message = '[{}] {}'.format(label, message)
        getattr(self.logger, type.lower())(message)
        if type.lower() == 'warning' or type.lower() == 'error':
            if type.lower() == 'warning':
                self.instance.warnings = F('warnings') + 1
            else:
                self.instance.errors = F('errors') + 1
            self.instance.save()

    def log_error(self, e_msg):
        """
        Helper for logging errors, including logging a traceback out to
        the console, if applicable.
        """
        ex_type, ex, tb = sys.exc_info()
        self.console_logger.info(traceback.extract_tb(tb))
        self.log('Error', e_msg, self.log_label)

    def save_status(self):
        """
        Saves self.status to the database.
        """
        try:
            status = Status.objects.get(pk=self.status)
        except Status.DoesNotExist:
            message = 'Could not set export instance status to "{}": status '\
                      'not defined in database.'.format(self.status)
            self.log('Warning', message)
            status = Status.objects.get(pk='unknown')
        self.instance.status = status
        self.instance.save()

    def get_records(self, prefetch=True):
        """
        Return a data structure containing the queryset that will be
        processed via `export_records` for a particular job. If
        `prefetch` is True, `select_related` and `prefetch_related`
        are set on the queryset before it is returned; if False, then
        they are explicitly NOT set.

        The default behavior should work for most needs, but you may
        override this method on your Exporter subclass if you need
        custom behavior. Whatever is returned is what is passed to
        `export_records`, so if you have compound Exporters that work
        on multiple querysets, you can return a dict of querysets. Just
        make sure to handle prefetching appropriately.

        (Note: Celery tasks set up in `export.tasks` assume that
        `get_records` and `get_deletions` return a single queryset or a
        dict of querysets. Anything different will require custom
        tasks.)
        """
        options = self.options.copy()
        options['is_deletion'] = False
        sr = self.select_related if prefetch else None
        pf = self.prefetch_related if prefetch else None
        return self.get_filtered_queryset(
            self.model, self.export_filter, options,
            added_filters=self.record_filter, select_related=sr,
            prefetch_related=pf)

    def get_deletions(self):
        """
        Like `get_records`, but returns a queryset of objects that
        represent things that should be deleted. If you haven't set
        self.deletion_filter, then this will just return None.
        Prefetching is not used because it doesn't apply.
        """
        if self.deletion_filter:
            options = self.options.copy()
            options['is_deletion'] = True
            return self.get_filtered_queryset(
                sierra_models.RecordMetadata, self.export_filter, options,
                added_filters=self.deletion_filter)
        else:
            return None

    def export_records(self, records):
        """
        When passed a `records` structure (from the `get_records`
        method), this should export the records as necessary.

        A return value is optional; returns None by default. Regarding
        the return value:

        When running an export job over a record-set piecemeal using
        Celery tasks, whatever the `export_records` or `delete_records`
        methods return is compiled together and passed to the
        `final_callback` method (`vals` kwarg). The return values from
        multiple parts of a single batch are compiled into one data
        structure using the `compile_vals` method, the result of which
        is passed to `final_callback`.

        E.g., you could pass meta-information needed for reporting or
        finalizing a batch by returning a dictionary of values, which
        is what the given `compile_vals` implementation assumes, and
        then use `final_callback` to finalize the batch and generate
        the report.
        """
        pass

    def delete_records(self, records):
        """
        Override this method in your subclasses only if you need to do
        deletions.
        
        When passed a queryset (e.g., from self.get_deletions()), this
        should delete the records as necessary.

        See the docstring for the `export_records` method for info
        about the optional return value.
        """
        pass

    def compile_vals(self, results):
        """
        Compile a single `vals` data value or structure given the list
        of `results` from running `export_records` and/or
        `delete_records` multiple times. Return the resulting `vals`
        value or structure, or None if there is no result.

        The method as implemented here assumes `results` is a list of
        dictionaries and attempts to merge them in a way that makes
        sense. Arrays are combined, and nested dicts are recursively
        merged.

        Override this to provide custom behavior for merging specific
        return values or data structures.
        """
        vals = {}
        for item in results:
            if isinstance(item, dict):
                vals = dict_merge(vals, item)
        return vals or None

    def final_callback(self, vals=None, status='success'):
        """
        Override this method in your subclasses if you need to provide
        something that runs once at the end of an export job that's
        been broken up into tasks.
        """
        pass


class ToSolrExporter(Exporter):
    """
    Exporter type for helping export records out to Solr.

    To use: first, subclass this type. In your subclass, override the
    `index_config` class attribute. It should contain a list (or tuple)
    of Index objects, each of which wraps a Haystack SearchIndex object
    that your exporter outputs to. Initialize each Index object by
    passing a name (string) you want to use to reference that index,
    the Haystack SearchIndex class to wrap, and the Haystack connection
    name (string) to use to connect to that index.

    When you instantiate an object using your subclass, you'll gain
    access to an `indexes` property. This is an OrderedDict allowing
    you to reference instantiated index objects by name.

    Note that the Index class monkey-patches a couple of methods onto
    the Haystack SearchIndex class when it spawns a new instance:
    `do_update` defines how an index update is done (e.g. if called
    from the exporter `export_records` method), and `do_delete` defines
    how to delete a record from the index (e.g. if called from the
    exporter `delete_records` method). You can customize these--or add
    your own--by subclassing the Index class in your subclass and then
    using your subclasses Index class in `index_config`.

    Instance methods on ToSolrExporter are defined for basic export,
    delete, and commit operations. Essentially, each of these loops
    through the `indexes` instances and calls `do_update`, `do_delete`,
    or `commit` on each one, in order. For more complex behavior, you
    can override these in your subclass.
    """
    class Index(namedtuple('Index', ['name', 'indexclass', 'conn'])):

        def do_update(self, instance, records):
            instance.update(commit=False, queryset=records)

        def do_delete(self, instance, records):
            instance.delete(commit=False, queryset=records)

        def spawn_instance(self, parent_name):
            nclassname = str('{}->{}'.format(parent_name, self.name))
            nclass_attrs = {
                '_config': self,
                'do_update': lambda s, recs: s._config.do_update(s, recs),
                'do_delete': lambda s, recs: s._config.do_delete(s, recs)
            }
            new_class = type(nclassname, (self.indexclass,), nclass_attrs)
            return new_class(using=self.conn)

    index_config = tuple()

    @classmethod
    def spawn_indexes(cls, parent_name='Exporter'):
        return OrderedDict(
            (i.name, i.spawn_instance(parent_name)) for i in cls.index_config
        )

    @property
    def indexes(self):
        try:
            self._indexes = self._indexes
        except AttributeError:
            self._indexes = type(self).spawn_indexes(self.export_type)
        return self._indexes

    def handle_error(self, obj_str, error):
        if obj_str == 'ERROR':
            raise error
        obj_info = '' if obj_str == 'WARNING' else '{} '.format(obj_str)
        msg = '{} update skipped due to error: {}'.format(obj_info, error)
        self.log('Warning', msg)

    def export_records(self, records):
        for index in self.indexes.values():
            index.do_update(records)

        for index in self.indexes.values():
            for obj_str, e in index.last_batch_errors:
                self.handle_error(obj_str, e)

    def delete_records(self, records):
        for index in self.indexes.values():
            index.do_delete(records)

    def commit_indexes(self):
        for name, index in self.indexes.items():
            self.log('Info', 'Committing {} updates to Solr...'.format(name))
            index.commit()

    def final_callback(self, vals=None, status='success'):
        self.commit_indexes()


class MetadataToSolrExporter(ToSolrExporter):
    """
    Base class for creating exporters to export simple Sierra
    "metadata" to Solr: Locations, Itypes, Ptypes, Material Types, etc.
    """
    class Index(ToSolrExporter.Index):

        def do_update(self, instance, records):
            instance.reindex(commit=False, queryset=records)

    index_config = tuple()

    def get_records(self, prefetch=False):
        return self.model.objects.all()

    def get_deletions(self):
        return None


class CompoundMixin(object):
    """
    Mixin for helping define Compound exporter jobs.

    If you have an exporter that needs to call other exporters in order
    to, e.g., index records in multiple indexes, use this mixin to help
    manage how you access and work with the child exporters.

    To use: first, include the mixin in your class definition (before
    the main Exporter class). Then override the `children_config` class
    attribute in your subclass. It should be a tuple of Child objects,
    where each defines a child exporter. Initialize each as appropriate
    depending on the subclass.

    When you instantiate an object using your subclass, you'll gain
    access to a `children` property. This is an OrderedDict allowing
    you to reference instantiated children exporter objects by name.

    Note that the Child class patches new attributes onto the exporter.
    By default, the Child config object can be accessed via a `_config`
    attribute. Subclasses may patch their own attributes (via the
    `Child.get_patched_class_attrs` method).
    """

    class Child(object):

        def __init__(self, name, export_type_code=None, expclass=None):
            self.name = name
            self.export_type_code = export_type_code or name
            self._expclass = expclass

        @property
        def expclass(self):
            if self._expclass is None:
                export_type = ExportType.objects.get(pk=self.export_type_code)
                self._expclass = export_type.get_exporter_class()
            return self._expclass

        def derive_rel_list(self, exporter, which_rel):
            return getattr(exporter, which_rel, [])

        def get_patched_class_attrs(self):
            """
            Implement this method in subclasses if additional attrs
            need to be attached to the patched exporter class.
            """
            return {'_config': self}

        def spawn_instance(self, parent_cls, parent_instance_pk,
                           parent_export_filter, parent_export_type,
                           parent_options):
            new_cls_name = str('{}->{}'.format(parent_cls.__name__, self.name))
            new_cls_attrs = self.get_patched_class_attrs()
            new_expclass = type(new_cls_name, (self.expclass,), new_cls_attrs)
            return new_expclass(parent_instance_pk, parent_export_filter,
                                parent_export_type,
                                options=parent_options.copy())

    children_config = tuple()

    @classmethod
    def spawn_children(cls, parent_args):
        return OrderedDict(
            (c.name, c.spawn_instance(cls, *parent_args))
                for c in cls.children_config
        )

    @property
    def children(self):
        try:
            self._children = self._children
        except AttributeError:
            args = (self.instance.pk, self.export_filter, self.export_type,
                    self.options)
            self._children = type(self).spawn_children(args)
        return self._children

    @staticmethod
    def combine_lists(*lists):
        """
        This is a helper method for combining and deduplicating entries
        from multiple lists, returning one sorted, flattened list.
        """
        combined_dupes = sorted([item for l in lists for item in l])
        return OrderedDict.fromkeys(combined_dupes).keys()

    def combine_rels_from_children(self, which_rel, which_children=None):
        """
        This is a helper method for combining lists of relations (like
        select_related or prefetch_related) from 1+ children.
        """
        children = which_children or self.children.values()
        rel_lists = [c._config.derive_rel_list(c, which_rel) for c in children]
        return self.combine_lists(*rel_lists)

    def get_records_from_children(self, deletions=False, prefetch=True,
                                  which_children=None):
        """
        This is a helper method that triggers either `get_records` or
        `get_deletions` on 1+ children. Returns a dict mapping each
        child's name to the set of records it returned.
        """
        records = {}
        for child in which_children or self.children.values():
            if deletions:
                records[child._config.name] = child.get_deletions()
            else:
                records[child._config.name] = child.get_records(prefetch)
        return records

    def do_op_on_children(self, operation, records, which_children=None):
        """
        This is a helper method that triggers an operation method
        (`export_records`, or `delete_records`) on 1+ children. If
        `records` is a dict (e.g., generated by the
        `get_records_from_children` method), then the appropriate
        record set is sent to the appropriate child exporter;
        otherwise, `records` is sent to each child.
        """
        vals = {}
        for child in which_children or self.children.values():
            op = getattr(child, operation)
            if isinstance(records, dict):
                child_rset = records.get(child._config.name, [])
            else:
                child_rset = records
            vals[child._config.name] = op(child_rset)
        return vals

    def compile_vals_from_children(self, results):
        """
        This is a helper method for compiling a list of export or
        deletion return values (`results`), aka "vals", from children
        via each child's `compile_vals` method. Results are compiled
        only for each child that has a key in at least one results
        item.
        """
        vals = {}
        for result in results:
            for name, rvals in (result or {}).items():
                cvals = vals.get(name, None)
                vals[name] = self.children[name].compile_vals([cvals, rvals])
        return vals

    def do_final_callback_on_children(self, vals, status, which_children=None):
        """
        This is a helper method that triggers the final_callback method
        on 1+ children, passing the appropriate vals to each call.
        """
        vals = vals or {}
        for child in which_children or self.children.values():
            child_vals = vals.get(child._config.name, None)
            child.final_callback(vals=child_vals, status=status)


class BatchExporter(CompoundMixin, Exporter):
    """
    Base class for writing exporters that need multiple recordsets
    and/or need to run multiple other exporters.
    """
    Child = CompoundMixin.Child
    children_config = tuple()

    def get_records(self, prefetch=True):
        return self.get_records_from_children(deletions=False,
                                              prefetch=prefetch)

    def get_deletions(self):
        return self.get_records_from_children(deletions=True)

    def export_records(self, records):
        """
        Export the given set of `records`, which should be a dict of
        recordsets from one or more children. Note that the
        `export_records` method for each child runs even if there is no
        queryset for that child in `records.`
        """
        return self.do_op_on_children('export_records', records)

    def delete_records(self, records):
        """
        Delete the given set of `records`, which should be a dict of
        recordsets from one or more children. Note that the
        `delete_records` method for each child runs even if there is no
        queryset for that child in `records.`
        """
        return self.do_op_on_children('delete_records', records)

    def compile_vals(self, results):
        return self.compile_vals_from_children(results)

    def final_callback(self, vals=None, status='success'):
        self.do_final_callback_on_children(vals, status)


class AttachedRecordExporter(CompoundMixin, Exporter):
    """
    Base class for creating exporters that export a main set of records
    plus one or more sets of attached records.

    The base Child config class for exporters of this type defines a
    base `derive_records_from_parent` method. In subclasses, this
    method should derive child records given a record from the main,
    parent exporter's record set. The default behavior just returns the
    parent_record.

    Example: BibsAndAttached is a Compound exporter that exports a set
    of bib records to Solr along with the items and holdings attached
    to each bib record in that set. The `derive_records...` method for
    the ItemChild type takes a bib record (model instance) and returns
    the list of attached items. The `derive_recordsets_from_parent`
    method on the AttachedRecordExporter object then compiles all the
    recordsets together.

    Because this implementation fetches attached records *during* the
    `export_records` run, and NOT during `get_records`, exporters of
    this type are extremely memory-intensive. The `prefetch_related`
    attribute is implemented as a property that calculates appropriate
    values from ALL children. Hence, the `rel_prefix` Child config
    attribute: this should be defined on attached children config
    classes, telling the exporter what prefix should be placed before
    each field in select_related and prefetch_related for that child,
    relative to the parent.
    """
    class Child(CompoundMixin.Child):
        rel_prefix = None

        def derive_records_from_parent(self, parent_record):
            return [parent_record]

        def derive_rel_list(self, exporter, which_rel):
            base_list = getattr(exporter, which_rel, [])
            if self.rel_prefix:
                return ['{}__{}'.format(self.rel_prefix, r) for r in base_list]
            return base_list

    children_config = tuple()

    @property
    def main_child(self):
        return self.children.items()[0][1]

    @property
    def attached_children(self):
        return [c[1] for c in self.children.items()[1:]]

    @property
    def select_related(self):
        """
        With main and attached records, using select_related generally
        only applies to the main (parent) child record type; attached
        records are related via a base M2M relationship, so those
        automatically become part of prefetch_related.
        """
        return self.main_child.select_related

    @property
    def prefetch_related(self):
        """
        With main and attached records, prefetch_related lists can be
        generated by combining the select_related lists for attached
        children and prefetch_related lists for all children.
        """
        try:
            self._prefetch_related = self._prefetch_related
        except AttributeError:
            att_sr = self.combine_rels_from_children('select_related',
                                                     self.attached_children)
            all_pr = self.combine_rels_from_children('prefetch_related')
            self._prefetch_related = self.combine_lists(all_pr, att_sr)
        return self._prefetch_related

    def derive_recordsets_from_parent(self, parent_recordset):
        """
        Given the `parent_recordset` (which should be the queryset from
        the main child's `get_records` method), collect the recordsets
        from attached children by passing each parent record to the
        `derive_records_from_parent` on each child's config class.
        """
        rsets = {}
        for record in parent_recordset:
            for name, child in self.children.items():
                rset = rsets.get(name, [])
                rset.extend(child._config.derive_records_from_parent(record))
                rsets[name] = rset
        return {k: list(set(v)) for k, v in rsets.items()}

    @property
    def deletion_filter(self):
        return self.main_child.deletion_filter

    def compile_vals(self, results):
        return self.compile_vals_from_children(results)

    def export_records(self, records):
        """
        Export the given set of `records`, which is a single queryset
        from the main child's `get_records` method. Records to pass to
        each attached child are derived from the parent set via
        `derive_recordsets_from_parent`. Each child's `export_records`
        method will run, even if passed an empty set.
        """
        rsets = self.derive_recordsets_from_parent(records)
        return self.do_op_on_children('export_records', rsets)

    def delete_records(self, records):
        """
        Delete the given set of `records` from the main child. A parent
        record being deleted doesn't necessarily mean the attached
        children should be deleted, so the `delete_records` method on
        the main child is the only one that runs.
        """
        return self.do_op_on_children('delete_records', records,
                                      which_children=[self.main_child])

    def final_callback(self, vals=None, status='success'):
        self.do_final_callback_on_children(vals, status)
