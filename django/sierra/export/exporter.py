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
    testing your export jobs, and adjust those numbers accordingly for
    your subclass.
    """

    class ValsManager(object):
        """
        Internal Exporter class used to manage `vals` data dicts, which
        are used to pass information from / between exporter tasks--
        assuming a scenario where a given batch of records is broken
        into multiple pieces, is handled as a serial or parallel series
        of `export_records` (or `delete_records`) calls, and ends with
        a call to `final_callback.`

        This functions as a temporary wrapper for a vals dict during an
        individual method call, which handles getting, setting, and
        merging values on the dict in a standard way, designed to work
        with compound exporters (with children) seamlessly. The dict
        itself (in `self.vals`) is what should be passed to the next
        process method call.

        One major function of this class is to merge `vals` dicts
        together from processes that have run serially or in parallel.
        Because different types of variables can be merged in different
        ways (appending to lists or overwriting them, keeping values
        unique or not, etc.), you may need to subclass this in your
        exporter subclass to deal with special cases. The default
        behavior is to merge lists together and keep values unique.
        Override the `merge_var` and/or private `_merge_*` methods to
        define different behavior. `_merge_*` methods specify how
        various data types are supposed to be merged, and are called
        via `merge_var`.

        To use:

        Use the `spawn_vals_manager` method on the parent Exporter
        class, passing in the current `vals` dict, to generate a new
        ValsManager instance. The `vals` dict for a given exporter is
        then treated as a namespaced key/value store. By default, the
        namespace is the name of the parent Exporter class. Use the
        `get`, `set`, `update`, `extend`, and `append` VarManager
        methods to get and modify vars within a particular namespace
        (default is the VarManager instance's default namespace).

        It's up to the parent Exporter class to make sure that the
        appropriate methods are used for the appropriate data types and
        that merges are handled appropriately. Remember that parallel
        tasks collect all `vals` dicts from the tasks and then provide
        them to the `final_callback` method as a list, while serial
        tasks pass `vals` dicts from task to task, accumulating values,
        and pass the final vals dict (from the last task) to the
        `final_callback` method. Also bear in mind that merges BY
        DEFAULT are idempotent, because sequences are merged and
        uniqueness is enforced. But if you override that behavior, then
        you may lose idempotency. (And you could end up with a bunch of
        duplicate values if you merge carelessly.)
        """
        def __init__(self, namespace, vals):
            """
            Initialize a new ValsManager using the given `namespace` as
            the default and the given `vals` data structure.

            If `vals` is None or empty, then a new `vals` dict is
            initialized. If `vals` is a list, then it's treated as a
            list of `vals` dicts, and they're all merged together.
            """
            self.default_namespace = namespace
            self.vals = {}
            if isinstance(vals, (list, tuple)):
                for v in vals:
                    self.merge(v)
            elif vals:
                self.vals = vals

        @staticmethod
        def _merge_sets(a, b):
            return set(a) | b

        @staticmethod
        def _merge_sequences(a, b, unique):
            if unique:
                return type(b)([i for i in a] + [i for i in b if i not in a])
            return type(b)([i for i in a] + [i for i in b])

        @staticmethod
        def _merge_dicts(a, b):
            new_dict = dict(a).copy()
            new_dict.update(b)
            return new_dict

        @staticmethod
        def _merge_others(a, b):
            return b

        def merge_var(self, varname, to_var, from_var, seq_merge=True,
                      unique=True):
            """
            Merge one vals var member with another of the same type,
            returning the merged value. `seq_merge` should be True if
            sequences should be merged together; if False, the new
            sequence (`from_var`) completely overwrites the old.
            `unique` should be True if merged sequences should retain
            only unique values (no duplicates).

            Override this if you need custom merging behavior. Add new
            `_merge` methods if you need to merge new types of values.
            `varname` is provided as a convenience in case you need to
            add custom merging behavior based on specific varnames.
            """
            from_type = type(from_var)
            to_var = to_var or from_type()
            if isinstance(from_var, (list, tuple)):
                if seq_merge:
                    return self._merge_sequences(to_var, from_var, unique)
                return from_type(i for i in from_var)
            try:
                do = getattr(self, '_merge_{}s'.format(from_type.__name__))
            except AttributeError:
                do = self._merge_others
            return do(to_var, from_var)

        def merge(self, from_):
            """
            Merge another vals dict (`from_`) with this one. Note that
            any namespaces in the foreign dict are copied into 
            """
            for namespace, variables in from_.items():
                self.init_namespace(namespace)
                for varname, from_item in variables.items():
                    to_item = self.vals[namespace].get(varname, None)
                    merged = self.merge_var(varname, to_item, from_item)
                    self.vals[namespace][varname] = merged

        def init_namespace(self, namespace):
            namespace = namespace or self.default_namespace
            self.vals[namespace] = self.vals.get(namespace, {})
            return namespace

        def set(self, varname, new_value, namespace=None):
            """
            Set a vals var member to a specific value.
            """
            namespace = self.init_namespace(namespace)
            self.vals[namespace][varname] = new_value

        def update(self, varname, new_dict, namespace=None):
            """
            Update a dict-like vals var member using a dict.
            """
            nspace = self.init_namespace(namespace)
            self.vals[nspace][varname] = self.vals[nspace].get(varname, {})
            self.vals[nspace][varname].update(new_dict)

        def extend(self, varname, extend_with, namespace=None, unique=False):
            """
            Extend a sequence-like vals var member with a list of new
            values. Pass `unique=True` if values in the resulting
            sequence must be unique.
            """
            namespace = self.init_namespace(namespace)
            merge_to = self.vals[namespace].get(varname, type(extend_with)())
            extend_with = type(merge_to)(extend_with)
            merged = self.merge_var(varname, merge_to, extend_with, True,
                                    unique)
            self.vals[namespace][varname] = merged

        def append(self, varname, value, namespace=None, unique=False):
            """
            Append a value to a sequence-like vals var member. Pass
            `unique=True` if values in the resulting sequence must be
            unique.
            """
            self.extend(varname, [value], namespace, unique)

        def get(self, varname, namespace=None):
            """
            Get the value of the given val member (`varname`).
            """
            namespace = self.init_namespace(namespace)
            return self.vals[namespace].get(varname, None)

    record_filter = []
    deletion_filter = []
    prefetch_related = []
    select_related = None
    max_rec_chunk = 3000
    max_del_chunk = 1000
    parallel = True
    model = ''
    app_name = 'export'

    def __init__(self, instance_pk, export_filter, export_type, options={},
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
        self.instance = ExportInstance.objects.get(pk=instance_pk)
        self.status = 'unknown'
        self.export_filter = export_filter
        self.export_type = export_type
        self.options = options
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


    def _base_get_records(self, model, filters, select_related=None,
                          prefetch_related=[], fail_on_zero=False):
        """
        Default method for getting records using self.export_filter.
        Returns the queryset. Generally you won't want to override this
        in your subclass--override get_records and get_deletions
        instead.
        
        Note that this assumes you're working with one of the main III
        record types and have the benefit of the RecordMetadata table.
        If this is not the case, you'll need to write your own
        get_records and get_deletions methods that don't use this.
        """
        try:
            # do base record filter.
            records = model.objects.filter_by(self.export_filter, 
                        options=self.options)
            
            # do additional filters, if provided.
            if filters:
                q_filter = helpers.reduce_filter_kwargs(filters)
                records = records.filter(q_filter)

            # apply select_ and prefetch_related
            if select_related and select_related is not None:
                records = records.select_related(*select_related)
            if prefetch_related or prefetch_related is None:
                records = records.prefetch_related(*prefetch_related)
                
        except Exception as e:
            model_name = model._meta.object_name
            raise ExportError('Could not retrieve records from {} via '
                    'export filter {} using options '
                    '{}, default filter {}: {}.'
                    ''.format(model_name, self.export_filter, self.options,
                              self.record_filter, e))
        if (fail_on_zero and len(records) == 0):
            raise ExportError('0 records retrieved from {} via export '
                    'filter {} using options '
                    '{}, default filter {}.'
                    ''.format(model_name, self.export_filter, self.options, 
                                 self.filter))
        return records

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

    def get_records(self):
        """
        Should return a full queryset or list of record objects based
        on the export filter passed into the class at initialization.
        If your export job is using one of the main III record types as
        its primary focus, then you can/should use the
        _base_get_records() method to make this simple. Otherwise 
        you'll have to override this (get_records) in your subclass.
        """
        try:
            in_records = self._base_get_records(self.model,
                            self.record_filter,
                            select_related=self.select_related,
                            prefetch_related=self.prefetch_related)
        except ExportError:
            raise
        return in_records

    def get_deletions(self):
        """
        Like get_records, but returns a queryset of objects that
        represent things that should be deleted. If you haven't set
        self.deletion_filter, then this will just return None.
        """
        if self.deletion_filter:
            return self._base_get_records(sierra_models.RecordMetadata,
                                          self.deletion_filter)
        else:
            return None

    @classmethod
    def spawn_vals_manager(cls, vals, namespace=None):
        namespace = namespace or cls.__name__
        return cls.ValsManager(namespace, vals)

    def export_records(self, records, vals=None):
        """
        Override this method in your subclasses.
        
        When passed a queryset (e.g., from self.get_records()), this
        should export the records as necessary. Totally up to you
        how to do this. Since export jobs are broken up into tasks via
        celery, and we may need to pass information from a task to a
        task or from multiple tasks to the final_callback method. Do
        this using vals. Here's how it works.
        
        If self.parallel is True, this is a job where task chunks can
        be run in parallel. In this case your export_record and
        delete_record jobs should return a dictionary of values. These
        will get passed in a list to final_callback, where you can then
        compile the info from all tasks and do something with it.
        Individual export_ and delete_ jobs won't have access to vals
        from other jobs running in parallel.
        
        If self.parallel is False, then export_record and delete_record
        job chunks will run one after the other. The vals dict will get
        passed from one chunk to the next, so each time it runs it can
        modify what's in vals. Finally, vals will be passed to
        final_callback. In this case the final_callback method will
        only have access to the vals passed to it from the last export
        or delete job chunk that ran (e.g. as a dict rather than a list
        of dicts).
        """
        vals_manager = self.spawn_vals_manager(vals)
        return vals_manager.vals

    def delete_records(self, records, vals=None):
        """
        Override this method in your subclasses only if you need to do
        deletions.
        
        When passed a queryset (e.g., from self.get_deletions()), this
        should delete the records as necessary. Like export_records,
        you may take/return a dictionary of values to pass info from
        task to task.
        """
        vals_manager = self.spawn_vals_manager(vals)
        return vals_manager.vals

    def final_callback(self, vals=None, status='success'):
        """
        Override this method in your subclasses if you need to provide
        something that runs once at the end of an export job that's
        been broken up into tasks.
        """
        vals_manager = self.spawn_vals_manager(vals)
        return vals_manager.vals


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

    def export_records(self, records, vals=None):
        vals_manager = self.spawn_vals_manager(vals)
        try:
            for index in self.indexes.values():
                index.do_update(records)
        except Exception as e:
            self.log_error(e)

        for index in self.indexes.values():
            for obj_str, e in index.last_batch_errors:
                msg = '{} update skipped due to error: {}'.format(obj_str, e)
                self.log('Warning', msg)
        return vals_manager.vals

    def delete_records(self, records, vals=None):
        vals_manager = self.spawn_vals_manager(vals)
        try:
            for index in self.indexes.values():
                index.do_delete(records)
        except Exception as e:
            self.log_error(e)
        return vals_manager.vals

    def commit_indexes(self):
        for name, index in self.indexes.items():
            self.log('Info', 'Committing {} updates to Solr...'.format(name))
            index.commit()

    def final_callback(self, vals=None, status='success'):
        vals_manager = self.spawn_vals_manager(vals)
        self.commit_indexes()
        return vals_manager.vals


class MetadataToSolrExporter(ToSolrExporter):
    """
    Base class for creating exporters to export simple Sierra
    "metadata" to Solr: Locations, Itypes, Ptypes, Material Types, etc.
    """
    class Index(ToSolrExporter.Index):

        def do_update(self, instance, records):
            instance.reindex(commit=False, queryset=records)

    index_config = tuple()

    def get_records(self):
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
    where each defines a child exporter. Initialize each by passing the
    exporter name--i.e., the export_type identifier string for that
    exporter type.

    When you instantiate an object using your subclass, you'll gain
    access to a `children` property. This is an OrderedDict allowing
    you to reference instantiated children exporter objects by name.

    Note that the Child class monkey-patches a new method onto each
    spawned exporter instance, `derive_records`. This method defines
    how a child exporter derives its input records when given a record
    from the main, parent exporter's record_set. Your subclass should
    also subclass Child if it needs to override the default behavior
    (which just returns a list containing the parent_record).
    Example: BibsAndAttached is a Compound exporter that exports a set
    of bib records to Solr along with the items and holdings attached
    to each bib record in that set. The `derive_records` method for the
    ItemsChild type takes a bib record (model instance) and returns the
    list of attached items. The parent exporter exposes a
    `generate_record_sets` method, which uses the `derive_records`
    method on each child exporter to compile those record_sets.
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

        def derive_records(self, parent_record):
            return [parent_record]

        def spawn_instance(self, parent_cls, parent_instance_pk,
                           parent_export_filter, parent_export_type,
                           parent_options):
            nclassname = str('{}->{}'.format(parent_cls.__name__, self.name))
            nclass_attrs = {
                '_config': self,
                'derive_records': lambda s, rec: s._config.derive_records(rec)
            }
            new_class = type(nclassname, (self.expclass,), nclass_attrs)
            return new_class(parent_instance_pk, parent_export_filter,
                             parent_export_type, options=parent_options)

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

    def generate_record_sets(self, record_set):
        child_rsets = {name: [] for name in self.children.keys()}
        for record in record_set:
            for name, child in self.children.items():
                child_rsets[name].extend(child.derive_records(record))
        return {k: list(set(v)) for k, v in child_rsets.items()}


class AttachedRecordExporter(CompoundMixin, Exporter):
    """
    Base class for creating exporters that export a main set of records
    plus one or more sets of attached records.
    """
    Child = CompoundMixin.Child
    children_config = tuple()

    @property
    def main_child(self):
        return self.children.items()[0][1]

    @property
    def attached_children(self):
        return [c[1] for c in self.children.items()[1:]]

    @property
    def prefetch_related(self):
        return self.main_child.prefetch_related

    @property
    def select_related(self):
        return self.main_child.select_related

    @property
    def deletion_filter(self):
        return self.main_child.deletion_filter

    def export_records(self, records, vals=None):
        vals_manager = self.spawn_vals_manager(vals)
        vals_manager.set('job_type', 'export')
        record_sets = self.generate_record_sets(records)
        for name, child in self.children.items():
            rset = record_sets[name]
            child_vals = child.export_records(rset, vals_manager.vals)
            vals_manager = self.spawn_vals_manager(child_vals)
        return vals_manager.vals

    def delete_records(self, records, vals=None):
        vals_manager = self.spawn_vals_manager(vals)
        vals_manager.set('job_type', 'delete')
        return self.main_child.delete_records(records, vals_manager.vals)

    def final_callback(self, vals=None, status='success'):
        vals_manager = self.spawn_vals_manager(vals)
        if vals_manager.get('job_type') == 'export':
            for name, child in self.children.items():
                child_vals = child.final_callback(vals_manager.vals, status)
                vals_manager = self.spawn_vals_manager(child_vals)
            return vals_manager.vals
        else:
            return self.main_child.final_callback(vals_manager.vals, status)
