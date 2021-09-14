from __future__ import absolute_import

import logging
import sys, traceback

import pysolr

from django import template
from django.conf import settings
from django.contrib.auth.models import User
from django.utils import timezone as tz
from django.db import connections

from celery import Task, shared_task, chord, chain, result

from sierra.celery import app
from . import models as export_models
from utils.redisobjs import RedisObject
import six
from six.moves import range

# set up loggers
logger = logging.getLogger('sierra.custom')
exp_logger = logging.getLogger('exporter.file')

OPERATIONS = ('export', 'deletion')


# TASK HELPERS

def needs_database(job_func):
    """
    Decorator that ensures all defunct connections are closed before
    and after the decorated function runs.
    """
    def _do_close():
        for conn in connections.all():
            conn.close_if_unusable_or_obsolete()

    def _wrapper(*args, **kwargs):
        _do_close()
        ret_val = job_func(*args, **kwargs)
        _do_close()
        return ret_val

    _wrapper.__name__ = 'managed__{}'.format(job_func.__name__)
    return _wrapper


def spawn_exporter(inst_pk, exp_filter, exp_type, opts):
    """
    Spawn an Exporter obj using the given parameters.

    Note: `inst_pk` is the PK value of the ExportInstance ORM obj
    for a particular task. If one does not already exist, pass -1
    as the inst_pk, and one will be generated using the username in
    settings.EXPORT_AUTOMATED_USERNAME. (This is legacy behavior for
    Celery task scheduling.)
    """
    if inst_pk == -1:
        u = User.objects.get(username=settings.EXPORTER_AUTOMATED_USERNAME)
        instance = export_models.ExportInstance(
            export_filter_id=exp_filter,
            export_type_id=exp_type,
            timestamp=tz.now(),
            user=u,
            status_id='in_progress')
        instance.save()
        inst_pk = instance.pk
    et = export_models.ExportType.objects.get(pk=exp_type)
    exporter_class = et.get_exporter_class()
    return exporter_class(inst_pk, exp_filter, exp_type, opts,
                          log_label=settings.TASK_LOG_LABEL)


@shared_task
def export_dispatch(instance_pk, export_filter, export_type, options):
    """
    Trigger an export from an external source.
    """
    connections['default'].close_if_unusable_or_obsolete()
    exp = spawn_exporter(instance_pk, export_filter, export_type, options)
    args = (exp.instance.pk, exp.export_filter, exp.export_type, options)
    init_vals = exp.initialize()
    job = delegate_batch.s([], *args, chunk_id='header',
                           cumulative_vals=init_vals)
    job.link_error(do_final_cleanup.s(*args, status='errors',
                                      delegate_error=True,
                                      chunk_id='error-callback',
                                      cumulative_vals=init_vals))
    job.apply_async()


# WORKFLOW COMPONENTS

# Miscellaneous tasks

@shared_task
def optimize():
    """
    Celery task that simply runs "optimize" on all Solr indexes
    """
    logger = logging.getLogger('exporter.file')
    logger.info('Running optimization on all Solr indexes.')
    url_stack = []
    for index, options in six.iteritems(settings.HAYSTACK_CONNECTIONS):
        if options['URL'] not in url_stack:
            conn = pysolr.Solr(options['URL'], 
                               timeout=options['TIMEOUT'])
            logger.info('Optimizing {} index.'.format(index))
            conn.optimize()
            url_stack.append(options['URL'])
    logger.info('Done.')


# Export Workflow Components

class RecordSetBundler(object):
    """
    Helper class for defining exactly how sets of records should be
    packed into bundles for management via a JobPlan.
    """

    def pack(self, queryset, size):
        """
        Pack the supplied `queryset` into a series of bundles, given
        the maximum `size` for each bundle. Should return an iterable
        (or yield a generator).
        """
        raise NotImplementedError()

    def unpack(self, bundle, all_recs):
        """
        Given a queryset (`all_recs`) and a data structure representing
        a single packed bundle--return the qset for that bundle.
        """
        raise NotImplementedError()

    def get_bundle_count(self, bundle):
        """
        Return the number of qset records that the given bundle
        represents or contains.
        """
        return len(bundle)

    def get_bundle_offset(self, bundle, part_num, size):
        """
        Return the (0-based) index offset for a particular bundle,
        given the `bundle` itself, the 0-based part_num index, and the
        max chunk `size`.
        """
        return part_num * size

    def get_bundle_label(self, bundle):
        """
        Return a string representing the label for a bundle created via
        this bundler--used when generating labels in the celery logs.
        Default is 'records'. Should be a noun--whatever type of thing
        is being packed/unpacked, and should make sense in context of
        a sentence like, "Now processing {records} 1-2500."
        """
        return 'records'


class SierraExplicitKeyBundler(RecordSetBundler):

    def apply_sort(self, qset):
        if hasattr(qset.model, 'record_metadata'):
            return qset.order_by('record_metadata__record_last_updated_gmt',
                                 'pk')
        if hasattr(qset.model, 'record_last_updated_gmt'):
            return qset.order_by('record_last_updated_gmt', 'pk')
        return qset.order_by('pk')

    def pack(self, queryset, size):
        sorted_qset = self.apply_sort(queryset)
        manifest = [r['pk'] for r in sorted_qset.values('pk')]
        for start in range(0, len(manifest), size):
            yield manifest[start:start+size]

    def unpack(self, bundle, all_recs):
        sorted_qset = self.apply_sort(all_recs)
        return sorted_qset.filter(pk__in=bundle)


class SolrKeyRangeBundler(RecordSetBundler):

    def __init__(self, id_field):
        self.id_field = id_field
        super(SolrKeyRangeBundler, self).__init__()

    def apply_sort(self, qset):
        return qset.order_by(self.id_field)

    def get_id_by_indexnum(self, qset, indexnum):
        id_f = self.id_field
        prepped = self.apply_sort(qset).set_raw_params({'fl': id_f})
        prepped.page_by = 1
        return prepped[indexnum][id_f]

    def pack(self, queryset, size):
        total = queryset.count()
        for start in range(0, total, size):
            bundle_count = size if total > start + size else total - start
            end = start + bundle_count - 1
            start_id = self.get_id_by_indexnum(queryset, start)
            end_id = self.get_id_by_indexnum(queryset, end)
            yield {'start': start_id, 'end': end_id, 'count': bundle_count}

    def unpack(self, bundle, all_recs):
        idf = self.id_field
        filter_params = {'{}__gte'.format(idf): bundle['start_id'],
                         '{}__lte'.format(idf): bundle['end_id']}
        qs = self.apply_sort(all_recs).filter(**filter_params)
        qs.page_by = bundle['count']
        return qs

    def get_bundle_count(self, bundle):
        return bundle['count']


class JobPlan(object):
    """
    Helper class for breaking jobs into batches and chunks and caching
    the relevant info (i.e. the "plan") in Redis.
    """
    redis_job_chunk_key = 'exporter-job-chunk'
    redis_job_reg_key = 'exporter-job-registry'
    redis_job_totals_key = 'exporter-job-totals'

    class AlreadyRegistered(Exception):
        pass

    def __init__(self, exp, batch_size=200, operations=OPERATIONS):
        self.instance_pk = exp.instance.pk
        self.chunk_sizes = {
            op: exp.max_del_chunk if op == 'deletion' else exp.max_rec_chunk
                for op in operations
        }
        self.bundler = exp.bundler
        self.batch_size = batch_size
        self.operations = operations
        self._registry = {}
        self._totals = {}

    def get_batch_id(self, batch_num):
        return '{}:{:05d}'.format(self.instance_pk, batch_num)

    def get_chunk_id(self, batch_num, chunk_num, op, rset_name, part_num):
        batch_id = self.get_batch_id(batch_num)
        max_digits = len(str(self.batch_size - 1))
        padded_cnum = str(chunk_num).zfill(max_digits)
        if rset_name is None:
            return '{}-{}-{}-{}'.format(batch_id, padded_cnum, op, part_num)
        else:
            return '{}-{}-{}-{}-{}'.format(batch_id, padded_cnum, op,
                                           rset_name, part_num)

    def get_chunk_info(self, chunk_id):
        try:
            batch_id, padded_cnum, op, rset_name, pnum = chunk_id.split('-')
        except ValueError:
            batch_id, padded_cnum, op, pnum = chunk_id.split('-')
            rset_name = None
        return { 'batch_id': batch_id, 'chunk_num': int(padded_cnum),
                 'rset_name': rset_name, 'op': op, 'part_num': int(pnum) }

    def get_records_for_operation(self, exp, op, prefetch=True):
        if op == 'export':
            return exp.get_records(prefetch=prefetch)
        return exp.get_deletions()

    def get_method_for_operation(self, exp, op):
        return exp.export_records if op == 'export' else exp.delete_records

    def get_chunk_label(self, chunk_id):
        info = self.get_chunk_info(chunk_id)
        bundle = self.get_bundle(chunk_id)
        count = self.bundler.get_bundle_count(bundle)
        bundle_label = self.bundler.get_bundle_label(bundle)
        size = self.chunk_sizes[info['op']]
        start = self.bundler.get_bundle_offset(bundle, info['part_num'],
                                               size) + 1
        end = start + count - 1
        chunk_label = '{} {} - {} for {}'.format(bundle_label, start, end,
                                                 info['op'])
        if info['rset_name'] is not None:
            chunk_label = '`{}` {}'.format(info['rset_name'], chunk_label)
        return chunk_label

    def _get_reg_obj(self):
        return RedisObject(self.redis_job_reg_key, self.instance_pk)

    def _get_totals_obj(self):
        return RedisObject(self.redis_job_totals_key, self.instance_pk)

    def _get_chunk_obj(self, chunk_id):
        return RedisObject(self.redis_job_chunk_key, chunk_id)

    @property
    def registry(self):
        self._registry = self._registry or self._get_reg_obj().get() or {}
        return self._registry

    @property
    def totals(self):
        self._totals = self._totals or self._get_totals_obj().get() or {}
        return self._totals

    @property
    def unprocessed_chunks(self):
        res = []
        for chunk_list in self.registry.values():
            for chunk_id in chunk_list:
                try:
                    self.get_bundle(chunk_id)
                except ValueError:
                    pass
                else:
                    res.append(chunk_id)
        return sorted(res)

    def get_recordsets_iterable(self, recs):
        return list(recs.items()) if hasattr(recs, 'items') else [(None, recs)]

    def generate(self, exp):
        if self.registry:
            msg = ('Detected an existing registry for this ExportInstance. '
                   'You must instantiate a new JobPlan using a new '
                   'ExportInstance or use the `clear` method to clear the '
                   'existing one before generating a new one.')
            raise self.AlreadyRegistered(msg)

        total_chunks = 0
        total_recs_by_op_and_rset = {op: {} for op in self.operations}
        total_chunks_by_op = {op: 0 for op in self.operations}

        for (bt_id, ch_id, op, name, count, bundle) in self.pack_records(exp):
            self._registry[bt_id] = self._registry.get(bt_id, [])
            self._registry[bt_id].append(ch_id)
            chunk_obj = self._get_chunk_obj(ch_id)
            chunk_obj.set(bundle)

            total_chunks += 1
            rset_ct = total_recs_by_op_and_rset[op].get(name, 0)
            total_recs_by_op_and_rset[op][name] = rset_ct + count
            total_chunks_by_op[op] += 1

        total_recs_by_op = {op: sum(total_recs_by_op_and_rset[op].values())
                            for op in self.operations}

        self._totals = {
            'batches': len(list(self.registry.keys())),
            'chunks': total_chunks,
            'chunks_by_op': total_chunks_by_op,
            'records': sum(total_recs_by_op.values()),
            'records_by_op_and_rset': total_recs_by_op_and_rset
        }

        self._get_reg_obj().set(self._registry)
        self._get_totals_obj().set(self._totals)
        return self._registry

    def pack_records(self, exp):
        bt_num, ch_num = 0, 0
        for op in self.operations:
            recs = self.get_records_for_operation(exp, op, prefetch=False)
            chunk_size = self.chunk_sizes[op]
            for name, qs in self.get_recordsets_iterable(recs):
                pt_num = 0
                if qs is not None:
                    for bundle in self.bundler.pack(qs, chunk_size):
                        bt_id = self.get_batch_id(bt_num)
                        ch_id = self.get_chunk_id(bt_num, ch_num, op, name,
                                                  pt_num)
                        bundle_count = self.bundler.get_bundle_count(bundle)
                        yield (bt_id, ch_id, op, name, bundle_count, bundle)
                        pt_num += 1
                        if ch_num == (self.batch_size - 1):
                            ch_num = 0
                            bt_num += 1
                        else:
                            ch_num += 1

    def unpack_chunk(self, exp, chunk_id):
        bundle = self.get_bundle(chunk_id)
        info = self.get_chunk_info(chunk_id)
        rset_name, op = info['rset_name'], info['op']
        recs = self.get_records_for_operation(exp, op, prefetch=False)
        if rset_name is None:
            all_recs = recs.model.objects.all()
            return self.bundler.unpack(bundle, all_recs)
        all_recs = recs[rset_name].model.objects.all()
        return {rset_name: self.bundler.unpack(bundle, all_recs)}

    def get_bundle(self, chunk_id):
        data = self._get_chunk_obj(chunk_id).get()
        if data is None:
            raise ValueError('chunk_id {} is not valid'.format(chunk_id))
        return data

    def log_plan_summary(self, exp):
        def _plural(word, count, add='s'):
            return '{}{}'.format(word, '' if count == 1 else add)

        lines = ['JOB PLAN']
        for op in self.operations:
            op_totals = self.totals['records_by_op_and_rset'][op]
            rec_count = sum(op_totals.values())
            rec_label = _plural('record', rec_count)
            chunk_count = self.totals['chunks_by_op'][op]
            chunk_label = _plural('chunk', chunk_count)
            chunk_size = self.chunk_sizes[op]
            lines.extend([
                '`{}`: {} {}'.format(op, rec_count, rec_label),
                '`{}`: {} {} (chunk size is {})'
                    ''.format(op, chunk_count, chunk_label, chunk_size),
            ])
            for rset_name, rset_count in op_totals.items():
                if rset_name is not None:
                    rset_rec_label = _plural('record', rset_count)
                    lines.extend([
                        '    `{}`: {} {}'.format(rset_name, rset_count,
                                                 rset_rec_label)
                    ])
                
        tot_chunks = self.totals['chunks']
        tot_chunks_label = _plural('chunk', tot_chunks)
        tot_batches = self.totals['batches']
        tot_batches_label = _plural('batch', tot_batches, 'es')
        lines.extend([
            '- {} total {}'.format(tot_chunks, tot_chunks_label),
            '- {} total {} ({} chunks per batch)'
                ''.format(tot_batches, tot_batches_label, self.batch_size)
        ])
        
        batch_ids = sorted(self.registry.keys())
        first_chunk_id = self.registry[batch_ids[0]][0]
        last_chunk_id = self.registry[batch_ids[-1]][-1]
        lines.extend([
            'Batch ID Range: {} to {}'.format(batch_ids[0], batch_ids[-1]),
            'Chunk ID Range: {} to {}'.format(first_chunk_id, last_chunk_id)
        ])

        for line in lines:
            exp.log('Info', '| {}'.format(line))

    def finish_chunk(self, chunk_id):
        chunk_obj = self._get_chunk_obj(chunk_id)
        chunk_obj.conn.delete(chunk_obj.key)

    def clear(self):
        for chunk_list in self.registry.values():
            for chunk_id in chunk_list:
                chunk_obj = self._get_chunk_obj(chunk_id)
                chunk_obj.conn.delete(chunk_obj.key)
        for obj in (self._get_reg_obj(), self._get_totals_obj()):
            obj.conn.delete(obj.key)
        self._registry = {}


class ExportTask(Task):
    """
    Subclasses celery.Task to provide custom on_failure and on_success
    behavior.
    """
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """
        Handle a task that raises an uncaught exception.
        """
        chunk_id = kwargs.get('chunk_id', '[UNKNOWN]')
        msg = ('Chunk {} (Celery task_id {}) failed: {}.\nTraceback: {}'
               ''.format(chunk_id, task_id, exc, einfo))
        try:
            exp = spawn_exporter(*args[1:])
        except Exception:
            exp_logger.error(msg)
        else:
            exp.log('Error', msg)

    def on_success(self, retval, task_id, args, kwargs):
        """
        When a task succeeds, the registry for that chunk should be
        cleared from Redis.
        """
        chunk_id = kwargs.get('chunk_id', '[UNKNOWN]')
        try:
            exp = spawn_exporter(*args[1:])
        except Exception as e:
            msg = ('During `on_success` for chunk {}, `spawn_exporter` failed '
                   'with error: {}.').format(chunk_id, e)
            exp_logger.error(msg)
        plan = JobPlan(exp)
        try:
            plan.finish_chunk(chunk_id)
        except Exception as e:
            msg = ('During `on_success` for chunk {}, `plan.finish_chunk` '
                   'failed with error: {}'.format(chunk_id, e))


# Private functions below are just reusable components for tasks

def _hr_line(char='-', len=80):
    return char * len


def _compile_vals_list_for_batch(prev_batch_task_id):
    """
    This is used when a task runs as an error callback. When that
    happens, Celery passes the ID of the task that failed as the first
    argument instead of a list of return values for the successful
    children tasks in the chord; so, to proceed, the error callback has
    to fetch the return values manually.
    """
    res = result.AsyncResult(prev_batch_task_id)
    return [c.result for c in res.children[0].children if c.successful()]


# EXPORT TASKS

@shared_task(base=ExportTask)
@needs_database
def delegate_batch(vals_list, instance_pk, export_filter, export_type, options,
                   chunk_id=None, batch_num=0, prev_batch_had_errors=False,
                   prev_batch_task_id=None, cumulative_vals=None):
    """
    Central Celery task that spawns chords/callbacks/etc. for a given
    export job. The task is recursive, in that, if multiple batches are
    needed to complete a job, another call to `delegate_batch` serves
    as the callback for the chord. Once all batches are completed,
    `do_final_cleanup` is called.
    """
    exp = spawn_exporter(instance_pk, export_filter, export_type, options)
    plan = JobPlan(exp)
    vals_list = vals_list or []

    if batch_num == 0:
        exp.log('Info', 'Job received.')
        exp.status = 'in_progress'
        exp.save_status()
        exp.log('Info', _hr_line())
        exp.log('Info', 'EXPORTER {} -- {}'.format(exp.instance.pk,
                                                   exp.export_type))
        exp.log('Info', _hr_line())
        exp.log('Info', 'Initializing job plan (may take several minutes).')
        plan.generate(exp)
        if plan.registry:
            exp.log('Info', _hr_line())
            plan.log_plan_summary(exp)
            exp.log('Info', _hr_line())
        else:
            msg = ('No records found! Nothing to do.')
            exp.log('Info', msg)

    elif prev_batch_had_errors:
        vals_list = _compile_vals_list_for_batch(prev_batch_task_id)
        exp.log('Info', vals_list)

    cumulative_vals = exp.compile_vals([cumulative_vals] + vals_list)

    # UNCOMMENT the below to help troubleshoot issues with `vals`
    # exp.log('Info', 'BATCH {}'.format(batch_num))
    # exp.log('Info', 'vals_list: {}'.format(vals_list))
    # exp.log('Info', 'cumulative_vals: {}'.format(cumulative_vals))

    batch_id = plan.get_batch_id(batch_num)
    args = (exp.instance.pk, exp.export_filter, exp.export_type, exp.options)

    batch_tasks = []
    for task_chunk_id in plan.registry.get(batch_id, []):
        kwargs = {'chunk_id': task_chunk_id}
        batch_tasks.append(do_export_chunk.s(cumulative_vals, *args, **kwargs))

    prev_batch_task_id = delegate_batch.request.id
    if batch_tasks:
        next_batch_num = batch_num + 1
        next_batch_id = plan.get_batch_id(next_batch_num)
        if next_batch_id in plan.registry:
            # If this isn't the last batch, then the callback for this
            # batch / chord is a new delegate_batch task, to start the
            # next batch.
            cb = delegate_batch.s(*args, chunk_id=next_batch_id,
                                  batch_num=next_batch_num,
                                  prev_batch_task_id=prev_batch_task_id,
                                  cumulative_vals=cumulative_vals)

            # The error callback for that task is another of the same
            # type. Celery will run that error callback if any chunk in
            # the (current) chord raises an error. In that case, we
            # want processing to continue, and we need to pass the
            # error-related args to work around the error. Or, if there
            # is an error in the first callback (delegate) task itself,
            # then this will run, too, which will effectively retry
            # that task. A second error callback is in place in case it
            # errors again.
            err_cb1 = delegate_batch.s(*args, chunk_id=next_batch_id,
                                       batch_num=next_batch_num,
                                       prev_batch_had_errors=True,
                                       prev_batch_task_id=prev_batch_task_id,
                                       cumulative_vals=cumulative_vals)

            # The second error callback is attached to the previous
            # error callback, and it's only needed in case there is a
            # fatal error in the delegate_batch task itself. Then
            # processing can't continue, and it just needs to run
            # do_final_cleanup to log the current state and exit as
            # gracefully as possible.
            err_cb2 = do_final_cleanup.s(*args, status='errors',
                                         delegate_error=True)
            err_cb1.link_error(err_cb2)
            cb.link_error(err_cb1)

        else:
            # If this IS the last batch in the job, then the callback
            # is do_final_cleanup.
            cb = do_final_cleanup.s(*args, cumulative_vals=cumulative_vals)

            # And, we add another call to do_final_cleanup as the
            # link_error for that callback. If any chunk in the current
            # chord raises an error, this will be called; that way the
            # job still completes.
            cb.link_error(
                do_final_cleanup.s(*args, status='errors',
                                   prev_batch_task_id=prev_batch_task_id,
                                   cumulative_vals=cumulative_vals)
            )
        chord(batch_tasks, cb).apply_async()
    else:
        do_final_cleanup.s(vals_list, *args).apply_async()


@shared_task(base=ExportTask)
@needs_database
def do_export_chunk(cumulative_vals, instance_pk, export_filter, export_type,
                    options, chunk_id=None):
    """
    Task that is triggered via `delegate_batch` to load one chunk of a
    larger job.
    """
    exp = spawn_exporter(instance_pk, export_filter, export_type, options)
    plan = JobPlan(exp)
    info = plan.get_chunk_info(chunk_id)
    chunk_label = plan.get_chunk_label(chunk_id)
    exp.log('Info', 'Starting {} ({}).'.format(chunk_id, chunk_label))
    chunk_records = plan.unpack_chunk(exp, chunk_id)
    if info['op'] == 'export':
        chunk_records = exp.apply_prefetches_to_queryset(chunk_records)
    vals = plan.get_method_for_operation(exp, info['op'])(chunk_records)
    exp.log('Info', 'Finished {} ({}).'.format(chunk_id, chunk_label))
    return vals


@shared_task(base=ExportTask)
@needs_database
def do_final_cleanup(vals_list, instance_pk, export_filter, export_type,
                     options, status='success', chunk_id=None,
                     delegate_error=False, prev_batch_task_id=None,
                     cumulative_vals=None):
    """
    Task that runs after all sub-tasks for an export job are done.
    Does final clean-up steps, such as updating the ExportInstance
    status, triggering the final callback function on the export job.
    """
    exp = spawn_exporter(instance_pk, export_filter, export_type, options)
    errors = exp.instance.errors
    warnings = exp.instance.warnings

    vals_list = vals_list or []

    if delegate_error:
        exp.log('Info', 'PROBLEM: A fatal error occurred during batch '
                        'delegation!')
        exp.log('Info', 'This job has ended prematurely. The final callback '
                        'for the exporter DID NOT run.')
        exp.log('Info', 'Please roll back any uncommitted changes this job '
                        'may have made before trying again.')
    else:
        if status == 'errors':
            vals_list = _compile_vals_list_for_batch(prev_batch_task_id)
        
        cumulative_vals = exp.compile_vals([cumulative_vals] + vals_list)

        # UNCOMMENT the below to help troubleshoot issues with `vals`
        # exp.log('Info', 'Final vals_list: {}'.format(vals_list))
        # exp.log('Info', 'Final cumulative_vals: {}'.format(cumulative_vals))

        exp.final_callback(cumulative_vals, status)

        if errors:
            status = 'done_with_errors'
            exp.log('Info', 'Job finished, with errors.')
        else:
            exp.log('Info', 'Job finished successfully.')

    exp.status = status
    exp.save_status()

    plan = JobPlan(exp)
    unprocessed = plan.unprocessed_chunks
    if unprocessed:
        exp.log('Info', 'The following chunks were not fully processed and '
                        'are still registered in Redis: {}'
                        ''.format(', '.join(unprocessed)))
    else:
        plan.clear()
    exp.log('Info', _hr_line('='))
