from __future__ import absolute_import

import logging
import sys, traceback

import pysolr

from django.core import mail
from django import template
from django.conf import settings
from django.contrib.auth.models import User
from django.utils import timezone as tz
from django.db import connections

from celery import Task, shared_task, chord, chain, result

from . import exporter
from . import models as export_models
from utils.redisobjs import RedisObject

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
    job = delegate_batch.s([], *args, chunk_id='header')
    job.link_error(do_final_cleanup.s(*args, status='errors',
                                      delegate_error=True,
                                      chunk_id='error-callback'))
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
    for index, options in settings.HAYSTACK_CONNECTIONS.iteritems():
        if options['URL'] not in url_stack:
            conn = pysolr.Solr(options['URL'], 
                               timeout=options['TIMEOUT'])
            logger.info('Optimizing {} index.'.format(index))
            conn.optimize()
            url_stack.append(options['URL'])
    logger.info('Done.')


# Export Workflow Components

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
        self.batch_size = batch_size
        self.operations = operations
        self._registry = {}
        self._totals = {}

    def get_batch_id(self, batch_num):
        return '{}-{:05d}'.format(self.instance_pk, batch_num)

    def get_chunk_id(self, batch_num, chunk_num, op):
        max_digits = len(str(self.batch_size - 1))
        padded_cnum = str(chunk_num).zfill(max_digits)
        return '{}-{}-{}'.format(self.get_batch_id(batch_num), padded_cnum, op)

    def what_operation(self, chunk_id):
        return chunk_id.split('-')[-1]

    def what_batch(self, chunk_id):
        return self.get_batch_id(int(chunk_id.split('-')[1]))

    def get_records_for_operation(self, exp, op):
        return exp.get_records() if op == 'export' else exp.get_deletions()

    def get_method_for_operation(self, exp, op):
        return exp.export_records if op == 'export' else exp.delete_records

    def get_chunk_record_range(self, chunk_id):
        _, batch_num, chunk_num, op = chunk_id.split('-')
        batch_num, chunk_num = int(batch_num), int(chunk_num)
        chunk_totals = self.totals['chunks_by_op']
        ch_offset = sum(
            [chunk_totals[iop] for i, iop in enumerate(self.operations)
             if i < self.operations.index(op)]
        )
        abs_chunk_num = ((batch_num * self.batch_size) + chunk_num) - ch_offset
        last_rec_count = abs_chunk_num * self.chunk_sizes[op]
        start = (last_rec_count + 1)
        end = (last_rec_count + len(self.get_chunk_pks(chunk_id)))
        return (start, end, op)

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
                    self.get_chunk_pks(chunk_id)
                except ValueError:
                    pass
                else:
                    res.append(chunk_id)
        return sorted(res)

    def get_chunk_pks(self, chunk_id):
        data = self._get_chunk_obj(chunk_id).get()
        if data is None:
            raise ValueError('chunk_id {} is not valid'.format(chunk_id))
        return data

    def slice_pk_lists(self, pk_lists):
        batch_num, chunk_num = 0, 0
        for op in self.operations:
            chunk_size = self.chunk_sizes[op]
            for i in range(0, len(pk_lists[op]), chunk_size):
                batch_id = self.get_batch_id(batch_num)
                chunk_id = self.get_chunk_id(batch_num, chunk_num, op)
                yield (batch_id, chunk_id, op, pk_lists[op][i:i+chunk_size])
                if chunk_num == (self.batch_size - 1):
                    chunk_num = 0
                    batch_num += 1
                else:
                    chunk_num += 1

    def generate(self, pk_lists):
        if self.registry:
            msg = ('Detected an existing registry for this ExportInstance. '
                   'You must instantiate a new JobPlan using a new '
                   'ExportInstance or use the `clear` method to clear the '
                   'existing one before generating a new one.')
            raise self.AlreadyRegistered(msg)

        total_chunks = 0
        total_recs_by_op = {op: 0 for op in self.operations}
        total_chunks_by_op = {op: 0 for op in self.operations}
        for batch_id, chunk_id, op, pk_slice in self.slice_pk_lists(pk_lists):
            self._registry[batch_id] = self._registry.get(batch_id, [])
            self._registry[batch_id].append(chunk_id)
            chunk_obj = self._get_chunk_obj(chunk_id)
            chunk_obj.set(pk_slice)

            total_chunks += 1
            total_chunks_by_op[op] += 1
            total_recs_by_op[op] += len(pk_slice)

        self._totals = {
            'batches': len(self.registry.keys()),
            'chunks': total_chunks,
            'chunks_by_op': total_chunks_by_op,
            'records': sum(total_recs_by_op.values()),
            'records_by_op': total_recs_by_op
        }

        self._get_reg_obj().set(self._registry)
        self._get_totals_obj().set(self._totals)
        return self._registry

    def log_plan_summary(self, exp):
        def _plural(word, count, add='s'):
            return '{}{}'.format(word, '' if count == 1 else add)

        lines = ['JOB PLAN']
        for op in self.operations:
            rec_count = self.totals['records_by_op'][op]
            rec_label = _plural('record', rec_count)
            chunk_count = self.totals['chunks_by_op'][op]
            chunk_label = _plural('chunk', chunk_count)
            chunk_size = self.chunk_sizes[op]
            lines.extend([
                '`{}`: {} {}'.format(op, rec_count, rec_label),
                '`{}`: {} {} ({} records per chunk)'
                    ''.format(op, chunk_count, chunk_label, chunk_size),
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

    def on_success(self, vals, task_id, args, kwargs):
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


def _fetch_job_pk_lists(exp, plan):
    pk_lists = {}
    for op in OPERATIONS:
        records = plan.get_records_for_operation(exp, op)
        if records is None:
            pk_list = []
        else:
            if hasattr(records, 'items'):
                # This logic branch is specifically for the
                # AllMetadataToSolr exporter, which does exports for
                # multiple querysets at one time.
                pk_list = []
                for name, qset in records.items():
                    pk_qset = qset.prefetch_related(None).select_related(None)
                    pk_qset = _apply_pk_sort_order(pk_qset)
                    new_pks = [(name, r['pk']) for r in pk_qset.values('pk')]
                    pk_list.extend(new_pks)
            else:
                pk_qset = records.prefetch_related(None).select_related(None)
                pk_qset = _apply_pk_sort_order(pk_qset)
                pk_list = [r['pk'] for r in pk_qset.values('pk')]
        pk_lists[op] = pk_list
    return pk_lists


def _initialize_job_plan(exp, plan, pk_lists):
    registry = plan.generate(pk_lists)
    totals = plan.totals
    total_op_chunks = totals['chunks_by_op']
    exp.log('Info', 'Job plan initialized. {} total chunk(s) grouped into {} '
                    'batch(es):'.format(totals['chunks'], totals['batches']))
    for op in plan.operations:
        exp.log('Info', '   {}, {} chunk(s)'.format(op, total_op_chunks[op]))
    return plan


def _apply_pk_sort_order(qset):
    if hasattr(qset.model, 'record_metadata'):
        return qset.order_by('record_metadata__record_last_updated_gmt', 'pk')
    if hasattr(qset.model, 'record_last_updated_gmt'):
        return qset.order_by('record_last_updated_gmt', 'pk')
    return qset.order_by('pk')


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
        exp.log('Info', 'Fetching PK lists.')
        pk_lists = _fetch_job_pk_lists(exp, plan)
        exp.log('Info', 'Initializing job plan.')
        plan.generate(pk_lists)
        exp.log('Info', _hr_line())
        plan.log_plan_summary(exp)
        exp.log('Info', _hr_line())

    elif prev_batch_had_errors:
        vals_list = _compile_vals_list_for_batch(prev_batch_task_id)
        exp.log('Info', vals_list)

    cumulative_vals = exp.compile_vals([cumulative_vals] + vals_list)

    # UNCOMMENT the below to help troubleshoot issues with `vals`
    # exp.log('Info', 'BATCH {}'.format(batch_num))
    # exp.log('Info', 'vals_list: {}'.format(vals_list))
    # exp.log('Info', 'cumulative_vals: {}'.format(cumulative_vals))

    batch_id = plan.get_batch_id(batch_num)
    args = (exp.instance.pk, exp.export_filter, exp.export_type,
            exp.options)

    batch_tasks = []
    for task_chunk_id in plan.registry[batch_id]:
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
    chunk_pks = plan.get_chunk_pks(chunk_id)
    start, end, op = plan.get_chunk_record_range(chunk_id)

    records = plan.get_records_for_operation(exp, op)
    exp_method = plan.get_method_for_operation(exp, op)

    chunk_label = 'records {} - {} for {}'.format(start, end, op)
    exp.log('Info', 'Starting {} ({}).'.format(chunk_id, chunk_label))
    if hasattr(records, 'items'):
        # This logic branch is specifically for the AllMetadataToSolr
        # exporter, which does exports for multiple querysets at once.
        records_to_export = {}
        for group_name, qset in records.items():
            group_pks = [pk for group, pk in chunk_pks if group == group_name]
            qset = _apply_pk_sort_order(qset).filter(pk__in=group_pks)
            records_to_export[group_name] = qset
        vals = exp_method(records_to_export)
    elif records is not None:
        records = _apply_pk_sort_order(records).filter(pk__in=chunk_pks)
        vals = exp_method(records)
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
    status, triggering the final callback function on the export job,
    emailing site admins if there were errors, etc.
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

    (send_errors, send_warnings) = (None, None)
    if errors > 0 and settings.EXPORTER_EMAIL_ON_ERROR:
        subject = '{} Exporter Errors'.format(
                        exp.instance.export_type.code)
        send_errors = errors
    if warnings > 0 and settings.EXPORTER_EMAIL_ON_WARNING:
        subject = '{} Exporter Warnings'.format(
                        exp.instance.export_type.code)
        send_warnings = warnings
    if send_errors or send_warnings:
        logfile = settings.LOGGING['handlers']['export_file']['filename']
        vars = template.Context({
            'i': exp.instance,
            'errors': send_errors,
            'warnings': send_warnings,
            'logfile': logfile
        })
        if send_errors and send_warnings:
            subject = '{} Exporter Errors and Warnings'.format(
                            exp.instance.export_type.code)
        email = template.loader.get_template('export/error_email.txt')
        mail.mail_admins(subject, email.render(vars))
