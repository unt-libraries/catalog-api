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

from celery import Task, shared_task, group, chain

from . import exporter
from . import models as export_models

# set up loggers
logger = logging.getLogger('sierra.custom')
exp_logger = logging.getLogger('exporter.file')


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


@needs_database
def trigger_export(instance, export_filter, export_type, options):
    """
    Non-task function to trigger an export from the Django admin view.
    """
    args = (instance.pk, export_filter, export_type, options)
    exp = spawn_exporter(*args)
    exp.status = 'waiting'
    exp.save_status()
    exp.log('Info', 'Export {} task triggered. Waiting on task to be '
                    'scheduled.'.format(instance.pk))
    export_dispatch.apply_async(args,
        link_error=do_final_cleanup.s(*args, status='errors')
    )


@needs_database
def handle_failure(inst_pk, exp_filter, exp_type, opts, message=None):
    """
    Handle logging and cleanup for an Exporter task failure.
    """
    exp = spawn_exporter(inst_pk, exp_filter, exp_type, opts)
    exp.log('Error', message)


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


# Export Workflow

class ExportTask(Task):
    """
    Subclasses celery.Task to provide custom failure behavior.
    """
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        message = 'Task {} failed: {}.'.format(task_id, exc)
        handle_failure(*[i for i in args[:4]], message=message)


@shared_task(base=ExportTask)
@needs_database
def export_dispatch(instance_pk, export_filter, export_type, options):
    """
    Control function for doing an export job.
    """
    exp = spawn_exporter(instance_pk, export_filter, export_type, options)
    exp.log('Info', 'Job received.')
    exp.status = 'in_progress'
    exp.save_status()

    exp.log('Info', '-------------------------------------------------------')
    exp.log('Info', 'EXPORTER {} -- {}'.format(exp.instance.pk, export_type))
    exp.log('Info', '-------------------------------------------------------')
    exp.log('Info', 'MAX RECORD CHUNK size is {}.'.format(exp.max_rec_chunk))
    exp.log('Info', 'MAX DELETION CHUNK size is {}.'.format(exp.max_del_chunk))

    try:
        records = exp.get_records()
        deletions = exp.get_deletions()
    except exporter.ExportError as err:
        exp.log('Error', err)
        exp.status = 'errors'
        exp.save_status()
    else:
        # Get records and deletions counts. If it's a queryset we want
        # to use count(), otherwise we have to use len(). Lists have a
        # count() method, but it's not the same as queryset.count().
        # Lists throw a TypeError if you call count() without an arg.
        try:
            records_count = records.count()
        except (TypeError, AttributeError):
            records_count = len(records)
        try:
            deletions_count = deletions.count()
        except (TypeError, AttributeError):
            try:
                deletions_count = len(deletions)
            except Exception:
                deletions_count = 0

        count = {'record': records_count, 'deletion': deletions_count}
        exp.log('Info', '{} records found.'.format(count['record']))
        if deletions is not None:
            exp.log('Info', '{} candidates found for deletion.'
                    ''.format(count['deletion']))

        do_it_tasks = []
        args = [exp.instance.pk, exp.export_filter, exp.export_type,
                exp.options]
        for type in count:
            max = exp.max_rec_chunk if type == 'record' else exp.max_del_chunk
            task_args = []
            batches = 0
            for start in range(0, count[type], max):
                end = min(start + max, count[type])
                i_args = [{}] + args if start == 0 or exp.parallel else args
                do_it_tasks.append(
                    do_export_chunk.s(*i_args, start=start, end=end, type=type)
                )
                batches += 1
            if batches > 0:
                exp.log('Info', 'Breaking {}s into {} chunk{}.'
                        ''.format(type, batches, 's' if batches > 1 else ''))

        if do_it_tasks:
            if exp.parallel:
                final_grouping = chain(group(do_it_tasks),
                                       do_final_cleanup.s(*args))
            else:
                final_grouping = chain(chain(do_it_tasks),
                                       do_final_cleanup.s(*args))
            final_grouping.apply_async(
                link_error=do_final_cleanup.s(*args, status='errors')
            )
        else:
            args = [{}] + args
            do_final_cleanup.s(*args).apply_async()


@shared_task(base=ExportTask)
@needs_database
def do_export_chunk(vals, instance_pk, export_filter, export_type, options,
                    start, end, type):
    """
    Processes a "chunk" of Exporter records, depending on type
    ("record" if it's a record load or "deletion" if it's a deletion).
    Variable vals should be a dictionary of arbitrary values used to
    pass information from task to task.
    """
    exp = spawn_exporter(instance_pk, export_filter, export_type, options)
    records = exp.get_records() if type == 'record' else exp.get_deletions()
    
    # This is sort of a hack. My strategy initially was to use queryset
    # slicing to get chunks we need, but apparently this doesn't work
    # with prefetch_related--that is, it prefetches data for the ENTIRE
    # friggin queryset despite the slice and makes us run out of memory
    # on large jobs. Instead of slicing we actually have to use a
    # filter before it correctly limits the prefetch. So we slice up
    # PKs instead and use that as a basis for a filter.
    try:
        pks = list(records.prefetch_related(None).order_by('pk')[start:end+1])
        pk_a = pks[0].pk
        pk_n = pks[-1].pk
        if type == 'record':
            records = exp.get_records() 
        else:
            records = exp.get_deletions()
        records = records.order_by('pk').filter(pk__gte=pk_a, pk__lte=pk_n)
    except AttributeError:
        if records is not None:
            records = records[start:end+1]
    
    job_id = '{}s {} - {}'.format(type, start+1, end)
    exp.log('Info', 'Starting processing {}.'.format(job_id))
    try:
        if type == 'record' and records is not None:
            vals = exp.export_records(records, vals=vals)
        elif records is not None:
            vals = exp.delete_records(records, vals=vals)
    except Exception as err:
        ex_type, ex, tb = sys.exc_info()
        logger.info(traceback.extract_tb(tb))
        exp.log('Error', 'Error processing {}: {}.'.format(job_id, err))
    else:
        exp.log('Info', 'Finished processing {}.'.format(job_id))
    return vals


@shared_task(base=ExportTask)
@needs_database
def do_final_cleanup(vals, instance_pk, export_filter, export_type, options,
                     status='success'):
    """
    Task that runs after all sub-tasks for an export job are done.
    Does final clean-up steps, such as updating the ExportInstance
    status, triggering the final callback function on the export job,
    emailing site admins if there were errors, etc.
    """
    exp = spawn_exporter(instance_pk, export_filter, export_type, options)
    exp.final_callback(vals, status)
    errors = exp.instance.errors
    warnings = exp.instance.warnings
    if status == 'success':
        if errors > 0:
            status = 'done_with_errors'
        exp.log('Info', 'Job finished.')
    elif status == 'errors':
        exp.log('Warning', 'Job terminated prematurely.')
    exp.status = status
    exp.save_status()
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
