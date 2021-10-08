"""
Contains integration tests for Celery tasks in `export/tasks.py`.
"""

import pytest
from export import tasks as export_tasks
from sierra.celery import app


# FIXTURES AND TEST DATA

# pytestmark = pytest.mark.django_db


@pytest.fixture(scope='function')
def export_and_monitor(global_new_export_instance, django_db_blocker):
    """
    Pytest fixture that returns a function for setting up a Celery
    monitor process, triggering an exporter via
    `export.tasks.export_dispatch`, and monitoring the events reported
    by Celery (tasks received, succeeded, failed, etc.). Returns a
    `celery.events.state` object that encapsulates the captured events
    and tasks.

    Note that, when you run the function, it blocks while all the
    Celery tasks run and doesn't return until it satisfies the
    `should_stop` criteria. (In all cases, the final callback task is
    `do_final_cleanup`; it waits for the final success or failure event
    for that task and then sends a signal to stop monitoring and
    return. However, if something goes wrong that prevents this task
    from firing or finishing, the monitor could hang on a test
    indefinitely. If necessary, you can use the `limit` parameter
    to set a limit on how many cycles the monitor will run before it
    exits. (What number is appropriate depends on how long the test
    tasks will run before they finish. Using too low a value will stop
    them prematurely. You will have to experiment.)
    """
    class StopMonitor(Exception):
        pass

    class CeleryExportMonitor(object):
        def __init__(self, celery_app):
            self.celery_app = celery_app
            self.reset()

        def reset(self, exp_type=None, exp_filter=None, options=None):
            self.exp = None
            if exp_type and exp_filter:
                self.exp = self.make_exporter(exp_type, exp_filter, options)
            self.celery_app.events.Receiver.should_stop = False
            self.state = self.celery_app.events.State()
            self.log = ['Reset. Waiting to start.']
            self.status = 'waiting'

        def make_exporter(self, exp_type, exp_filter, options):
            options = options or {}
            inst = global_new_export_instance(exp_type, exp_filter, 'waiting')
            exp_class = inst.export_type.get_exporter_class()
            return exp_class(inst.pk, exp_filter, exp_type, options)

        def start_export(self):
            export_tasks.export_dispatch.delay(
                self.exp.instance.pk,
                self.exp.export_filter,
                self.exp.export_type,
                self.exp.options
            )

        def should_stop(self, event):
            task = self.state.tasks.get(event['uuid'])
            is_final_task = task.name.endswith('do_final_cleanup')
            task_done = event['type'] not in ('task-received', 'task-started')
            return is_final_task and task_done

        def stop(self):
            self.celery_app.events.Receiver.should_stop = True

        def on_heartbeat(self, event):
            self.log.append('Heartbeat: clock {}'.format(event['clock']))
            if self.status == 'capturing':
                self.status = 'export started'
                self.log.append('Starting exporter.')
                self.start_export()
            elif self.status == 'stopping':
                self.log.append('Stop signal did not work: stopping on HB')
                raise StopMonitor()

        def on_task_event(self, event):
            prefix, etype = event['type'].split('-')
            if prefix == 'task':
                self.state.event(event)
                task = self.state.tasks.get(event['uuid'])
                self.log.append(
                    '{} {}: clock {}'.format(
                        event['type'], task.name, event['clock']
                    )
                )
            if self.should_stop(event):
                self.log.append('Sending signal to stop.')
                self.status = 'stopping'
                self.stop()

        def start(self, exp_type, exp_filter, options, limit=None):
            self.reset(exp_type, exp_filter, options)
            with self.celery_app.connection() as conn:
                self.log.append('Connected to worker: {}'.format(conn))
                self.status = 'capturing'
                recv = self.celery_app.events.Receiver(conn, handlers={
                    'worker-heartbeat': self.on_heartbeat,
                    '*': self.on_task_event,
                })
                try:
                    recv.capture(limit=limit, timeout=None, wakeup=True)
                except StopMonitor:
                    pass

            if self.status != 'stopping':
                self.log.append('Exited before stop signal (hit `limit`?)')
            self.log.append('Done capturing.')
            self.exp.instance.refresh_from_db()
            self.celery_app.events.Receiver.should_stop = False

    def _export_and_monitor(exp_type, exp_filter, options, limit=None):
        with django_db_blocker.unblock():
            monitor = CeleryExportMonitor(app)
            monitor.start(exp_type, exp_filter, options, limit)
        return monitor
    return _export_and_monitor


# TESTS

@pytest.mark.parametrize('exp_type, exp_filter, options, num_expected_batches, '
                         'num_expected_chunks, exp_solr_results, wait_limit', [
                             ('LocationsToSolr', 'full_export',
                              {}, 1, 1, {'haystack': 131}, 30),
                             ('ItypesToSolr', 'full_export',
                              {}, 1, 1, {'haystack': 100}, 30),
                             ('ItemStatusesToSolr', 'full_export',
                              {}, 1, 1, {'haystack': 22}, 30),
                             ('AllMetadataToSolr', 'full_export',
                              {}, 1, 3, {'haystack': 253}, 30),
                             ('ItemsToSolr', 'full_export',
                              {}, 1, 2, {'haystack': 269}, 30),
                             ('EResourcesToSolr', 'full_export',
                              {}, 1, 2, {'haystack': 1}, 30),
                             ('ItemsBibsToSolr', 'full_export', {}, 1, 2,
                              {'haystack': 269, 'bibdata': 0}, 30),
                             ('BibsAndAttachedToSolr', 'full_export', {}, 1, 4,
                              {'haystack': 269, 'bibdata': 0}, 30),
                             ('BibsToDiscover', 'full_export',
                              {}, 1, 2, {'discover-02': 261}, 30),
                             ('BibsToDiscoverAndAttachedToSolr', 'full_export', {}, 1, 2,
                              {'haystack': 269, 'bibdata': 0, 'discover-02': 261}, 30),
                         ])
def test_export_tasks(exp_type, exp_filter, options, num_expected_chunks,
                      num_expected_batches, exp_solr_results, wait_limit,
                      solr_conns, export_and_monitor):
    """
    Exporters that run via Celery tasks should work correctly. This is
    a complete integration test for all of the available exporters,
    running the `full_export` process. The following should be true for
    the test to pass:
    - A task should have run for each expected chunk, each expected
      batch, plus two additional tasks (delegate_batch and
      do_final_cleanup).
    - All tasks should succeed.
    - The last task should be do_final_cleanup.
    - The exporter instance should report a status code of 'success.'
    - The correct Solr core(s) should have the correct number of
      records. All other Solr cores should be empty.
    - The JobPlan (used to corral the tasks for a given export) should
      have an empty registry and no unprocessed chunks, indicating it
      has successfully been cleared out.
    """
    monitor = export_and_monitor(exp_type, exp_filter, options, wait_limit)
    tasks = list(monitor.state.tasks.values())
    plan = export_tasks.JobPlan(monitor.exp)

    # Print the monitor log if test fails:
    for entry in monitor.log:
        print('*MONITOR* {}'.format(entry))

    assert len(tasks) == num_expected_chunks + num_expected_batches + 2
    assert all([t.succeeded for t in tasks])
    assert tasks[-1].name.endswith('do_final_cleanup')
    assert monitor.exp.instance.status.code == 'success'
    assert len(plan.unprocessed_chunks) == 0
    assert plan.registry == {}
    for name, conn in solr_conns.items():
        if name != 'default':
            assert conn.search('*:*').hits == exp_solr_results.get(name, 0)
