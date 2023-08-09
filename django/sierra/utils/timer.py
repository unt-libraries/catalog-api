"""
Contains a generic utilty class for timing events.
"""

from datetime import datetime, timedelta

import logging

logger = logging.getLogger('sierra.custom')



class Timer(object):
    def __init__(self, logger):
        self.timings = []
        self.event_stack = {}
        self.logger = logger
        self.enabled = True

    def log_timing(self, label, timing):
        as_seconds = timing.total_seconds()
        self.logger.info('{: >40} {: >10.6f}s'.format(label, as_seconds))

    def start(self, label):
        if self.enabled:
            now = datetime.now()
            self.event_stack[label] = now

    def end(self, label):
        if self.enabled:
            if label in self.event_stack:
                now = datetime.now()
                event_start = self.event_stack.pop(label)
                timing = now - event_start
                self.timings.append((label, timing))
                self.log_timing(label, timing)
            else:
                self.logger.info('{: >40} {: >11}'.format(label, 'N/A'))

    def report(self):
        report_tally = {}
        self.logger.info('FINAL TIMING REPORT')
        for l, t in self.timings:
            tallied = report_tally.get(l, [])
            tallied.append(t)
            report_tally[l] = tallied
        self.logger.info('    TOTALS')
        for l, tallied in report_tally.items():
            self.log_timing(l, sum(tallied, timedelta()))
        self.logger.info('    AVERAGES')
        for l, tallied in report_tally.items():
            avg = sum(tallied, timedelta()) / len(tallied)
            self.log_timing(l, avg)


TIMER = Timer(logger)

