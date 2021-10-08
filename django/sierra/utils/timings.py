'''
This contains code for helping test timings of Django REST framework
views.

TO USE

First, go into your view code and create a dispatch method on your base
view class. It should call the dispatch method on the parent class,
but before returning the Response object, have it add an element called
'timings' that contains a dictionary, where each element will be a
timing for an individual task you want to measure, in seconds. For
example, here's what a Response object might contain:

response_obj['timings'] = {
    'query_time': 0.075,
    'render_time': 0.006,
    'serializer_time': 0.075,
    'view_time': 0.004
}

Next, in your view code (in the dispatch method and in the get/post/put
etc. methods), add statements that measure the timings. For example,
set a variable to the current time (using datetime.now()) before the
code you want to time and then set another variable to the current time
after. Subtract the two and use the .total_seconds() on the result to
get seconds. Make sure that the timings are saved in a way that will be
accessible to your dispatch method (like a global variable).

Now you can import this module in a console, instantiate some
APIViewTimingTester objects, and go to town. Use .run_tests() to run
your tests and then .log_results() to log the results to whatever
logger you want.
'''

from __future__ import absolute_import
from __future__ import print_function
import logging
import ast
from datetime import datetime, time

from rest_framework.test import APIClient
from six import iteritems
from six.moves import range


class APIViewTimingTester(object):
    console_logger = logging.getLogger('sierra.custom')
    file_logger = logging.getLogger('sierra.file')

    def __init__(self, url_base='/', test_params={}, repeat=4):
        self.url_base = url_base
        self.test_params = test_params
        self.repeat = repeat
        self.results = []

    def _output_file_log_header(self, logger):
        logger.info('***************************************************')
        logger.info('API VIEW TIMING TESTS')
        logger.info(str(datetime.now()))
        logger.info('***************************************************')

    def run_tests(self, url_base=None, test_params=None, repeat=None):
        if url_base is None:
            url_base = self.url_base
        if test_params is None:
            test_params = self.test_params
        if repeat is None:
            repeat = self.repeat

        logger = self.console_logger

        print('Starting API View timing test set.')
        client = APIClient()
        for test in test_params:
            url = '{}{}'.format(url_base, test['url'])
            print(('Sending {} requests for {}, using data {}.'
                   ''.format(repeat, url, test['data'])))
            timings = {}
            print ('First throwaway request...')
            response = client.get(url, test['data'])
            print ('Now the real requests.')
            for i in range(0, repeat):
                print('{}...'.format(i+1))
                response = client.get(url, test['data'])
                try:
                    raw_timings = ast.literal_eval(response['timings'])
                except ValueError:
                    raw_timings = response['timings']
                for key, value in iteritems(raw_timings):
                    timings[key] = timings.get(key, 0) + value
                    if i == repeat - 1:
                        timings[key] = timings.get(key, 0) / repeat
            print('Done.')
            self.results.append({
                'repeat': repeat,
                'url': url,
                'data': test['data'],
                'timings': timings
            })
        print('Finished API View timing test set.')

    def log_results(self, logger=None):
        if logger is None:
            logger = self.file_logger

        self._output_file_log_header(logger)
        for r in self.results:
            logger.info('Test {}, {}, repeat {}'
                        ''.format(r['url'], r['data'], r['repeat']))
            for key, value in sorted(iteritems(r['timings'])):
                logger.info('{:<30}{:.3f}'.format('{}:'.format(key), value))
            logger.info('')


p = [
    {'url': 'items/.json', 'data': {'offset': 1, 'limit': 20}},
    {'url': 'items/.json', 'data': {'offset': 79460, 'limit': 20}},
    {'url': 'items/.json', 'data': {'offset': 158000, 'limit': 20}},
    {'url': 'items/.json', 'data': {'offset': 1, 'limit': 100}},
    {'url': 'items/.json', 'data': {'offset': 79460, 'limit': 100}},
    {'url': 'items/.json', 'data': {'offset': 158000, 'limit': 100}},
    {'url': 'items/.json', 'data': {'offset': 1, 'limit': 500}},
    {'url': 'items/.json', 'data': {'offset': 79460, 'limit': 500}},
    {'url': 'items/.json', 'data': {'offset': 158000, 'limit': 500}},
]
tester = APIViewTimingTester('/api/v1/', p, repeat=10)
