from __future__ import absolute_import

from __future__ import print_function
import os
from unipath import Path

import dotenv
from celery import Celery


# set the default Django settings module for the 'celery' program.
dotenv.load_dotenv('{}/settings/.env'.format(Path(__file__).ancestor(1)))

app = Celery('sierra')

# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    print(('Request: {0!r}'.format(self.request)))
