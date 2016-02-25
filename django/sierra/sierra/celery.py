from __future__ import absolute_import

import os
from unipath import Path

import ujson
from celery import Celery

from django.conf import settings

# set the default Django settings module for the 'celery' program.
with open('{}/settings/settings.json'.format(Path(__file__).ancestor(1))) as f:
    local_settings = ujson.loads(f.read())

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 
                      local_settings.get('SETTINGS_MODULE'))

app = Celery('sierra')

# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))