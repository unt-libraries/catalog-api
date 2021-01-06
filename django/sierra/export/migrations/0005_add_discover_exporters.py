# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from utils.load_data import load_data


class Migration(migrations.Migration):

    dependencies = [
        ('export', '0004_remove_exporttype_model'),
    ]

    operations = [
        migrations.RunPython(
            load_data('export/migrations/data/discover.json',
                      'default')),
    ]
