# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from utils.load_data import load_data


class Migration(migrations.Migration):

    dependencies = [
        ('export', '0002_auto_20170620_1001'),
    ]

    operations = [
        migrations.RunPython(
            load_data('export/migrations/data/bl_alpha_solrmarc.json',
                      'default')),
    ]
