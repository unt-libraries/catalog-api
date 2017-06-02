# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from utils.load_data import load_data


class Migration(migrations.Migration):

    dependencies = [
        ('base', '0001_squashed_0009_auto_20170602_1057'),
    ]

    operations = [
        migrations.RunPython(
            load_data('base/migrations/data/metadatafixtures.json', 'sierra')),
        migrations.RunPython(
            load_data('base/migrations/data/bibfixtures.json', 'sierra')),
    ]
