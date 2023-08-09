# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('export', '0003_auto_20171219_1255'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='exporttype',
            name='model',
        ),
    ]
