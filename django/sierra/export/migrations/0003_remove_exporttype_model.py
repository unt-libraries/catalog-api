# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('export', '0002_auto_20170620_1001'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='exporttype',
            name='model',
        ),
    ]
