# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from __future__ import absolute_import
from django.db import migrations, models


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
