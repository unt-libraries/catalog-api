# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0002_auto_20190517_1053'),
    ]

    operations = [
        migrations.AlterField(
            model_name='apiuser',
            name='permissions',
            field=models.TextField(default='{}'),
        ),
    ]
