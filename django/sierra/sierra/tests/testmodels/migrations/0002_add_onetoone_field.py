# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from __future__ import absolute_import
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('testmodels', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='OneToOneNode',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
            ],
        ),
        migrations.AddField(
            model_name='referencenode',
            name='one',
            field=models.OneToOneField(null=True, to='testmodels.OneToOneNode'),
        ),
    ]
