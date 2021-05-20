# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from __future__ import absolute_import
from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='ExportFilter',
            fields=[
                ('code', models.CharField(max_length=255, serialize=False, primary_key=True)),
                ('label', models.CharField(max_length=255)),
                ('order', models.IntegerField()),
                ('description', models.TextField()),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ExportInstance',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('filter_params', models.CharField(max_length=255)),
                ('timestamp', models.DateTimeField()),
                ('errors', models.IntegerField(default=0)),
                ('warnings', models.IntegerField(default=0)),
                ('export_filter', models.ForeignKey(to='export.ExportFilter')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ExportType',
            fields=[
                ('code', models.CharField(max_length=255, serialize=False, primary_key=True)),
                ('path', models.CharField(max_length=255)),
                ('label', models.CharField(max_length=255)),
                ('description', models.TextField()),
                ('order', models.IntegerField()),
                ('model', models.CharField(max_length=255)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Status',
            fields=[
                ('code', models.CharField(max_length=255, serialize=False, primary_key=True)),
                ('label', models.CharField(max_length=255)),
                ('description', models.TextField()),
            ],
            options={
                'verbose_name_plural': 'statuses',
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='exportinstance',
            name='export_type',
            field=models.ForeignKey(to='export.ExportType'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='exportinstance',
            name='status',
            field=models.ForeignKey(to='export.Status'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='exportinstance',
            name='user',
            field=models.ForeignKey(to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
    ]
