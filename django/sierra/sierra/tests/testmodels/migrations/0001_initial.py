# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='EndNode',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ManyToManyNode',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
                ('end', models.ForeignKey(to='testmodels.EndNode')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ReferenceNode',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
                ('end', models.ForeignKey(to='testmodels.EndNode')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='SelfReferentialNode',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
                ('end', models.ForeignKey(to='testmodels.EndNode', null=True)),
                ('parent', models.ForeignKey(to='testmodels.SelfReferentialNode', null=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ThroughNode',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
                ('m2m', models.ForeignKey(to='testmodels.ManyToManyNode')),
                ('ref', models.ForeignKey(to='testmodels.ReferenceNode')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='referencenode',
            name='m2m',
            field=models.ManyToManyField(to='testmodels.ManyToManyNode', through='testmodels.ThroughNode'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='referencenode',
            name='srn',
            field=models.ForeignKey(to='testmodels.SelfReferentialNode'),
            preserve_default=True,
        ),
    ]
