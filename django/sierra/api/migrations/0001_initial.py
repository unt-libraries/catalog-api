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
            name='APIUser',
            fields=[
                ('id', models.AutoField(verbose_name='ID',
                 serialize=False, auto_created=True, primary_key=True)),
                ('secret', models.CharField(max_length=128)),
                ('permissions', models.TextField(
                    default=b'{"system_shelflist_item_note":false,"change_shelflist_item_note":false,"delete_shelflist_item_note":false,"change_shelflist_item":false,"add_shelflist_item_note":false}')),
                ('user', models.OneToOneField(on_delete=models.CASCADE,
                                              to=settings.AUTH_USER_MODEL)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
    ]
