# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


def populate_test_data(apps, schema_editor):
    if schema_editor.connection.alias == 'sierra':
        return True

    EndNode = apps.get_model('testmodels', 'EndNode')
    SelfReferentialNode = apps.get_model('testmodels', 'SelfReferentialNode')
    ManyToManyNode = apps.get_model('testmodels', 'ManyToManyNode')
    ReferenceNode = apps.get_model('testmodels', 'ReferenceNode')
    ThroughNode = apps.get_model('testmodels', 'ThroughNode')

    end = [EndNode.objects.create(name='end0'),
           EndNode.objects.create(name='end1'),
           EndNode.objects.create(name='end2')]

    srn = [SelfReferentialNode.objects.create(name='srn0', end=end[0]),
           SelfReferentialNode.objects.create(name='srn1'),
           SelfReferentialNode.objects.create(name='srn2', end=end[2])]
    srn[2].parent = srn[1]
    srn[2].save()

    m2m = [ManyToManyNode.objects.create(name='m2m0', end=end[1]),
           ManyToManyNode.objects.create(name='m2m1', end=end[2]),
           ManyToManyNode.objects.create(name='m2m2', end=end[0])]

    ref = [ReferenceNode.objects.create(name='ref0', srn=srn[1], end=end[0]),
           ReferenceNode.objects.create(name='ref1', srn=srn[2], end=end[2]),
           ReferenceNode.objects.create(name='ref2', srn=srn[0], end=end[2])]

    thr = [ThroughNode.objects.create(name='thr0', ref=ref[0], m2m=m2m[0]),
           ThroughNode.objects.create(name='thr1', ref=ref[0], m2m=m2m[1]),
           ThroughNode.objects.create(name='thr2', ref=ref[1], m2m=m2m[0]),
           ThroughNode.objects.create(name='thr3', ref=ref[1], m2m=m2m[2])]


class Migration(migrations.Migration):

    dependencies = [
        ('testmodels', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(populate_test_data)
    ]
