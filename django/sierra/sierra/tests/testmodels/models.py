"""
Contains models used only for testing purposes.
"""
from __future__ import absolute_import
from django.db import models


class EndNode(models.Model):
    name = models.CharField(max_length=255)


class SelfReferentialNode(models.Model):
    name = models.CharField(max_length=255)
    end = models.ForeignKey(EndNode, on_delete=models.CASCADE, null=True)
    parent = models.ForeignKey('self', on_delete=models.CASCADE, null=True)


class ManyToManyNode(models.Model):
    name = models.CharField(max_length=255)
    end = models.ForeignKey(EndNode, on_delete=models.CASCADE)


class ThroughNode(models.Model):
    name = models.CharField(max_length=255)
    ref = models.ForeignKey('ReferenceNode', on_delete=models.CASCADE)
    m2m = models.ForeignKey(ManyToManyNode, on_delete=models.CASCADE)

class OneToOneNode(models.Model):
    name = models.CharField(max_length=255)


class ReferenceNode(models.Model):
    name = models.CharField(max_length=255)
    srn = models.ForeignKey(SelfReferentialNode, on_delete=models.CASCADE)
    end = models.ForeignKey(EndNode, on_delete=models.CASCADE)
    one = models.OneToOneField(OneToOneNode, on_delete=models.CASCADE,
                               null=True)
    m2m = models.ManyToManyField(ManyToManyNode, through=ThroughNode)
