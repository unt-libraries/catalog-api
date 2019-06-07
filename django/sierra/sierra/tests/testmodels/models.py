"""
Contains models used only for testing purposes.
"""
from django.db import models


class EndNode(models.Model):
    name = models.CharField(max_length=255)


class SelfReferentialNode(models.Model):
    name = models.CharField(max_length=255)
    end = models.ForeignKey(EndNode, null=True)
    parent = models.ForeignKey('self', null=True)


class ManyToManyNode(models.Model):
    name = models.CharField(max_length=255)
    end = models.ForeignKey(EndNode)


class ThroughNode(models.Model):
    name = models.CharField(max_length=255)
    ref = models.ForeignKey('ReferenceNode')
    m2m = models.ForeignKey(ManyToManyNode)

class OneToOneNode(models.Model):
    name = models.CharField(max_length=255)


class ReferenceNode(models.Model):
    name = models.CharField(max_length=255)
    srn = models.ForeignKey(SelfReferentialNode)
    end = models.ForeignKey(EndNode)
    one = models.OneToOneField(OneToOneNode, null=True)
    m2m = models.ManyToManyField(ManyToManyNode, through=ThroughNode)
