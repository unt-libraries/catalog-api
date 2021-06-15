"""
Contains models used only for testing relationtrees and makefixtures.

These test models function as test fixtures or input data for the tests
that use them. E.g., `relationtrees` is designed to traverse relations
on Django models and build tree-like structures to represent them.
These models emulate the various common relationships you might have in
a live database, and tests ensure that the relevant code can traverse
them correctly.

Summary of model structure:

RN => SRN => SRN => End
          => End
   <= ThN => M2M => End
   -> One
   => End

Where:
=> indicates a Forward Many to One relationship
<= indicates a Reverse Many to One relationship
-> indicates a Forward One to One relationship
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
