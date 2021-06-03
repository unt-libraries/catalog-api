"""
Contains models for the export app. These are mainly just to provide
some way to track your exports.
"""
from __future__ import absolute_import
import importlib

from django.db import models
from django.contrib.auth.models import User
from django.utils.encoding import python_2_unicode_compatible


@python_2_unicode_compatible
class ExportType(models.Model):
    """
    Describes 'Type' of export: Bib Records (as MARC), etc.
    """
    code = models.CharField(max_length=255, primary_key=True)
    path = models.CharField(max_length=255)
    label = models.CharField(max_length=255)
    description = models.TextField()
    order = models.IntegerField()

    def __str__(self):
        return self.label

    def get_exporter_class(self):
        mod_path = '.'.join(self.path.split('.')[:-1])
        exporter = self.path.split('.')[-1]
        mod = importlib.import_module(mod_path)
        return getattr(mod, exporter)


@python_2_unicode_compatible
class ExportFilter(models.Model):
    """
    Describes the filter used to limit what entities were exported:
    Full export, date-range export, etc.
    """
    code = models.CharField(max_length=255, primary_key=True)
    label = models.CharField(max_length=255)
    order = models.IntegerField()
    description = models.TextField()

    def __str__(self):
        return self.label


@python_2_unicode_compatible
class Status(models.Model):
    """
    Used by ExportInstance to describe the status or state of the job.
    """
    code = models.CharField(max_length=255, primary_key=True)
    label = models.CharField(max_length=255)
    description = models.TextField()
    
    def __str__(self):
        return self.label

    class Meta:
        verbose_name_plural = 'statuses'


@python_2_unicode_compatible
class ExportInstance(models.Model):
    """
    Instances of exports that have actually been run, including date
    and user that ran them.
    """
    user = models.ForeignKey(User)
    export_type = models.ForeignKey(ExportType)
    export_filter = models.ForeignKey(ExportFilter)
    filter_params = models.CharField(max_length=255)
    status = models.ForeignKey(Status)
    timestamp = models.DateTimeField()
    errors = models.IntegerField(default=0)
    warnings = models.IntegerField(default=0)
    
    def __str__(self):
        return u'{} - {} - {}'.format(self.timestamp,
                                      self.export_type,
                                      self.status)
