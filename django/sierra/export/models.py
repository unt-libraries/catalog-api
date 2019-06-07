"""
Contains models for the export app. These are mainly just to provide
some way to track your exports.
"""
import importlib

from django.db import models
from django.contrib.auth.models import User

class ExportType(models.Model):
    """
    Describes 'Type' of export: Bib Records (as MARC), etc.
    """
    code = models.CharField(max_length=255, primary_key=True)
    path = models.CharField(max_length=255)
    label = models.CharField(max_length=255)
    description = models.TextField()
    order = models.IntegerField()

    def __unicode__(self):
        return self.label

    def get_exporter_class(self):
        mod_path = '.'.join(self.path.split('.')[:-1])
        exporter = self.path.split('.')[-1]
        mod = importlib.import_module(mod_path)
        return getattr(mod, exporter)


class ExportFilter(models.Model):
    """
    Describes the filter used to limit what entities were exported:
    Full export, date-range export, etc.
    """
    code = models.CharField(max_length=255, primary_key=True)
    label = models.CharField(max_length=255)
    order = models.IntegerField()
    description = models.TextField()

    def __unicode__(self):
        return self.label


class Status(models.Model):
    """
    Used by ExportInstance to describe the status or state of the job.
    """
    code = models.CharField(max_length=255, primary_key=True)
    label = models.CharField(max_length=255)
    description = models.TextField()
    
    def __unicode__(self):
        return self.label

    class Meta:
        verbose_name_plural = 'statuses'

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
    
    def __unicode__(self):
        return u'{} - {} - {}'.format(self.timestamp,
                                      self.export_type,
                                      self.status)
