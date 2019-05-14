"""
Default batch Exporters are defined here.

ExportType entities translate 1:1 to Exporter subclasses. For each
ExportType you define, you're going to have a class defined here that
inherits from the base Exporter class. Your ExportType.code should
match the class name that handles that ExportType.
"""
from __future__ import unicode_literals
import logging
import re

from django.conf import settings

from . import exporter
from . import models

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class AllMetadataToSolr(exporter.Exporter):
    """
    Loads ALL metadata-type data into Solr, as defined by the
    EXPORTER_METADATA_TYPE_REGISTRY setting in your Django settings.
    """
    child_etype_names = settings.EXPORTER_METADATA_TYPE_REGISTRY
    
    def __init__(self, *args, **kwargs):
        super(AllMetadataToSolr, self).__init__(*args, **kwargs)
        self.child_instances = {}
        for etype_name in self.child_etype_names:
            etype = models.ExportType.objects.get(pk=etype_name)
            etype_class = etype.get_exporter_class()
            exp_inst = etype_class(self.instance.pk, self.export_filter,
                                   self.export_type, self.options)
            self.child_instances[etype_name] = exp_inst

    def get_records(self):
        return { k: v.get_records() for k, v in self.child_instances.items() }
    
    def export_records(self, records):
        ret_vals = {}
        for etype_name in self.child_etype_names:
            exp_inst = self.child_instances[etype_name]
            et_vals = exp_inst.export_records(records[etype_name])
            ret_vals[etype_name] = et_vals
        return ret_vals

    def final_callback(self, vals=None, status='success'):
        vals = vals or {}
        for etype_name in self.child_etype_names:
            exp_inst = self.child_instances[etype_name]
            exp_inst.final_callback(vals=vals.get(etype_name, {}),
                                    status=status)
