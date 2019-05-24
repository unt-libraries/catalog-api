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

from export.exporter import BatchExporter
from . import models

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class AllMetadataToSolr(BatchExporter):
    """
    Loads ALL metadata-type data into Solr, as defined by the
    EXPORTER_METADATA_TYPE_REGISTRY setting in your Django settings.
    """
    Child = BatchExporter.Child
    children_config = tuple([
        Child(n) for n in settings.EXPORTER_METADATA_TYPE_REGISTRY
    ])

    def get_deletions(self):
        return None
