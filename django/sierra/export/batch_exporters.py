"""
Default batch Exporters are defined here.

ExportType entities translate 1:1 to Exporter subclasses. For each
ExportType you define, you're going to have a class defined here that
inherits from the base Exporter class. Your ExportType.code should
match the class name that handles that ExportType.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from django.conf import settings
from export.exporter import BatchExporter

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class AllMetadataToSolr(BatchExporter):
    """
    Loads ALL metadata-type data into Solr, as defined by the
    EXPORTER_METADATA_TYPE_REGISTRY setting in your Django settings.
    """
    Child = BatchExporter.Child
    children_config_list = []

    for n in settings.EXPORTER_METADATA_TYPE_REGISTRY:
        children_config_list.append(Child(n))

    children_config = tuple(children_config_list)

    def get_deletions(self):
        return None
