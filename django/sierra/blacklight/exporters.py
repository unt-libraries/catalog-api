"""
Exporters module for catalog-api `blacklight` app.
"""

from __future__ import unicode_literals
import logging

from export import exporter


class BibsToBlacklightStaging(exporter.Exporter):
    """
    This is a temporary placeholder.

    Once our Blacklight staging/beta site is up, this will become the
    primary Exporter class for loading bib records into our Blacklight
    Solr instance (blacklight-staging), which has yet to be created.

    Changes made and features created using an exporters_* file should
    be incorporated into this class to be deployed on staging.

    """
    pass
