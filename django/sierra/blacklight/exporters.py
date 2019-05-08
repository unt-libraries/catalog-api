"""
Exporters module for catalog-api `blacklight` app.
"""

from __future__ import unicode_literals
import logging
import subprocess
import os
import re
import shlex
import sys, traceback

import pysolr

from django.conf import settings

from export import exporter
from export.basic_exporters import BibsToSolr, BibsDownloadMarc
from blacklight.sierra2marc import S2MarcBatchBlacklightSolrMarc

# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


class BaseBibsDownloadMarc(BibsDownloadMarc):
    """
    This is a base exporter class for converting Sierra data to MARC
    records, to be used with a BaseSolrMarcBibsToSolr class.

    Subclass and specify a different s2marc_batch_class to change how
    Sierra records are converted to MARC.
    """

    s2marc_batch_class = S2MarcBatchBlacklightSolrMarc

    _new_pfetch = ['locations',
                   'bibrecorditemrecordlink_set__item_record__location']
    prefetch_related = sorted(BibsDownloadMarc.prefetch_related + _new_pfetch)
    select_related = BibsDownloadMarc.select_related

    def export_records(self, records):
        log_label = self.__class__.__name__
        batch = type(self).s2marc_batch_class(records)
        out_recs = batch.to_marc()
        try:
            filename = batch.to_file(out_recs, append=False)
        except IOError as e:
            self.log('Error', 'Error writing to output file: {}'.format(e), 
                     log_label)
        else:
            for e in batch.errors:
                self.log('Warning', 'Record {}: {}'.format(e.id, e.msg),
                         log_label)
        return { 'marcfile': filename }


class BaseSolrMarcBibsToSolr(BibsToSolr):
    """
    This is a base exporter class for creating bib exporters that run
    via SolrMarc.

    Subclass and specify the `cores` class attr so that the `bibs` dict
    element points to the correct Solr core and the `bib2marc_class`
    to point to a BaseBibsDownloadMarc subclass, if needed.
    """
    
    bib2marc_class = BaseBibsDownloadMarc
    cores = {'bibs': 'SPECIFY_SOLR_CORE_HERE'}

    prefetch_related = bib2marc_class.prefetch_related
    select_related = bib2marc_class.select_related

    @classmethod
    def solr_url(cls, ctype):
        host, port = settings.SOLR_HOST, settings.SOLR_PORT
        return 'http://{}:{}/solr/{}'.format(host, port, cls.cores[ctype])

    @classmethod
    def solr_conn(cls, ctype):
        return pysolr.Solr(cls.solr_url(ctype))

    def export_records(self, records):
        log_label = type(self).__name__
        bibs_solr_url = type(self).solr_url('bibs')
        bibs_indprop = '{}_index.properties'.format(type(self).cores['bibs'])
        jarfile = ('{}/../../solr/solrmarc/StanfordSearchWorksSolrMarc.jar'
                   '').format(settings.PROJECT_DIR)
        config_file = settings.SOLRMARC_CONFIG_FILE
        filedir = settings.MEDIA_ROOT
        if filedir[-1] != '/':
            filedir = '{}/'.format(filedir)
        bib_converter = self.bib2marc_class(
            self.instance.pk, self.export_filter, self.export_type,
            self.options
        )
        converter_vals = bib_converter.export_records(records)
        filename = converter_vals['marcfile']
        filepath = '{}{}'.format(filedir, filename)

        cmd = ('java -Xmx1g -Dsolr.hosturl="{}" '
               '-Dsolrmarc.indexing.properties="{}" '
               '-jar "{}" {} {}').format(bibs_solr_url, bibs_indprop, jarfile,
                                         config_file, filepath)

        try:
            output = subprocess.check_output(shlex.split(cmd),
                                             stderr=subprocess.STDOUT,
                                             shell=False,
                                             universal_newlines=True)
            output = output.decode('unicode-escape')
        except subprocess.CalledProcessError as e:
            error_lines = e.output.split("\n")
            for line in error_lines:
                self.log('Error', line)
            self.log('Error', 'Solrmarc process did not run successfully.',
                     log_label)
        else:
            error_lines = output.split("\n")
            del(error_lines[-1])
            if error_lines:
                for line in error_lines:
                    line = re.sub(r'^\s+', '', line)
                    if re.match(r'^WARN', line):
                        self.log('Warning', line, log_label)
                    elif re.match(r'^ERROR', line):
                        self.log('Warning', line, log_label)

        os.remove(filepath)

    def get_record_id(self, record):
        return 'base.bibrecord.{}'.format(record.id)

    def delete_records(self, records):
        bibs_solr = type(self).solr_conn('bibs')
        log_label = type(self).__name__
        for r in records:
            try:
                bibs_solr.delete(id=self.get_record_id(r), commit=False)
            except Exception as e:
                ex_type, ex, tb = sys.exc_info()
                logger.info(traceback.extract_tb(tb))
                self.log('Error', 'Record {}: {}'
                         ''.format(str(r), e), log_label)

    def final_callback(self, vals=None, status='success'):
        bibs_solr = type(self).solr_conn('bibs')
        log_label = type(self).__name__
        self.log('Info', 'Committing updates to Solr...', log_label)
        bibs_solr.commit()
