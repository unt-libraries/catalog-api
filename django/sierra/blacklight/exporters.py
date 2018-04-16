"""
Exporters module for catalog-api `blacklight` app.
"""

from __future__ import unicode_literals
import logging
import subprocess
import os
import re
import shlex

import pysolr

from django.conf import settings

from export import exporter
from export.basic_exporters import BibsToSolr, BibsDownloadMarc
from export.sierra2marc import S2MarcBatch


class BaseBibsDownloadMarc(BibsDownloadMarc):
    """
    This is a base exporter class for converting Sierra data to MARC
    records, to be used with a BaseSolrMarcBibsToSolr class.

    Subclass and specify a different s2marc_batch_class to change how
    Sierra records are converted to MARC.
    """

    s2marc_batch_class = S2MarcBatch

    def export_records(self, records, vals={}):
        log_label = self.__class__.__name__
        batch = type(self).s2marc_batch_class(records)
        out_recs = batch.to_marc()
        try:
            if 'marcfile' in vals:
                marcfile = batch.to_file(out_recs, vals['marcfile'])
            else:
                vals['marcfile'] = batch.to_file(out_recs, append=False)
        except IOError as e:
            self.log('Error', 'Error writing to output file: {}'.format(e), 
                     log_label)
        else:
            for e in batch.errors:
                self.log('Warning', 'Record {}: {}'.format(e.id, e.msg),
                         log_label)
            if 'success_count' in vals:
                vals['success_count'] += batch.success_count
            else:
                vals['success_count'] = batch.success_count
        return vals


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

    @classmethod
    def solr_url(cls, ctype):
        host, port = settings.SOLR_HOST, settings.SOLR_PORT
        return 'http://{}:{}/solr/{}'.format(host, port, cls.cores[ctype])

    @classmethod
    def solr_conn(cls, ctype):
        return pysolr.Solr(cls.solr_url(ctype))

    def export_records(self, records, vals={}):
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
        ret_vals = bib_converter.export_records(records, vals={})
        filename = ret_vals['marcfile']
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
                        self.log('Error', line, log_label)

        os.remove(filepath)
        return vals

    def get_record_id(self, record):
        return 'base.bibrecord.{}'.format(record.id)

    def delete_records(self, records, vals={}):
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
        return vals

    def final_callback(self, vals={}, status='success'):
        bibs_solr = type(self).solr_conn('bibs')
        log_label = type(self).__name__
        self.log('Info', 'Committing updates to Solr...', log_label)
        bibs_solr.commit()


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
