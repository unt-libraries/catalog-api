"""
Exporters module for catalog-api `blacklight` app, dev01 version.
"""

from __future__ import unicode_literals
import logging
import subprocess
import os
import re
import shlex

import pysolr

from django.conf import settings

from export.basic_exporters import BibsToSolr


class BibsToBlacklightDev01(BibsToSolr):
    
    cores = {'bibs': 'blacklight-dev-01'}

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
        index_prop = 'dev_index.properties'
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
               '-jar "{}" {} {}').format(bibs_solr_url, index_prop, jarfile,
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

    def delete_records(self, records, vals={}):
        bibs_solr = type(self).solr_conn('bibs')
        log_label = type(self).__name__
        for r in records:
            try:
                bibs_solr.delete(id='base.bibrecord.{}'.format(r.id),
                                 commit=False)
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
