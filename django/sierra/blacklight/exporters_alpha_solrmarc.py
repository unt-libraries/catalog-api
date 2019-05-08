"""
Exporters module for catalog-api `blacklight` app, alpha-solrmarc.
"""

from __future__ import unicode_literals
import logging

from export import models as em
from export.exporter import Exporter
from export.basic_exporters import combine_table_lists
from .exporters import BaseSolrMarcBibsToSolr, BaseBibsDownloadMarc


class BibsToAlphaSolrmarc(BaseSolrMarcBibsToSolr):
    
    bib2marc_class = BaseBibsDownloadMarc
    cores = {'bibs': 'alpha-solrmarc'}

    def get_record_id(self, record):
        return record.get_iii_recnum(False)


class BibsToAlphaSmAndAttachedToSolr(Exporter):

    model_name = 'BibRecord'
    etype_names = ('bibs_and_attached_to_solr', 'bibs_to_alpha_solrmarc')

    def __init__(self, *args, **kwargs):
        super(BibsToAlphaSmAndAttachedToSolr, self).__init__(*args, **kwargs)
        attached_et = em.ExportType.objects.get(pk='BibsAndAttachedToSolr')
        to_alpha_sm_et = em.ExportType.objects.get(pk='BibsToAlphaSolrmarc')
        attached_class = attached_et.get_exporter_class()
        to_alpha_sm_class = to_alpha_sm_et.get_exporter_class()
        args = (self.instance.pk, self.export_filter, self.export_type,
                self.options)
        bibs_and_attached_to_solr = attached_class(*args)
        bibs_to_alpha_solrmarc = to_alpha_sm_class(*args)
        children = (bibs_and_attached_to_solr, bibs_to_alpha_solrmarc)
        prefetch = self.combine_table_lists('prefetch_related', children)
        select = self.combine_table_lists('select_related', children)
        self.deletion_filter = bibs_and_attached_to_solr.deletion_filter
        self.max_rec_chunk = bibs_and_attached_to_solr.max_rec_chunk
        self.prefetch_related = prefetch
        self.select_related = select
        self.bibs_and_attached_to_solr = bibs_and_attached_to_solr
        self.bibs_to_alpha_solrmarc = bibs_to_alpha_solrmarc

    def combine_table_lists(self, tlist_attr, exporters):
        table_lists = [getattr(exp, tlist_attr) for exp in exporters]
        return combine_table_lists(table_lists)

    def compile_vals(self, results):
        pre_vals = {et: [] for et in self.etype_names}
        vals = {}
        for v in results:
            if v is not None:
                for et in self.etype_names:
                    pre_vals[et].append(v.get(et, []))
        return {
            et: getattr(self, et).compile_vals(pre_vals[et])
                for et in self.etype_names
        }

    def export_records(self, records):
        vals = {}
        for et in self.etype_names:
            vals[et] = getattr(self, et).export_records(records)
        return vals

    def delete_records(self, records):
        vals = {}
        for et in self.etype_names:
            vals[et] = getattr(self, et).delete_records(records)
        return vals

    def final_callback(self, vals=None, status='success'):
        vals = vals or {}
        for et in self.etype_names:
            getattr(self, et).final_callback(vals.get(et, None), status)
