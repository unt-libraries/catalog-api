"""
Exporters module for catalog-api `blacklight` app, dev01 version.
"""

from __future__ import unicode_literals
import pymarc
import logging
import subprocess
import os
import re
import shlex

import pysolr

from django.conf import settings

from export.sierra2marc import S2MarcBatch
from utils import helpers
from .exporters import BaseSolrMarcBibsToSolr, BaseBibsDownloadMarc


class S2MarcBatchDemo(S2MarcBatch):
    
    """
    Sierra to MARC converter for the Blacklight demo site.
    """

    def _one_to_marc(self, r):
        marc_record = pymarc.record.Record(force_utf8=True)
        try:
            control_fields = r.record_metadata.controlfield_set.all()
        except Exception as e:
            raise S2MarcError('Skipped. Couldn\'t retrieve control fields. '
                    '({})'.format(e), str(r))
        for cf in control_fields:
            try:
                data = cf.get_data()
                field = pymarc.field.Field(tag=cf.get_tag(), data=data)
                marc_record.add_ordered_field(field)
            except Exception as e:
                raise S2MarcError('Skipped. Couldn\'t create MARC field '
                    'for {}. ({})'.format(cf.get_tag(), e), str(r))
        try:
            varfields = r.record_metadata.varfield_set\
                        .exclude(marc_tag=None)\
                        .exclude(marc_tag='')\
                        .order_by('marc_tag')
        except Exception as e:
            raise S2MarcError('Skipped. Couldn\'t retrieve varfields. '
                    '({})'.format(e), str(r))
        for vf in varfields:
            tag = vf.marc_tag
            ind1 = vf.marc_ind1
            ind2 = vf.marc_ind2
            content = vf.field_content
            try:
                if tag in ['{:03}'.format(num) for num in range(1,10)]:
                    field = pymarc.field.Field(tag=tag, data=content)
                else:
                    field = pymarc.field.Field(
                            tag=tag,
                            indicators=[ind1, ind2],
                            subfields=re.split(r'\|([a-z0-9])', content)[1:]
                    )
                    if tag == '856' and field['u'] is not None:
                        field['u'] = re.sub(r'^([^ ]+) ".*$', r'\1',
                                            field['u'])
                marc_record.add_ordered_field(field)
            except Exception as e:
                raise S2MarcError('Skipped. Couldn\'t create MARC field '
                        'for {}. ({})'.format(vf.marc_tag, e), str(r))
                break
        if not marc_record.fields:
            raise S2MarcError('Skipped. No MARC fields on Bib record.', str(r))
        # Now add various metadata to the 907 field, starting with the
        # record number
        recnum = r.record_metadata.get_iii_recnum(False)
        suppressed = 'true' if r.is_suppressed else 'false'
        # material_type = r.bibrecordproperty_set.all()[0].material\
        #     .materialpropertyname_set.all()[0].name
        material_type = r.bibrecordproperty_set.all()[0].material.code
        metadata_field = pymarc.field.Field(
                tag='907',
                indicators=[' ', ' '],
                subfields=['a', '.{}'.format(recnum), 'b', str(r.id), 
                           'c', suppressed, 'd', material_type]
        )
        # Add a list of attached items to the 908 field.
        marc_record.add_ordered_field(metadata_field)
        for item_link in r.bibrecorditemrecordlink_set.all():
            item = item_link.item_record
            item_field = pymarc.field.Field(
                tag='908',
                indicators=[' ', ' '],
                subfields=['a', item.record_metadata.get_iii_recnum(True),
                           'b', str(item.pk)]
            )
            marc_record.add_ordered_field(item_field)
        # For each call number in the record, add a 909 field.
        i = 0
        for cn, ctype in r.get_call_numbers():
            subfield_data = []

            if i == 0:
                try:
                    srt = helpers.NormalizedCallNumber(cn, ctype).normalize()
                except helpers.CallNumberError:
                    srt = helpers.NormalizedCallNumber(cn, 'other').normalize()
                subfield_data = ['a', cn, 'b', srt]

            subfield_data.extend([self.cn_type_subfield_mapping[ctype], cn])

            cn_field = pymarc.field.Field(
                tag='909',
                indicators=[' ', ' '],
                subfields=subfield_data
            )
            marc_record.add_ordered_field(cn_field)
            i += 1

        # If this record has a media game facet field: clean it up,
        # split by semicolon, and put into 910$a (one 910, and one $a
        # per token)
        media_tokens = self._record_get_media_game_facet_tokens(r, marc_record)
        if media_tokens is not None:
            mf_subfield_data = []
            for token in media_tokens:
                mf_subfield_data += ['a', token]
            mf_field = pymarc.field.Field(
                tag='910',
                indicators=[' ', ' '],
                subfields = mf_subfield_data
            )
            marc_record.add_ordered_field(mf_field)

        if re.match(r'[0-9]', marc_record.as_marc()[5]):
            raise S2MarcError('Skipped. MARC record exceeds 99,999 bytes.', 
                              str(r))

        return marc_record


class BibsDownloadMarcDemo(BaseBibsDownloadMarc):
    
    s2marc_batch_class = S2MarcBatchDemo


class BibsToBlacklightDemo(BaseSolrMarcBibsToSolr):
    
    bib2marc_class = BibsDownloadMarcDemo
    cores = {'bibs': 'bl-demo'}
