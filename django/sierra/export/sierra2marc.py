'''
sierra2marc.py defines a class (S2MarcBatch) for parsing the Sierra
models out into MARC21 using Pymarc.
'''
import re
import codecs
import sys
import pymarc
from time import time as timestamp

from django.conf import settings

from utils import helpers


class S2MarcError(Exception):
    def __init__(self, message, record_id):
        self.msg = message
        self.id = record_id
    
    def __str__(self):
        return 'Record {}: {}'.format(self.id, self.msg)


class S2MarcBatch(object):
    '''
    Sierra to MARC21 Batch converter: instantiate this class to
    generate MARC21 records from a queryset of BibRecords.
    '''

    cn_type_subfield_mapping = {
        'lc': 'c',
        'dewey': 'd',
        'sudoc': 'e',
        'other': 'f'
    } 

    def __init__(self, records):
        if (hasattr(records, '__iter__')):
            self.records = records
        else:
            self.records = [records]

        self.errors = []
        self.success_count = 0

    def _one_to_marc(self, r):
        '''
        Converts one record to a pymarc.record.Record object. Returns
        the object. Note that the III record number is stored in 907$a
        and the database ID is stored in 907$b. Other metadata fields
        are stored in 9XXs as needed, to ease conversion from MARC to
        Solr.
        '''
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
        recnum = r.record_metadata.get_iii_recnum(True)
        suppressed = 'true' if r.is_suppressed else 'false'
        material_type = r.bibrecordproperty_set.all()[0].material\
            .materialpropertyname.name
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

        if re.match(r'[0-9]', marc_record.as_marc()[5]):
            raise S2MarcError('Skipped. MARC record exceeds 99,999 bytes.', 
                              str(r))

        return marc_record

    def to_marc(self):
        '''
        Converts all self.records to pymarc record objects and
        returns an array of them. Stores errors in self.errors.
        '''
        marc_records = []
        for r in self.records:
            try:
                marc_records.append(self._one_to_marc(r))
            except S2MarcError as e:
                self.errors.append(e)
        self.success_count = len(marc_records)
        return marc_records

    def _write_records(self, records, file_handle):
        #utf8_writer = codecs.getwriter('utf8')
        #writer = pymarc.writer.MARCWriter(utf8_writer(file_handle))
        writer = pymarc.writer.MARCWriter(file_handle)
        success_count = 0
        for r in records:
            try:
                writer.write(r)
            except Exception as e:
                rec_num = r.get_fields('907')[0].get_subfields('a')[0]
                self.errors.append(S2MarcError('Could not write '
                        'record to file. {}'.format(e), str(rec_num)))
            else:
                success_count += 1
        return success_count

    def to_file(self, marc_records, filename='{}.mrc'.format(timestamp()),
                filepath='{}'.format(settings.MEDIA_ROOT), append=True):
        '''
        Writes MARC21 file to disk.
        '''
        self.success_count = 0
        # If the file exists and append is True, we want to open the
        # file up, read in the MARC records, then append our
        # marc_records to that.
        existing_records = []
        if filepath[-1] != '/':
            filepath = '{}/'.format(filepath)
        try:
            marcfile = file('{}{}'.format(filepath, filename), 'r')
        except IOError:
            pass
        else:
            if append:
                reader = pymarc.MARCReader(marcfile)
                existing_records.extend(reader)
            else:
                # If we're not appending but we found an existing file,
                # let's find a new filename that doesn't exist.
                file_exists = True
                while file_exists:
                    filename = '{}.mrc'.format(timestamp())
                    try:
                        file('{}{}'.format(filepath, filename), 'r')
                    except IOError:
                        file_exists = False

        try:
            marcfile = file('{}{}'.format(filepath, filename), 'w')
        except IOError:
            raise
        if existing_records:
            self._write_records(existing_records, marcfile)
        self.success_count = self._write_records(marc_records, marcfile)
        marcfile.close()
        return filename
        