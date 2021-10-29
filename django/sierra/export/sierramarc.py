# -*- coding: utf-8 -*-

"""
Make/represent MARC records from Sierra data.
"""
from __future__ import absolute_import

import re
from time import time as timestamp

import pymarc
from django.conf import settings
from six.moves import range
from utils import helpers


class SierraToMarcError(Exception):
    def __init__(self, message, record_id):
        self.msg = message
        self.id = record_id

    def __str__(self):
        return 'Record {}: {}'.format(self.id, self.msg)


class SierraMarcField(pymarc.field.Field):
    """
    Subclass of pymarc field.Field; adds `group_tag` (III field group tag)
    to the Field object.
    """
    def __init__(self, tag, indicators=None, subfields=None, data=None,
                 group_tag=None):
        kwargs = {'tag': tag}
        if data is None:
            kwargs['indicators'] = indicators or [' ', ' ']
            kwargs['subfields'] = subfields or []
        else:
            kwargs['data'] = data
        super(SierraMarcField, self).__init__(**kwargs)
        self.group_tag = group_tag or ' '
        self.full_tag = ''.join((self.group_tag, tag))

    def matches_tag(self, tag):
        """
        Does this field match the provided `tag`? The `tag` may be the
        MARC field tag ('100'), the group_field tag ('a'), or both
        ('a100').
        """
        return tag in (self.tag, self.group_tag, self.full_tag)

    def filter_subfields(self, include=None, exclude=None):
        """
        Filter subfields on this field based on the provided subfields
        to `include` or `exclude`. Both, either, or neither args may
        be provided, and they may be strings ('abcde'), lists, or
        tuples. Conceptually, the set of SFs to exclude is substracted
        from the set of SFs to include; if `include` is None, then the
        "include" set includes all SFs. A subfield listed in `exclude`
        will always be excluded. Include='abcde', exclude='a' will only
        include subfields b, c, d, and e. Include=None and exclude='a'
        will include all subfields except a.

        Produces a generator that yields a sftag, val tuple for each
        matching subfield, in the order they appear on the field.
        """
        incl, excl = include or '', exclude or ''
        for tag, val in self:
            if ((incl and tag in incl) or not incl) and tag not in excl:
                yield (tag, val)

    @classmethod
    def make_from_string(cls, fstring):
        """
        Parse `fstring`, a formatted MARC field string, and generate a
        SierraMarcField object.

        `fstring` may follow any of several patterns.
            - An LC-docs style string.
                  100   1#$aBullett, Gerald William,$d1894-1958.
            - An OCLC-docs style string.
                  100  1  Bullett ǂd 1894-1958.
            - A MarcEdit-style string.
                  =100  1\\$aBullett, Gerald William,$d1894-1958.
            - A III Sierra-style string (field group tag is optional).
                  a100 1  Bullett, Gerald William,|d1894-1958.
            - A combination of the above.
                  a100 1# $aBullett, Gerald William,$d1894-1958.
            - A control field.
                  001 ocn012345678
            - A control field that specifies a char-position range.
                  008/18-21 b###

        Take care with spacing following the field tag.
            - If this is a control field, then any spaces between the
              field tag and first non-space character are removed; the
              first legitimate data value should not be space; use '#'
              or '\' for blank. Spaces may be used throughout the rest
              of the field data.
            - Otherwise, it attempts to determine two indicator values
              which each may be 0-9, space, #, or \\. If spaces are used
              for indicator values in combination with spaces used for
              separation, then every attempt is made to interpret them
              correctly but it may be ambiguous. The first non-space
              character will be considered the first indicator value
              unless positioned against the subfield data.
                  `100  1 $a` -- Indicators are 1 and blank.
                  `100  1$a` -- Indicators are blank and 1.
        """
        fstring = re.sub(r'\n\s*', ' ', fstring)
        tag_match = re.match(r'[\s=]*([a-z])?(\d{3})(/[\d\-]+)?(.*)$', fstring)
        group_tag, tag, cps, remainder = tag_match.groups()
        data, ind, sfs = None, None, None

        if int(tag) < 10:
            data = remainder.lstrip().replace('#', ' ').replace('\\', ' ')
            if cps:
                start = int(cps[1:].split('-')[0])
                data = ''.join([' ' * start, data])
        else:
            rem_match = re.match(r'\s*([\s\d#\\])\s*([\s\d#\\])\s*(?=\S)(.*)$',
                                 remainder)
            ind = ''.join(rem_match.groups()[0:2])
            ind = ind.replace('#', ' ').replace('\\', ' ')
            sf_str = re.sub(r'\s?ǂ(.)\s', r'$\1', rem_match.group(3))
            if sf_str[0] != '$':
                sf_str = '$a{}'.format(sf_str)
            sfs = re.split(r'[$|](.)', sf_str)[1:]
        return cls(tag, data=data, subfields=sfs, indicators=ind,
                   group_tag=group_tag)


class SierraMarcRecord(pymarc.record.Record):
    """
    Extends pymarc's `record.Record`.
        - Allows you to pass the III `record_num` on __init__ as a
          kwarg and saves it as an object attribute.
        - Assuming your added fields use type SierraMarcField, it
          overrides the `get_field` method so you can get fields by
          field group tag as well as MARC tag.
    """
    def __init__(self, *args, **kwargs):
       self.record_num = kwargs.pop('record_num', None)
       super(SierraMarcRecord, self).__init__(*args, **kwargs)

    def _field_matches_tag(self, field, tag):
        try:
            return field.matches_tag(tag)
        except AttributeError:
            return field.tag == tag

    def get_fields_gen(self, *args):
        """
        Same as `get_fields`, but it's a generator.
        """
        no_args = len(args) == 0
        for f in self.fields:
            if no_args:
                yield f
            else:
                for arg in args:
                    if self._field_matches_tag(f, arg):
                        yield f
                        break

    def get_fields(self, *args):
        """
        Return a list of fields (in the order they appear on the
        record) that match the given list of args. Args may include
        MARC tags ('100'), III field group tags ('a'), or both
        ('a100'). 'a100' would get all a-tagged 100 fields; separate
        'a' and '100' args would get all a-tagged fields and all 100
        fields.

        This returns a list to maintain compatibility with the parent
        class `get_fields` method. Use `get_fields_gen` if you want a
        generator instead.
        """
        return list(self.get_fields_gen(*args))

    def filter_fields(self, include=None, exclude=None):
        """
        Like `get_fields_gen` but lets you provide a list of tags to
        include and a list to exclude. All tags should be ones such as
        what's defined in the `get_fields` docstring.
        """
        include, exclude = include or tuple(), exclude or tuple()
        for f in self.get_fields_gen(*include):
            if all([not self._field_matches_tag(f, ex) for ex in exclude]):
                yield f


class SierraToMarcConverter(object):
    """
    Generate SierraMarcRecords from a queryset of Sierra BibRecords.
    """
    def __init__(self):
        self.reset()

    def reset(self):
        self.errors = []
        self.success_count = 0

    def compile_leader(self, r, base):
        try:
            lf = r.record_metadata.leaderfield_set.all()[0]
        except IndexError:
            return base

        return ''.join([
            base[0:5], lf.record_status_code, lf.record_type_code,
            lf.bib_level_code, lf.control_type_code,
            lf.char_encoding_scheme_code, base[10:17], lf.encoding_level_code,
            lf.descriptive_cat_form_code, lf.multipart_level_code, base[20:]
        ])

    def compile_control_fields(self, r):
        mfields = []
        try:
            control_fields = r.record_metadata.controlfield_set.all()
        except Exception as e:
            msg = "Skipped. Couldn't retrieve control fields. ({})".format(e)
            raise SierraToMarcError(msg, str(r))
        for cf in control_fields:
            try:
                data = cf.get_data()
                field = SierraMarcField(cf.get_tag(), data=data)
            except Exception as e:
                
                msg = ("Skipped. Couldn't create MARC field for {}. ({})"
                       "".format(cf.get_tag(), e))
                raise SierraToMarcError(msg, str(r))
            mfields.append(field)
        return mfields

    def order_varfields(self, varfields):
        groups = []
        vfgroup_ordernum, last_vftag = 0, None
        for vf in sorted(varfields, key=lambda vf: vf.marc_tag or ''):
            if vf.marc_tag:
                if last_vftag and last_vftag != vf.varfield_type_code:
                    vfgroup_ordernum += 1
                sort_key = (vfgroup_ordernum * 1000) + vf.occ_num
                groups.append((sort_key, vf))
                last_vftag = vf.varfield_type_code
        return [vf for _, vf in sorted(groups, key=lambda r: r[0])]

    def compile_varfields(self, r):
        mfields = []
        try:
            varfields = r.record_metadata.varfield_set.all()
        except Exception as e:
            msg = "Skipped. Couldn't retrieve varfields. ({})".format(e)
            raise SierraToMarcError(msg, str(r))
        for vf in self.order_varfields(varfields):
            tag, ind1, ind2 = vf.marc_tag, vf.marc_ind1, vf.marc_ind2
            content, field = vf.field_content, None
            try:
                if tag in ['{:03}'.format(num) for num in range(1, 10)]:
                    field = SierraMarcField(tag, data=content)
                else:
                    ind = [ind1, ind2]
                    if not content.startswith('|'):
                        content = ''.join(('|a', content))
                    sf = re.split(r'\|([a-z0-9])', content)[1:]
                    field = SierraMarcField(tag, indicators=ind, subfields=sf,
                                            group_tag=vf.varfield_type_code)
            except Exception as e:
                msg = ("Skipped. Couldn't create MARC field for {}. ({})"
                       "".format(vf.marc_tag, e))
                raise SierraToMarcError(msg, str(r))
            if field is not None:
                mfields.append(field)
        return mfields

    def compile_original_marc(self, r):
        record_num = r.record_metadata.get_iii_recnum(False)
        marc_record = SierraMarcRecord(force_utf8=True, record_num=record_num)
        marc_record.add_field(*self.compile_control_fields(r))
        marc_record.add_field(*self.compile_varfields(r))
        marc_record.leader = self.compile_leader(r, marc_record.leader)
        if not marc_record.fields:
            msg = 'Skipped. No MARC fields on Bib record.'
            raise SierraToMarcError(msg, str(r))
        return marc_record

    def to_marc(self, records, reset=True):
        """
        Converts all `records` to SierraMarc record objects and
        returns an array of them. Stores errors in self.errors.
        """
        if reset:
            self.reset()
        marc_records = []
        for r in records:
            try:
                marc_records.append(self.compile_original_marc(r))
            except SierraToMarcError as e:
                self.errors.append(e)
        self.success_count = len(marc_records)
        return marc_records

    def _write_marc_records(self, marc_records, file_handle):
        writer = pymarc.writer.MARCWriter(file_handle)
        success_count = 0
        for mr in marc_records:
            try:
                writer.write(mr)
            except Exception as e:
                msg = 'Could not write record to file. {}'.format(e)
                self.errors.append(SierraToMarcError(msg, str(mr.record_num)))
            else:
                success_count += 1
        return success_count

    def to_file(self, marc_records, filename=None, filepath=None, append=True):
        """
        Writes MARC21 file to disk.
        """
        filename = filename or '{}.mrc'.format(timestamp())
        filepath = filepath or '{}'.format(settings.MEDIA_ROOT)
        self.success_count = 0
        # If the file exists and append is True, we want to open the
        # file up, read in the MARC records, then append our
        # marc_records to that.
        existing_records = []
        if filepath[-1] != '/':
            filepath = '{}/'.format(filepath)
        try:
            marcfile = open('{}{}'.format(filepath, filename), 'r')
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
                        open('{}{}'.format(filepath, filename), 'r')
                    except IOError:
                        file_exists = False
        try:
            marcfile = open('{}{}'.format(filepath, filename), 'w')
        except IOError:
            raise
        if existing_records:
            self._write_marc_records(existing_records, marcfile)
        self.success_count = self._write_marc_records(marc_records, marcfile)
        marcfile.close()
        return filename

