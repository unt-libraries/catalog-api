"""
These are models for the Sierra Database.

NOTES:
Models below are organized the same way the Sierra DNA is organized.
(http://techdocs.iii.com/sierradna/)

*_myuser tables are problematic--they don't have a proper foreign key to the
table that they should hang off of, and they're generally redundant with
existing tables. I think they're meant to be used in contexts where you have
different languages in use for different Sierra users. I've left their
corresponding models in, but they don't have relationship fields included,
unless the relationship is explicitly supposed to be there (e.g. there's an
actual foreign key in the database table/view).

Most *_view tables are redundant, so I'm commenting them out. If you want to
use them, just uncomment them to enable them.

Primary keys: most of the important views/tables have ID fields as PKs, but
there are a ton of them that have no single PK. This is a problem for Django,
as it doesn't support composite keys, and every model HAS to have a PK. If a
model doesn't specify a PK directly, then Django assumes the presence of an
id field. This causes errors when no id field actually exists. A workaround is
to specify that one of the fields in the model is a PK even if it isn't (and
even if it isn't unique); this supposedly will work(ish) for read access,
which is all we care about. The other option is just to switch to SqlAlchemy.

Codes and *_property tables: Where possible and appropriate, 'code' fields are
linked up as foreign keys. Locations, for instance: where location codes are
present in a table, the location code is a foreign key to the Location table.
In many cases there are *_property tables that simply have an id, a code, and a
display_order. When the code is present in some other table, it's then linked
as a foreign key to the _property table. For example: acq_type_property.code
and order_record.acq_type_code. I've linked order_record.acq_type_code up as a
foreign key relationship to acq_type_property.code, even though it's not
technically a foreign key in the database structure. In some cases this is a
little bit confusing, as there are *_code fields that DON'T have a
corresponding _property table or otherwise can't be linked and so are left as
codes in the model. Notably, the user-defined codes (acode1 & 2, bcode1-3,
etc.) can't be linked to the user_defined_property table (or the similar
_myuser tables) due to lack of a unique foreign key. User_defined_property
contains codes for ALL *codeN user defined fields, so individual codes aren't
unique. I think the _myuser tables can have multiple language representations
for each code, so codes here probably aren't unique either.

For the most part, what models and fields are available here doesn't depend on
the configuration of your III system. Fields and tables for products or options
that you don't have or use will remain empty. There are a few exceptions, where
empty tables translate to broken relationships that can cause problems when
using the models. For these few exceptions, I've tried to find and disable the
offending relationships. I've tried to write tests in tests.py that test for
these, under the SierraViewerModelDbConfigTests class.

Record linking (e.g., between III record types) is hella confusing.
RecordMetadata is sort of the "master" table for all records of all types, but
there are also tables for each individual type of record--bibs, items,
holdings/checkins, etc. Tables that link to a III record sometimes specify
RecordMetadata and sometimes they don't. Conceptually it's unclear whether
something links to the RM table or the record-specific table. In practice, it
doesn't really matter--the RM.id field and the record-specific table IDs are
exactly the same, so you could use either. For purposes of the models, we have
to be specific. For the sake of consistency, here's what I've done:

  1. ALL references that specify a record type (bib, authority, item, etc.)
     point to the [type]_record table, even if the reference is on a column
     named [type]_record_metadata_id or something like that.
     References to "record" (e.g., through record_id) point to the
     record_metadata table--since there's no "record" table we can access.
     References from [type]_record tables back to record_metadata use
     record_metadata.
  2. I've explicitly defined Many-To-Many relationships for III records where
     it makes sense--bibs to items, holdings to items, etc.
"""

from __future__ import unicode_literals
import re

from django.db import models

from .managers import RecordManager
from utils import helpers


class ReadOnlyModel(models.Model):
    """
    Basic abstract base model to disable the save and delete methods,
    and provide a standard __unicode__ string representation.
    """

    def save(self, *args, **kwargs):
        raise NotImplementedError('Saving not allowed.')

    def delete(self, *args, **kwargs):
        raise NotImplementedError('Deleting not allowed.')

    def __unicode__(self):
        if hasattr(self, 'code'):
            return unicode(str(self.code))
        elif hasattr(self, 'record_metadata'):
            return unicode(self.record_metadata.get_iii_recnum())
        elif hasattr(self, 'record_type') and hasattr(self, 'record_num'):
            return unicode(self.get_iii_recnum())
        elif hasattr(self, 'marc_tag') and hasattr(self, 'field_content'):
            return unicode('{} {}{}{} {}'.format(
                self.varfield_type_code,
                self.marc_tag,
                self.marc_ind1,
                self.marc_ind2,
                self.field_content
            ))
        elif hasattr(self, 'marc_tag') and hasattr(self, 'content'):
            return unicode('{} {}{}{} {}{}'.format(
                self.field_type_code,
                self.marc_tag,
                self.marc_ind1,
                self.marc_ind2,
                ' |{}'.format(self.tag) if self.tag is not None else '',
                self.content
            ))
        else:
            return unicode(str(self.pk))

    class Meta(object):
        abstract = True


class MainRecordTypeModel(ReadOnlyModel):
    """
    For the main record types, we use a custom model manager. This
    class specifies this. Models for main record types should inherit
    from this class.
    """
    objects = RecordManager()  # custom manager

    class Meta(object):
        abstract = True


# ENTITIES -- GENERIC RECORD --------------------------------------------------|

class ControlField(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record = models.ForeignKey('RecordMetadata')
    varfield_type_code = models.CharField(max_length=1, blank=True)
    control_num = models.IntegerField(null=True, blank=True)
    p00 = models.CharField(max_length=1, blank=True)
    p01 = models.CharField(max_length=1, blank=True)
    p02 = models.CharField(max_length=1, blank=True)
    p03 = models.CharField(max_length=1, blank=True)
    p04 = models.CharField(max_length=1, blank=True)
    p05 = models.CharField(max_length=1, blank=True)
    p06 = models.CharField(max_length=1, blank=True)
    p07 = models.CharField(max_length=1, blank=True)
    p08 = models.CharField(max_length=1, blank=True)
    p09 = models.CharField(max_length=1, blank=True)
    p10 = models.CharField(max_length=1, blank=True)
    p11 = models.CharField(max_length=1, blank=True)
    p12 = models.CharField(max_length=1, blank=True)
    p13 = models.CharField(max_length=1, blank=True)
    p14 = models.CharField(max_length=1, blank=True)
    p15 = models.CharField(max_length=1, blank=True)
    p16 = models.CharField(max_length=1, blank=True)
    p17 = models.CharField(max_length=1, blank=True)
    p18 = models.CharField(max_length=1, blank=True)
    p19 = models.CharField(max_length=1, blank=True)
    p20 = models.CharField(max_length=1, blank=True)
    p21 = models.CharField(max_length=1, blank=True)
    p22 = models.CharField(max_length=1, blank=True)
    p23 = models.CharField(max_length=1, blank=True)
    p24 = models.CharField(max_length=1, blank=True)
    p25 = models.CharField(max_length=1, blank=True)
    p26 = models.CharField(max_length=1, blank=True)
    p27 = models.CharField(max_length=1, blank=True)
    p28 = models.CharField(max_length=1, blank=True)
    p29 = models.CharField(max_length=1, blank=True)
    p30 = models.CharField(max_length=1, blank=True)
    p31 = models.CharField(max_length=1, blank=True)
    p32 = models.CharField(max_length=1, blank=True)
    p33 = models.CharField(max_length=1, blank=True)
    p34 = models.CharField(max_length=1, blank=True)
    p35 = models.CharField(max_length=1, blank=True)
    p36 = models.CharField(max_length=1, blank=True)
    p37 = models.CharField(max_length=1, blank=True)
    p38 = models.CharField(max_length=1, blank=True)
    p39 = models.CharField(max_length=1, blank=True)
    p40 = models.CharField(max_length=1, blank=True)
    p41 = models.CharField(max_length=1, blank=True)
    p42 = models.CharField(max_length=1, blank=True)
    p43 = models.CharField(max_length=1, blank=True)
    occ_num = models.IntegerField(null=True, blank=True)
    remainder = models.CharField(max_length=100, null=True, blank=True)

    def get_tag(self):
        return '{:03}'.format(self.control_num)

    def get_data(self):
        p_fields = ['p{:02}'.format(num) for num in range(0, 44)]
        return ''.join([getattr(self, p) for p in p_fields])

    class Meta(ReadOnlyModel.Meta):
        db_table = 'control_field'


class FixfldType(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    record_type = models.ForeignKey('RecordType',
                                    db_column='record_type_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    is_enabled = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fixfld_type'


class FixfldTypeMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fixfld_type_myuser'


class FixfldTypeName(ReadOnlyModel):
    fixfldtype = models.OneToOneField(FixfldType,
                                   primary_key=True,
                                   db_column='fixfld_property_id')
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fixfld_type_name'


# note: record_type_code is MARC record type (language materials, etc.),
# NOT III record type (bib, authority, item, etc.)
class LeaderField(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record = models.ForeignKey('RecordMetadata', null=True, blank=True)
    record_status_code = models.CharField(max_length=1, blank=True)
    record_type_code = models.CharField(max_length=1, blank=True)
    bib_level_code = models.CharField(max_length=1, blank=True)
    control_type_code = models.CharField(max_length=1, blank=True)
    char_encoding_scheme_code = models.CharField(max_length=1, blank=True)
    encoding_level_code = models.CharField(max_length=1, blank=True)
    descriptive_cat_form_code = models.CharField(max_length=1, blank=True)
    multipart_level_code = models.CharField(max_length=1, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'leader_field'


class MarclabelType(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record_type = models.ForeignKey('RecordType',
                                    db_column='record_type_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    varfield_type_code = models.CharField(max_length=1, blank=True)
    marc_tag_pattern = models.CharField(max_length=50, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'marclabel_type'


class MarclabelTypeMyuser(ReadOnlyModel):
    record_type = models.ForeignKey('RecordType',
                                    db_column='record_type_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    varfield_type_code = models.CharField(max_length=1, blank=True)
    marctag_pattern = models.CharField(max_length=50, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'marclabel_type_myuser'


class MarclabelTypeName(ReadOnlyModel):
    marclabel_type = models.OneToOneField(MarclabelType, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'marclabel_type_name'


# Note from Sierra DNA: "USE WITH CAUTION: The contents of this view are
# subject to change ... " etc.
class PhraseEntry(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record = models.ForeignKey('RecordMetadata', null=True, blank=True)
    index_tag = models.CharField(max_length=20, blank=True)
    varfield_type_code = models.CharField(max_length=1, blank=True)
    occurrence = models.IntegerField(null=True, blank=True)
    is_permuted = models.NullBooleanField(null=True, blank=True)
    type2 = models.IntegerField(null=True, blank=True)
    type3 = models.CharField(max_length=1, blank=True)
    index_entry = models.CharField(max_length=512, blank=True)
    insert_title = models.CharField(max_length=256, blank=True)
    phrase_rule_rule_num = models.IntegerField(null=True, blank=True)
    phrase_rule_operation = models.CharField(max_length=1, blank=True)
    phrase_rule_subfield_list = models.CharField(max_length=50, blank=True)
    original_content = models.CharField(max_length=1000, blank=True)
    parent_record = models.ForeignKey('RecordMetadata',
                                      related_name='+',
                                      null=True,
                                      blank=True)
    insert_title_tag = models.CharField(max_length=1, blank=True)
    insert_title_occ = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'phrase_entry'


# Note: For some reason the phrase_rule table doesn't appear in the Sierra DNA,
# even though it's obviously referenced in phrase_entry. Also, phrase_type_
# code doesn't correspond with anything in the phrase_type table.
class PhraseRule(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record_type = models.ForeignKey('RecordType',
                                    db_column='record_type_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    varfield_type_code = models.CharField(max_length=1, blank=True)
    marc_tag_pattern = models.CharField(max_length=50, blank=True)
    operation = models.CharField(max_length=1, blank=True)
    subfield_list = models.CharField(max_length=50, blank=True)
    phrase_type_code = models.CharField(max_length=1, blank=True)
    rule_num = models.IntegerField(null=True, blank=True)
    is_continue = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'phrase_rule'


class PhraseType(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record_type = models.ForeignKey('RecordType',
                                    db_column='record_type_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    varfield_type_code = models.CharField(max_length=1, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'phrase_type'


class PhraseTypeName(ReadOnlyModel):
    phrase_type = models.OneToOneField(PhraseType, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)
    plural_name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'phrase_type_name'


# We don't use agencies. Enable 'agency,' below, if you do.
class RecordMetadata(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record_type = models.ForeignKey('RecordType',
                                    db_column='record_type_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    record_num = models.IntegerField(null=True, blank=True)
    creation_date_gmt = models.DateTimeField(null=True, blank=True)
    deletion_date_gmt = models.DateField(null=True, blank=True)
    campus_code = models.CharField(max_length=5, blank=True)
    # agency = models.ForeignKey('AgencyProperty',
    #                             db_column='agency_code_num',
    #                             to_field='code_num',
    #                             null=True,
    #                             blank=True)
    num_revisions = models.IntegerField(null=True, blank=True)
    record_last_updated_gmt = models.DateTimeField(null=True, blank=True)
    previous_last_updated_gmt = models.DateTimeField(null=True, blank=True)
    objects = RecordManager()  # custom manager    
    # This maps III record type letters to Django model names
    record_type_models = {
        'b': 'BibRecord',
        'i': 'ItemRecord',
        'a': 'AuthorityRecord',
        'r': 'CourseRecord',
        'o': 'OrderRecord',
        'p': 'PatronRecord',
        'e': 'ResourceRecord',
        't': 'ContactRecord',
        'c': 'HoldingRecord',
        'l': 'LicenseRecord',
    }

    @classmethod
    def get_record_type(cls, model_name):
        """
        Given a model name, this returns the record type code that is
        associated with it. E.g.:
        RecordMetadata.get_record_type('BibRecord') returns 'b'.
        """
        for rt in cls.record_type_models:
            if cls.record_type_models[rt] == model_name:
                return rt
        return None

    def get_iii_recnum(self, use_check_digit=False):
        """
        Returns the full III record number. If use_check_digit is True, it
        calculates and appends the check digit for you.
        """
        rn = int(self.record_num)
        full_rec_num = '{}{}'.format(self.record_type, self.record_num)
        if use_check_digit:
            check_digit = (sum([(rn / (10 ** i) % 10) * (i + 2)
                                for i in range(0, 6)])
                           + (rn / (10 ** 6)) * 8) % 11
            if check_digit == 10:
                check_digit = "x"
            full_rec_num = '{}{}'.format(full_rec_num, check_digit)
        return full_rec_num

    def get_full_record(self):
        """
        Returns the "full" record object attached to this
        RecordMetadata object--e.g., the ItemRecord, BibRecord,
        PatronRecord, etc. None if there is no attached record of the
        correct type (as happens with deleted records).
        """
        try:
            model_name = self.record_type_models[self.record_type.code].lower()
        except KeyError:
            return None
        record_set = getattr(self, '{}_set'.format(model_name))
        full_record = None
        try:
            full_record = record_set.all()[0]
        except IndexError:
            pass
        return full_record

    def get_full_record_model(self):
        """
        Returns the model class of the "full" record object attached to
        this RecordMetadata object.
        """
        return self.get_full_record()._meta.concrete_model

    class Meta(ReadOnlyModel.Meta):
        db_table = 'record_metadata'


class RecordRange(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record_type = models.ForeignKey('RecordType',
                                    db_column='record_type_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    accounting_unit = models.ForeignKey('AccountingUnit',
                                        db_column='accounting_unit_code_num',
                                        to_field='code_num',
                                        null=True,
                                        blank=True)
    start_num = models.IntegerField(null=True, blank=True)
    last_num = models.IntegerField(null=True, blank=True)
    current_count = models.IntegerField(null=True, blank=True)
    deleted_count = models.IntegerField(null=True, blank=True)
    max_num = models.IntegerField(null=True, blank=True)
    size = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'record_range'


class RecordType(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=1, unique=True, blank=True)
    tag = models.CharField(max_length=1, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'record_type'


class RecordTypeMyuser(ReadOnlyModel):
    code = models.CharField(max_length=1, primary_key=True)
    tag = models.CharField(max_length=1, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'record_type_myuser'


class RecordTypeName(ReadOnlyModel):
    record_type = models.OneToOneField(RecordType, primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'record_type_name'


class Subfield(ReadOnlyModel):
    record = models.ForeignKey(RecordMetadata, null=True, blank=True)
    varfield = models.OneToOneField('Varfield', primary_key=True)
    field_type_code = models.CharField(max_length=1, blank=True)
    marc_tag = models.CharField(max_length=3, null=True, blank=True)
    marc_ind1 = models.CharField(max_length=1, blank=True)
    marc_ind2 = models.CharField(max_length=1, blank=True)
    occ_num = models.IntegerField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    tag = models.CharField(max_length=1, null=True, blank=True)
    content = models.CharField(max_length=20001, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'subfield'


# class SubfieldView(ReadOnlyModel):
#     record = models.ForeignKey(RecordMetadata, null=True, blank=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     varfield = models.ForeignKey('Varfield', null=True, blank=True)
#     varfield_type_code = models.CharField(max_length=1, blank=True)
#     marc_tag = models.CharField(max_length=3, blank=True)
#     marc_ind1 = models.CharField(max_length=1, blank=True)
#     marc_ind2 = models.CharField(max_length=1, blank=True)
#     occ_num = models.IntegerField(null=True, blank=True)
#     display_order = models.IntegerField(null=True, blank=True)
#     tag = models.CharField(max_length=1, blank=True)
#     content = models.CharField(max_length=20001, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'subfield_view'

class Varfield(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record = models.ForeignKey(RecordMetadata, null=True, blank=True)
    varfield_type_code = models.CharField(max_length=1, blank=True)
    marc_tag = models.CharField(max_length=3, null=True, blank=True)
    marc_ind1 = models.CharField(max_length=1, blank=True)
    marc_ind2 = models.CharField(max_length=1, blank=True)
    occ_num = models.IntegerField(null=True, blank=True)
    field_content = models.CharField(max_length=20001, blank=True)

    def display_field_content(self, subfield_replace=' ', subfields=''):
        """
        Returns semi-readable display of field content, replacing
        subfield codes (e.g., |a, |b) with the specified
        subfield_replace string. Defaults to space. The initial |a is
        always removed. If needed, specify the subfields you want to
        include and/or exclude using the subfields param. It should be
        a string indicating either subfields to include: 'abrz2' or
        exclude: '-fvy0123456789'.
        """
        content = self.field_content

        if subfields:
            if not re.search(r'^\|[a-z]', content):
                content = '|a{}'.format(content)
            content = re.sub(r'\|([^a-z0-9]|$)', r'\1', content)

            if subfields[0] == '-':
                rem_sf = subfields[1:]
            else:
                a_set = set('abcdefghijklmnopqrstuvwxyz0123456789')
                b_set = set(subfields)
                rem_sf = a_set - b_set
            rem_sf = ''.join(list(rem_sf))

            content = re.sub(r'\|[{}][^|]*'.format(rem_sf), '', content)

        return re.sub(r'\|[a-z0-9]', subfield_replace,
                      re.sub(r'^\|[a-z0-9]', '', content))

    class Meta(ReadOnlyModel.Meta):
        db_table = 'varfield'


class VarfieldType(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record_type = models.ForeignKey(RecordType,
                                    db_column='record_type_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    code = models.CharField(max_length=1, unique=True, blank=True)
    marc_tag = models.CharField(max_length=3, blank=True)
    is_enabled = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'varfield_type'


class VarfieldTypeName(ReadOnlyModel):
    varfield_type = models.OneToOneField(VarfieldType, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    short_name = models.CharField(max_length=20, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'varfield_type_name'


# class VarfieldView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record = models.ForeignKey(RecordMetadata, null=True, blank=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code',
#                                     null=True,
#                                     blank=True)
#     record_num = models.IntegerField(null=True, blank=True)
#     varfield_type_code = models.CharField(max_length=1, blank=True)
#     marc_tag = models.CharField(max_length=3, blank=True)
#     marc_ind1 = models.CharField(max_length=1, blank=True)
#     marc_ind2 = models.CharField(max_length=1, blank=True)
#     occ_num = models.IntegerField(null=True, blank=True)
#     field_content = models.CharField(max_length=20001, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'varfield_view'

# ENTITIES -- AUTHORITY -------------------------------------------------------|

class AuthorityRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    marc_type_code = models.CharField(max_length=1, blank=True)
    code1 = models.CharField(max_length=1, blank=True)
    code2 = models.CharField(max_length=1, blank=True)
    suppress_code = models.CharField(max_length=1, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'authority_record'


# class AuthorityView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code',
#                                     null=True,
#                                     blank=True)
#     record_num = models.IntegerField(null=True, blank=True)
#     marc_type_code = models.CharField(max_length=1, blank=True)
#     code1 = models.CharField(max_length=1, blank=True)
#     code2 = models.CharField(max_length=1, blank=True)
#     suppress_code = models.CharField(max_length=1, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'authority_view'

class Catmaint(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    is_locked = models.NullBooleanField(null=True, blank=True)
    is_viewed = models.NullBooleanField(null=True, blank=True)
    condition_code_num = models.IntegerField(null=True, blank=True)
    index_tag = models.CharField(max_length=1, blank=True)
    index_entry = models.TextField(blank=True)
    record_metadata = models.ForeignKey(RecordMetadata, null=True, blank=True)
    statistics_group_code_num = models.IntegerField(null=True, blank=True)
    process_gmt = models.DateTimeField(null=True, blank=True)
    program_code = models.CharField(max_length=255, blank=True)
    iii_user_name = models.CharField(max_length=255, blank=True)
    one_xx_entry = models.TextField(blank=True)
    authority_record = models.ForeignKey(AuthorityRecord,
                                         db_column='authority_record_metadata_id',
                                         null=True,
                                         blank=True)
    old_field = models.TextField(blank=True)
    new_240_field = models.TextField(blank=True)
    field = models.TextField(blank=True)
    cataloging_date_gmt = models.DateTimeField(null=True, blank=True)
    index_prev = models.TextField(blank=True)
    index_next = models.TextField(blank=True)
    correct_heading = models.TextField(blank=True)
    author = models.TextField(blank=True)
    title = models.TextField(blank=True)
    phrase_entry = models.ForeignKey(PhraseEntry, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'catmaint'


class UserDefinedAcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_acode1_myuser'


class UserDefinedAcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_acode2_myuser'


# ENTITIES -- BIB -------------------------------------------------------------|

class BibLevelProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_level_property'


class BibLevelPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_level_property_myuser'


class BibLevelPropertyName(ReadOnlyModel):
    bib_level_property = models.OneToOneField(BibLevelProperty, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_level_property_name'


class BibRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    holding_records = models.ManyToManyField('HoldingRecord',
                                             through='BibRecordHoldingRecordLink',
                                             blank=True)
    item_records = models.ManyToManyField('ItemRecord',
                                          through='BibRecordItemRecordLink',
                                          blank=True)
    order_records = models.ManyToManyField('OrderRecord',
                                           through='BibRecordOrderRecordLink',
                                           blank=True)
    volume_records = models.ManyToManyField('VolumeRecord',
                                            through='BibRecordVolumeRecordLink',
                                            blank=True)
    locations = models.ManyToManyField('Location',
                                       through='BibRecordLocation',
                                       blank=True)
    language = models.ForeignKey('LanguageProperty',
                                 db_column='language_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    bcode1 = models.CharField(max_length=1, blank=True)
    bcode2 = models.CharField(max_length=1, blank=True)
    bcode3 = models.CharField(max_length=1, blank=True)
    country = models.ForeignKey('CountryProperty',
                                db_column='country_code',
                                to_field='code',
                                null=True,
                                blank=True)
    index_change_count = models.IntegerField(null=True, blank=True)
    is_on_course_reserve = models.NullBooleanField(null=True, blank=True)
    is_right_result_exact = models.NullBooleanField(null=True, blank=True)
    allocation_rule_code = models.CharField(max_length=1, null=True, blank=True)
    skip_num = models.IntegerField(null=True, blank=True)
    cataloging_date_gmt = models.DateTimeField(null=True, blank=True)
    marc_type_code = models.CharField(max_length=1, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    def get_call_numbers(self):
        bib_cn_specs = [
            {'vf_tag': 'c', 'marc_tags':
                ['050', '055', '090', '091', '093', '094', '095', '096', '097',
                 '098'],
             'type': 'lc'},
            {'vf_tag': 'c', 'marc_tags': '092', 'type': 'dewey'},
            {'vf_tag': 'c', 'marc_tags': '099', 'type': 'other'},
            {'vf_tag': 'c', 'marc_tags': '086', 'sf': '-z', 'type': 'sudoc'},
            {'vf_tag': 'g', 'marc_tags': '086', 'sf': '-z', 'type': 'sudoc'}
        ]

        cn_tuples = []
        varfields = self.record_metadata.varfield_set.all()
        for spec in bib_cn_specs:
            sf_param = {'subfields': spec.get('sf', '')}
            cns = helpers.get_varfield_vals(varfields, spec['vf_tag'],
                                            marc_tags=spec['marc_tags'],
                                            cm_kw_params=sf_param,
                                            content_method='display_field_content',
                                            many=True)
            if cns:
                cns = [(cn, spec['type']) for cn in cns]
                cn_tuples.extend(cns)

        return cn_tuples

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_record'


class BibRecordCallNumberPrefix(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    bib_record = models.ForeignKey(BibRecord, null=True, blank=True)
    call_number_prefix = models.CharField(max_length=10, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_record_call_number_prefix'


class BibRecordLocation(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    bib_record = models.ForeignKey(BibRecord, null=True, blank=True)
    copies = models.IntegerField(null=True, blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True,
                                 db_constraint=False)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_record_location'


class BibRecordProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    bib_record = models.ForeignKey(BibRecord, null=True, blank=True)
    best_title = models.CharField(max_length=1000, blank=True)
    bib_level = models.ForeignKey(BibLevelProperty,
                                  db_column='bib_level_code',
                                  to_field='code',
                                  null=True,
                                  blank=True)
    material = models.ForeignKey('MaterialProperty',
                                 db_column='material_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    publish_year = models.IntegerField(null=True, blank=True)
    best_title_norm = models.CharField(max_length=1000, blank=True)
    best_author = models.CharField(max_length=1000, blank=True)
    best_author_norm = models.CharField(max_length=1000, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_record_property'


# class BibView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code',
#                                     null=True,
#                                     blank=True)
#     record_num = models.IntegerField(null=True, blank=True)
#     language = models.ForeignKey('LanguageProperty',
#                                  db_column='language_code',
#                                  to_field='code',
#                                  null=True,
#                                  blank=True)
#     bcode1 = models.CharField(max_length=1, blank=True)
#     bcode2 = models.CharField(max_length=1, blank=True)
#     bcode3 = models.CharField(max_length=1, blank=True)
#     country = models.ForeignKey('CountryProperty',
#                                 db_column='country_code',
#                                 to_field='code',
#                                 null=True,
#                                 blank=True)
#     is_available_at_library = models.NullBooleanField(null=True, blank=True)
#     index_change_count = models.IntegerField(null=True, blank=True)
#     allocation_rule_code = models.CharField(max_length=1, blank=True)
#     is_on_course_reserve = models.NullBooleanField(null=True, blank=True)
#     is_right_result_exact = models.NullBooleanField(null=True, blank=True)
#     skip_num = models.IntegerField(null=True, blank=True)
#     cataloging_date_gmt = models.DateTimeField(null=True, blank=True)
#     marc_type_code = models.CharField(max_length=1, blank=True)
#     title = models.CharField(max_length=1000, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'bib_view'

class CourseRecordBibRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    course_record = models.ForeignKey('CourseRecord', null=True, blank=True)
    bib_record = models.ForeignKey(BibRecord, null=True, blank=True)
    status_change_date = models.DateTimeField(null=True, blank=True)
    status_code = models.CharField(max_length=5, blank=True)
    bibs_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'course_record_bib_record_link'


# class CourseView is under the COURSE entity

# class ItypePropertyCategoryMyuser is under the ITEM entity

class MaterialProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_public = models.NullBooleanField(null=True, blank=True)
    material_property_category = models.ForeignKey('MaterialPropertyCategory',
                                                   null=True,
                                                   blank=True)
    physical_format = models.ForeignKey('PhysicalFormat', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'material_property'


class MaterialPropertyCategory(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(unique=True, null=True, blank=True)
    is_default = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'material_property_category'


class MaterialPropertyCategoryMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_default = models.NullBooleanField(null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'material_property_category_myuser'


class MaterialPropertyCategoryName(ReadOnlyModel):
    material_property_category = models.OneToOneField(MaterialPropertyCategory,
                                                   primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'material_property_category_name'


class MaterialPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_public = models.NullBooleanField(null=True, blank=True)
    material_property_category = models.ForeignKey(MaterialPropertyCategory,
                                                   null=True,
                                                   blank=True)
    physical_format = models.ForeignKey('PhysicalFormat', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'material_property_myuser'


class MaterialPropertyName(ReadOnlyModel):
    material_property = models.OneToOneField(MaterialProperty, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)
    facet_text = models.CharField(max_length=500, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'material_property_name'


class UserDefinedBcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_bcode1_myuser'


class UserDefinedBcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_bcode2_myuser'


class UserDefinedBcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_bcode3_myuser'


# ENTITIES -- CONTACT ---------------------------------------------------------|

class ContactRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    code = models.CharField(max_length=5, unique=True, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'contact_record'


class ContactRecordAddressType(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=1, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'contact_record_address_type'


# class ContactView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_metadata = models.ForeignKey(RecordMetadata,
#                                         db_column='record_id',
#                                         null=True,
#                                         blank=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code',
#                                     null=True,
#                                     blank=True)
#     record_num = models.IntegerField(null=True, blank=True)
#     code = models.CharField(max_length=5, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'contact_view'

# ENTITIES -- COURSE ----------------------------------------------------------|

class CourseRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    bib_records = models.ManyToManyField(BibRecord,
                                         through=CourseRecordBibRecordLink,
                                         blank=True)
    item_records = models.ManyToManyField('ItemRecord',
                                          through='CourseRecordItemRecordLink',
                                          blank=True)
    begin_date = models.DateTimeField(null=True, blank=True)
    end_date = models.DateTimeField(null=True, blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    ccode1 = models.CharField(max_length=20, blank=True)
    ccode2 = models.CharField(max_length=20, blank=True)
    ccode3 = models.CharField(max_length=20, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'course_record'


# class CourseView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     begin_date = models.DateTimeField(null=True, blank=True)
#     end_date = models.DateTimeField(null=True, blank=True)
#     location = models.ForeignKey('Location',
#                                  db_column='location_code',
#                                  to_field='code',
#                                  null=True,
#                                  blank=True)
#     ccode1 = models.CharField(max_length=20, blank=True)
#     ccode2 = models.CharField(max_length=20, blank=True)
#     ccode3 = models.CharField(max_length=20, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#          db_table = 'course_view'

class UserDefinedCcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ccode1_myuser'


class UserDefinedCcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ccode2_myuser'


class UserDefinedCcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ccode3_myuser'


# ENTITIES -- HOLDING ---------------------------------------------------------|

class BibRecordHoldingRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    bib_record = models.ForeignKey(BibRecord, null=True, blank=True)
    holding_record = models.OneToOneField('HoldingRecord',
                                       unique=True,
                                       null=True,
                                       blank=True)
    holdings_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_record_holding_record_link'


class HoldingRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    item_records = models.ManyToManyField('ItemRecord',
                                          through='HoldingRecordItemRecordLink',
                                          blank=True)
    locations = models.ManyToManyField('Location',
                                       through='HoldingRecordLocation',
                                       blank=True)
    is_inherit_loc = models.NullBooleanField(null=True, blank=True)
    allocation_rule_code = models.CharField(max_length=1, null=True, blank=True)
    accounting_unit = models.ForeignKey('AccountingUnit',
                                        db_column='accounting_unit_code_num',
                                        to_field='code_num',
                                        null=True,
                                        blank=True)
    label_code = models.CharField(max_length=1, blank=True)
    scode1 = models.CharField(max_length=1, blank=True)
    scode2 = models.CharField(max_length=1, blank=True)
    claimon_date_gmt = models.DateTimeField(null=True, blank=True)
    receiving_location = models.ForeignKey('ReceivingLocationProperty',
                                           db_column='receiving_location_code',
                                           to_field='code',
                                           null=True,
                                           blank=True)
    vendor_record = models.ForeignKey('VendorRecord',
                                      db_column='vendor_code',
                                      to_field='code',
                                      null=True,
                                      blank=True)
    scode3 = models.CharField(max_length=1, blank=True)
    scode4 = models.CharField(max_length=1, blank=True)
    update_cnt = models.CharField(max_length=1, blank=True)
    piece_cnt = models.IntegerField(null=True, blank=True)
    echeckin_code = models.CharField(max_length=1, blank=True)
    media_type_code = models.CharField(max_length=1, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record'


class HoldingRecordAddressType(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=1, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record_address_type'


class HoldingRecordBox(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    holding_record_cardlink = models.ForeignKey('HoldingRecordCardlink',
                                                null=True,
                                                blank=True)
    item_records = models.ManyToManyField('ItemRecord',
                                          through='HoldingRecordBoxItem',
                                          blank=True)
    box_count = models.IntegerField(null=True, blank=True)
    enum_level_a = models.CharField(max_length=256, blank=True)
    enum_level_b = models.CharField(max_length=256, blank=True)
    enum_level_c = models.CharField(max_length=256, blank=True)
    enum_level_d = models.CharField(max_length=256, blank=True)
    enum_level_e = models.CharField(max_length=256, blank=True)
    enum_level_f = models.CharField(max_length=256, blank=True)
    enum_level_g = models.CharField(max_length=256, blank=True)
    enum_level_h = models.CharField(max_length=256, blank=True)
    chron_level_i = models.CharField(max_length=256, blank=True)
    chron_level_i_trans_date = models.CharField(max_length=256, blank=True)
    chron_level_j = models.CharField(max_length=256, blank=True)
    chron_level_j_trans_date = models.CharField(max_length=256, blank=True)
    chron_level_k = models.CharField(max_length=256, blank=True)
    chron_level_k_trans_date = models.CharField(max_length=256, blank=True)
    chron_level_l = models.CharField(max_length=256, blank=True)
    chron_level_l_trans_date = models.CharField(max_length=256, blank=True)
    chron_level_m = models.CharField(max_length=256, blank=True)
    chron_level_m_trans_date = models.CharField(max_length=256, blank=True)
    note = models.CharField(max_length=256, blank=True)
    box_status_code = models.CharField(max_length=1, blank=True)
    claim_cnt = models.IntegerField(null=True, blank=True)
    copies_cnt = models.IntegerField(null=True, blank=True)
    url = models.CharField(max_length=256, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)
    staff_note = models.CharField(max_length=256, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record_box'


class HoldingRecordBoxItem(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    holding_record_box = models.ForeignKey(HoldingRecordBox,
                                           null=True,
                                           blank=True)
    item_record = models.ForeignKey('ItemRecord',
                                    db_column='item_record_metadata_id',
                                    null=True,
                                    blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record_box_item'


class HoldingRecordCard(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    holding_record = models.ForeignKey(HoldingRecord, null=True, blank=True)
    status_code = models.CharField(max_length=1, blank=True)
    display_format_code = models.CharField(max_length=1, blank=True)
    is_suppress_opac_display = models.NullBooleanField(null=True, blank=True)
    order_record = models.ForeignKey('OrderRecord',
                                     db_column='order_record_metadata_id',
                                     null=True,
                                     blank=True)
    is_create_item = models.NullBooleanField(null=True, blank=True)
    is_usmarc = models.NullBooleanField(null=True, blank=True)
    is_marc = models.NullBooleanField(null=True, blank=True)
    is_use_default_enum = models.NullBooleanField(null=True, blank=True)
    is_use_default_date = models.NullBooleanField(null=True, blank=True)
    update_method_code = models.CharField(max_length=1, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record_card'


class HoldingRecordCardlink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    holding_record_card = models.ForeignKey(HoldingRecordCard,
                                            null=True,
                                            blank=True)
    card_type_code = models.CharField(max_length=1, blank=True)
    link_count = models.IntegerField(null=True, blank=True)
    enum_level_a = models.CharField(max_length=256, blank=True)
    enum_level_a_disp_mode = models.CharField(max_length=1, blank=True)
    enum_level_b = models.CharField(max_length=256, blank=True)
    enum_level_b_limit = models.IntegerField(null=True, blank=True)
    enum_level_b_rollover = models.CharField(max_length=1, blank=True)
    enum_level_b_disp_mode = models.CharField(max_length=1, blank=True)
    enum_level_c = models.CharField(max_length=256, blank=True)
    enum_level_c_limit = models.IntegerField(null=True, blank=True)
    enum_level_c_rollover = models.CharField(max_length=1, blank=True)
    enum_level_c_disp_mode = models.CharField(max_length=1, blank=True)
    enum_level_d = models.CharField(max_length=256, blank=True)
    enum_level_d_limit = models.IntegerField(null=True, blank=True)
    enum_level_d_rollover = models.CharField(max_length=1, blank=True)
    enum_level_d_disp_mode = models.CharField(max_length=1, blank=True)
    enum_level_e = models.CharField(max_length=256, blank=True)
    enum_level_e_limit = models.IntegerField(null=True, blank=True)
    enum_level_e_rollover = models.CharField(max_length=1, blank=True)
    enum_level_e_disp_mode = models.CharField(max_length=1, blank=True)
    enum_level_f = models.CharField(max_length=256, blank=True)
    enum_level_f_limit = models.IntegerField(null=True, blank=True)
    enum_level_f_rollover = models.CharField(max_length=1, blank=True)
    enum_level_f_disp_mode = models.CharField(max_length=1, blank=True)
    alt_enum_level_g = models.CharField(max_length=256, blank=True)
    alt_enum_level_g_disp_mode = models.CharField(max_length=1, blank=True)
    alt_enum_level_h = models.CharField(max_length=256, blank=True)
    alt_enum_level_h_disp_mode = models.CharField(max_length=1, blank=True)
    chron_level_i = models.CharField(max_length=256, blank=True)
    chron_level_j = models.CharField(max_length=256, blank=True)
    chron_level_k = models.CharField(max_length=256, blank=True)
    chron_level_l = models.CharField(max_length=256, blank=True)
    chron_level_m = models.CharField(max_length=256, blank=True)
    frequency_code = models.CharField(max_length=10, blank=True)
    calendar_change = models.CharField(max_length=256, blank=True)
    opac_label = models.CharField(max_length=256, blank=True)
    is_advanced = models.NullBooleanField(null=True, blank=True)
    days_btw_iss = models.IntegerField(null=True, blank=True)
    claim_days = models.IntegerField(null=True, blank=True)
    bind_unit = models.IntegerField(null=True, blank=True)
    bind_delay = models.IntegerField(null=True, blank=True)
    is_bind_with_issue = models.NullBooleanField(null=True, blank=True)
    is_use_autumn = models.NullBooleanField(null=True, blank=True)
    enum_level_count = models.IntegerField(null=True, blank=True)
    alt_enum_level_count = models.IntegerField(null=True, blank=True)
    current_item = models.IntegerField(null=True, blank=True)
    alt_enum_level_h_limit = models.IntegerField(null=True, blank=True)
    alt_enum_level_h_rollover = models.CharField(max_length=1, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record_cardlink'


class HoldingRecordErmHolding(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    holding_record = models.ForeignKey(HoldingRecord, null=True, blank=True)
    varfield_type_code = models.CharField(max_length=1, blank=True)
    marc_tag = models.CharField(max_length=3, blank=True)
    marc_ind1 = models.CharField(max_length=1, blank=True)
    marc_ind2 = models.CharField(max_length=1, blank=True)
    occ_num = models.IntegerField(null=True, blank=True)
    field_content = models.CharField(max_length=20001, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record_erm_holding'


class HoldingRecordItemRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    holding_record = models.ForeignKey(HoldingRecord, null=True, blank=True)
    item_record = models.OneToOneField('ItemRecord',
                                    unique=True,
                                    null=True,
                                    blank=True)
    items_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record_item_record_link'


class HoldingRecordLocation(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    holding_record = models.ForeignKey(HoldingRecord, null=True, blank=True)
    copies = models.IntegerField(null=True, blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record_location'


class HoldingRecordRouting(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    holding_record = models.ForeignKey(HoldingRecord, null=True, blank=True)
    copy_num = models.IntegerField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_patron_routing = models.NullBooleanField(null=True, blank=True)
    priority_num = models.IntegerField(null=True, blank=True)
    patron_record = models.ForeignKey('PatronRecord',
                                      db_column='patron_record_metadata_id',
                                      null=True,
                                      blank=True)
    routefile_code_num = models.IntegerField(null=True, blank=True)
    iii_user_name = models.CharField(max_length=3, blank=True)
    field_position = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'holding_record_routing'


# class HoldingView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     is_inherit_loc = models.NullBooleanField(null=True, blank=True)
#     allocation_rule_code = models.CharField(max_length=1, blank=True)
#     accounting_unit = models.ForeignKey('AccountingUnit',
#                                         db_column='accounting_unit_code_num',
#                                         to_field='code_num',
#                                         blank=True,
#                                         null=True)
#     label_code = models.CharField(max_length=1, blank=True)
#     scode1 = models.CharField(max_length=1, blank=True)
#     scode2 = models.CharField(max_length=1, blank=True)
#     update_cnt = models.CharField(max_length=1, blank=True)
#     piece_cnt = models.IntegerField(null=True, blank=True)
#     echeckin_code = models.CharField(max_length=1, blank=True)
#     media_type_code = models.CharField(max_length=1, blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'holding_view'

class ResourceRecordHoldingRecordRelatedLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    resource_record = models.ForeignKey('ResourceRecord', null=True, blank=True)
    holding_record = models.ForeignKey(HoldingRecord, null=True, blank=True)
    resources_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'resource_record_holding_record_related_link'
        # truncated verbose_name for Django 1.7 39 character limit
        verbose_name = 'resource record holding record rel link'


class UserDefinedScode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_scode1_myuser'


class UserDefinedScode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_scode2_myuser'


class UserDefinedScode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_scode3_myuser'


class UserDefinedScode4Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_scode4_myuser'


# ENTITIES -- INVOICE ---------------------------------------------------------|

class ForeignCurrency(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    accounting_unit = models.ForeignKey('AccountingUnit',
                                        db_column='accounting_code_num',
                                        to_field='code_num',
                                        blank=True,
                                        null=True)
    code = models.CharField(max_length=5, blank=True)
    rate = models.DecimalField(null=True,
                               max_digits=30,
                               decimal_places=6,
                               blank=True)
    description = models.CharField(max_length=256, blank=True)
    format = models.CharField(max_length=10, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'foreign_currency'


class InvoiceRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    accounting_unit = models.ForeignKey('AccountingUnit',
                                        db_column='accounting_unit_code_num',
                                        to_field='code_num',
                                        blank=True,
                                        null=True)
    invoice_date_gmt = models.DateTimeField(null=True, blank=True)
    paid_date_gmt = models.DateTimeField(null=True, blank=True)
    status_code = models.CharField(max_length=20, blank=True)
    posted_data_gmt = models.DateTimeField(null=True, blank=True)
    is_paid_date_received_date = models.NullBooleanField(null=True, blank=True)
    ncode1 = models.CharField(max_length=1, blank=True)
    ncode2 = models.CharField(max_length=1, blank=True)
    ncode3 = models.CharField(max_length=1, blank=True)
    invoice_number_text = models.CharField(max_length=20, blank=True)
    iii_user_name = models.CharField(max_length=20, blank=True)
    foreign_currency_code = models.CharField(max_length=20, blank=True)
    foreign_currency_format = models.CharField(max_length=30, blank=True)
    foreign_currency_exchange_rate = models.DecimalField(null=True,
                                                         max_digits=30,
                                                         decimal_places=6,
                                                         blank=True)
    tax_fund = models.ForeignKey('Fund',
                                 db_column='tax_fund_code',
                                 related_name='tax_invoicerecord_set',
                                 to_field='fund_code',
                                 null=True,
                                 blank=True)
    tax_type_code = models.CharField(max_length=30, blank=True)
    discount_amt = models.DecimalField(null=True,
                                       max_digits=30,
                                       decimal_places=6,
                                       blank=True)
    grand_total_amt = models.DecimalField(null=True,
                                          max_digits=30,
                                          decimal_places=6,
                                          blank=True)
    subtotal_amt = models.DecimalField(null=True,
                                       max_digits=30,
                                       decimal_places=6,
                                       blank=True)
    shipping_amt = models.DecimalField(null=True,
                                       max_digits=30,
                                       decimal_places=6,
                                       blank=True)
    total_tax_amt = models.DecimalField(null=True,
                                        max_digits=30,
                                        decimal_places=6,
                                        blank=True)
    use_tax_fund = models.ForeignKey('Fund',
                                     db_column='use_tax_fund_code',
                                     to_field='fund_code',
                                     related_name='usetax_invoicerecord_set',
                                     null=True,
                                     blank=True)
    use_tax_percentage_rate = models.DecimalField(null=True,
                                                  max_digits=30,
                                                  decimal_places=6,
                                                  blank=True)
    use_tax_type_code = models.CharField(max_length=10, blank=True)
    use_tax_ship_service_code = models.CharField(max_length=10, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'invoice_record'


class InvoiceRecordLine(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    invoice_record = models.ForeignKey(InvoiceRecord, null=True, blank=True)
    order_record = models.ForeignKey('OrderRecord',
                                     db_column='order_record_metadata_id',
                                     null=True,
                                     blank=True)
    paid_amt = models.DecimalField(null=True,
                                   max_digits=30,
                                   decimal_places=6,
                                   blank=True)
    lien_amt = models.DecimalField(null=True,
                                   max_digits=30,
                                   decimal_places=6,
                                   blank=True)
    lien_flag = models.IntegerField(null=True, blank=True)
    list_price = models.DecimalField(null=True,
                                     max_digits=30,
                                     decimal_places=6,
                                     blank=True)
    fund_code = models.CharField(max_length=20, blank=True)
    subfund_num = models.IntegerField(null=True, blank=True)
    copies_paid_cnt = models.IntegerField(null=True, blank=True)
    external_fund = models.ForeignKey('ExternalFundProperty',
                                      db_column='external_fund_code_num',
                                      to_field='code_num',
                                      null=True,
                                      blank=True)
    status_code = models.CharField(max_length=5, blank=True)
    note = models.CharField(max_length=20001, blank=True)
    is_single_copy_partial_pmt = models.NullBooleanField(null=True, blank=True)
    title = models.CharField(max_length=20001, blank=True)
    multiflag_code = models.CharField(max_length=1, blank=True)
    line_level_tax = models.DecimalField(null=True,
                                         max_digits=30,
                                         decimal_places=6,
                                         blank=True)
    vendor_record = models.ForeignKey('VendorRecord',
                                      db_column='vendor_code',
                                      to_field='code',
                                      null=True,
                                      blank=True)
    accounting_transaction_voucher_num = models.IntegerField(null=True,
                                                             blank=True)
    accounting_transaction_voucher_seq_num = models.IntegerField(null=True,
                                                                 blank=True)
    line_cnt = models.IntegerField(null=True, blank=True)
    invoice_record_vendor_summary = models.ForeignKey(
        'InvoiceRecordVendorSummary',
        null=True,
        blank=True)
    is_use_tax = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'invoice_record_line'


class InvoiceRecordVendorSummary(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    invoice_record = models.ForeignKey(InvoiceRecord, null=True, blank=True)
    vendor_record = models.ForeignKey('VendorRecord',
                                      db_column='vendor_code',
                                      to_field='code',
                                      null=True,
                                      blank=True)
    vendor_address_line1 = models.CharField(max_length=1000, blank=True)
    voucher_num = models.IntegerField(null=True, blank=True)
    voucher_total = models.IntegerField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'invoice_record_vendor_summary'


# class InvoiceView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     accounting_unit = models.ForeignKey('AccountingUnit',
#                                         db_column='accounting_unit_code_num',
#                                         to_field='code_num',
#                                         blank=True,
#                                         null=True)
#     invoice_date_gmt = models.DateTimeField(null=True, blank=True)
#     paid_date_gmt = models.DateTimeField(null=True, blank=True)
#     status_code = models.CharField(max_length=20, blank=True)
#     posted_date_gmt = models.DateTimeField(null=True, blank=True)
#     is_paid_date_received_date = models.NullBooleanField(null=True, blank=True)
#     ncode1 = models.CharField(max_length=1, blank=True)
#     ncode2 = models.CharField(max_length=1, blank=True)
#     ncode3 = models.CharField(max_length=1, blank=True)
#     invoice_number_text = models.CharField(max_length=20, blank=True)
#     iii_user_name = models.CharField(max_length=20, blank=True)
#     foreign_currency_code = models.CharField(max_length=20, blank=True)
#     foreign_currency_format = models.CharField(max_length=30, blank=True)
#     foreign_currency_exchange_rate = models.DecimalField(null=True,
#                                                          max_digits=30,
#                                                          decimal_places=6,
#                                                          blank=True)
#     tax_fund = models.ForeignKey('Fund',
#                                  db_column='tax_fund_code',
#                                  to_field='fund_code',
#                                  null=True,
#                                  blank=True)
#     tax_type_code = models.CharField(max_length=30, blank=True)
#     discount_amt = models.DecimalField(null=True,
#                                        max_digits=30,
#                                        decimal_places=6,
#                                        blank=True)
#     grand_total_amt = models.DecimalField(null=True,
#                                           max_digits=30,
#                                           decimal_places=6,
#                                           blank=True)
#     subtotal_amt = models.DecimalField(null=True,
#                                        max_digits=30,
#                                        decimal_places=6,
#                                        blank=True)
#     shipping_amt = models.DecimalField(null=True,
#                                        max_digits=30,
#                                        decimal_places=6,
#                                        blank=True)
#     total_tax_amt = models.DecimalField(null=True,
#                                         max_digits=30,
#                                         decimal_places=6,
#                                         blank=True)
#     use_tax_fund = models.ForeignKey('Fund',
#                                      db_column='use_tax_fund_code',
#                                      to_field='fund_code',
#                                      null=True,
#                                      blank=True)
#     use_tax_percentage_rate = models.DecimalField(null=True,
#                                                   max_digits=30,
#                                                   decimal_places=6,
#                                                   blank=True)
#     use_tax_type_code = models.CharField(max_length=10, blank=True)
#     use_tax_ship_service_code = models.CharField(max_length=10, blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'invoice_view'

class UserDefinedNcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ncode1_myuser'


class UserDefinedNcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ncode2_myuser'


class UserDefinedNcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ncode3_myuser'


# ENTITIES -- ITEM ------------------------------------------------------------|

class BibRecordItemRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    bib_record = models.ForeignKey(BibRecord, null=True, blank=True)
    item_record = models.ForeignKey('ItemRecord', null=True, blank=True)
    items_display_order = models.IntegerField(null=True, blank=True)
    bibs_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_record_item_record_link'


class CourseRecordItemRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    course_record = models.ForeignKey(CourseRecord, null=True, blank=True)
    item_record = models.ForeignKey('ItemRecord', null=True, blank=True)
    status_change_date = models.DateTimeField(null=True, blank=True)
    status_code = models.CharField(max_length=5, blank=True)
    items_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'course_record_item_record_link'


# Our system doesn't use agencies. If yours does, enable 'agency,' below.
class ItemRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    transit_box_records = models.ManyToManyField('TransitBoxRecord',
                                                 through='TransitBoxRecordItemRecord',
                                                 blank=True)
    volume_records = models.ManyToManyField('VolumeRecord',
                                            through='VolumeRecordItemRecordLink',
                                            blank=True)
    icode1 = models.IntegerField(null=True, blank=True)
    icode2 = models.CharField(max_length=1, blank=True)
    itype = models.ForeignKey('ItypeProperty',
                              db_column='itype_code_num',
                              to_field='code_num',
                              null=True,
                              blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    # agency = models.ForeignKey('AgencyProperty',
    #                            db_column='agency_code_num',
    #                            to_field='code_num',
    #                            null=True,
    #                            blank=True)
    item_status = models.ForeignKey('ItemStatusProperty',
                                    db_column='item_status_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    is_inherit_loc = models.NullBooleanField(null=True, blank=True)
    price = models.DecimalField(null=True,
                                max_digits=30,
                                decimal_places=6,
                                blank=True)
    last_checkin_gmt = models.DateTimeField(null=True, blank=True)
    checkout_total = models.IntegerField(null=True, blank=True)
    renewal_total = models.IntegerField(null=True, blank=True)
    last_year_to_date_checkout_total = models.IntegerField(null=True,
                                                           blank=True)
    year_to_date_checkout_total = models.IntegerField(null=True, blank=True)
    is_bib_hold = models.NullBooleanField(null=True, blank=True)
    copy_num = models.IntegerField(null=True, blank=True)
    checkout_statistic_group = models.ForeignKey('StatisticGroup',
                                                 db_column='checkout_statistic_group_code_num',
                                                 to_field='code_num',
                                                 related_name='checkout_itemrecord_set',
                                                 null=True,
                                                 blank=True)
    last_patron_record = models.ForeignKey('PatronRecord',
                                           db_column='last_patron_record_metadata_id',
                                           null=True,
                                           blank=True)
    inventory_gmt = models.DateTimeField(null=True, blank=True)
    checkin_statistics_group = models.ForeignKey('StatisticGroup',
                                                 null=True,
                                                 db_column='checkin_statistics_group_code_num',
                                                 related_name='checkin_itemrecord_set',
                                                 to_field='code_num',
                                                 blank=True)
    use3_count = models.IntegerField(null=True, blank=True)
    last_checkout_gmt = models.DateTimeField(null=True, blank=True)
    internal_use_count = models.IntegerField(null=True, blank=True)
    copy_use_count = models.IntegerField(null=True, blank=True)
    item_message_code = models.CharField(max_length=1, blank=True)
    opac_message_code = models.CharField(max_length=1, blank=True)
    virtual_type_code = models.CharField(max_length=1, null=True, blank=True)
    virtual_item_central_code_num = models.IntegerField(null=True, blank=True)
    holdings_code = models.CharField(max_length=1, blank=True)
    save_itype = models.ForeignKey('ItypeProperty',
                                   db_column='save_itype_code_num',
                                   to_field='code_num',
                                   related_name='save_itemrecord_set',
                                   null=True,
                                   blank=True)
    save_location = models.ForeignKey('Location',
                                      db_column='save_location_code',
                                      to_field='code',
                                      related_name='save_itemrecord_set',
                                      null=True,
                                      blank=True)
    save_checkout_total = models.IntegerField(null=True, blank=True)
    old_location = models.ForeignKey('Location',
                                     db_column='old_location_code',
                                     to_field='code',
                                     related_name='old_itemrecord_set',
                                     null=True,
                                     blank=True)
    distance_learning_status = models.SmallIntegerField(null=True, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    def get_call_numbers(self):
        """
        Returns a list of tuples, one per call number found on the
        item: (call_number, type), where type = sudoc, dewey, lc, or
        other. Call numbers are prioritized so that the first call
        number in the list is (most likely) the "main" call number.
        """
        item_cn_specs = [
            {'vf_tag': 'c', 'marc_tags': ['050', '055', '090'], 'type': 'lc'},
            {'vf_tag': 'c', 'marc_tags': '092', 'type': 'dewey'},
            {'vf_tag': 'c', 'marc_tags':
                ['091', '093', '094', '095', '096', '097', '098', '099'],
             'type': 'other'},
            {'vf_tag': 'c', 'marc_tags': None, 'type': 'other'},
            {'vf_tag': 'c', 'marc_tags': '086', 'sf': '-z', 'type': 'sudoc'},
            {'vf_tag': 'g', 'marc_tags': '*', 'sf': '-z', 'type': 'sudoc'},
        ]
        call_number_tuples = []
        varfields = self.record_metadata.varfield_set.all()
        for spec in item_cn_specs:
            call_numbers = []
            sf_param = {'subfields': spec.get('sf', '')}
            call_numbers = helpers.get_varfield_vals(varfields, spec['vf_tag'],
                                                     marc_tags=spec[
                                                         'marc_tags'],
                                                     cm_kw_params=sf_param,
                                                     content_method='display_field_content',
                                                     many=True)
            if call_numbers:
                call_numbers = [(cn, spec['type']) for cn in call_numbers]
                call_number_tuples.extend(call_numbers)
        return call_number_tuples

    def _cn_is_sudoc(self, cn_string, bib_cn_tuples):
        """
        Takes a call number string and a list of bib call number
        tuples. Returns True if the cn_string matches a sudoc number
        in the list of bib_cn_tuples.
        """
        is_sudoc = False
        if cn_string and (cn_string, 'sudoc') in bib_cn_tuples:
            is_sudoc = True
        return is_sudoc

    def _item_is_probably_shelved_by_title(self, cn_string):
        """
        Takes a call number string and tries to determine whether this
        item is probably shelved by title. If the ITYPE is 5 (bound
        periodicals) and the call number is just one or two letters
        (like M or MT), or if the word "periodical" appears in the call
        number string, or if there's ANY c-tagged field on the item
        with "SHELVED BY TITLE," returns True, else returns False.
        """
        probably_shelved_by_title = (
            self.itype.code_num == 5 and (not cn_string
                or re.search(r'^[A-Za-z]{,2}$', cn_string))
            or (cn_string and
                re.search(r'^periodical', cn_string, re.IGNORECASE)))

        if not probably_shelved_by_title:
            item_vfs = self.record_metadata.varfield_set.all()
            for cn in helpers.get_varfield_vals(item_vfs, 'c', many=True,
                    content_method='display_field_content'):
                if cn.strip().upper() == 'SHELVED BY TITLE':
                    probably_shelved_by_title = True
                    break
        return probably_shelved_by_title

    def _add_title_to_cn_string(self, cn_string):
        """
        Takes a call number string, appends the bib record title onto
        the end, and returns it. Useful for generating the call number
        string for items that are shelved by title.
        """
        bib = self.bibrecorditemrecordlink_set.all()[0].bib_record
        title = bib.bibrecordproperty_set.all()[0].best_title
        title = re.sub(r'\.*\s*$', r'', title)
        cn_string = '{} -- {}'.format(cn_string, title)
        return cn_string

    def get_shelving_call_number_tuple(self):
        """
        Returns a tuple that should represent the call number used
        to shelve this item: (cn_string, cn_type), where the cn_string
        is usable for sorting a list of items in call number order
        (after normalization).
        """
        item_cn_tuples = self.get_call_numbers()
        try:
            bib_cn_tuples = (self.bibrecorditemrecordlink_set.all()[0]
                                 .bib_record.get_call_numbers())
        except IndexError:
            bib_cn_tuples = []

        cn_string, cn_type = (None, None)
        if len(item_cn_tuples) > 0:
            (cn_string, cn_type) = item_cn_tuples[0]
        elif len(bib_cn_tuples) > 0:
            (cn_string, cn_type) = bib_cn_tuples[0]

        if self._cn_is_sudoc(cn_string, bib_cn_tuples):
            cn_type = 'sudoc'

        if self._item_is_probably_shelved_by_title(cn_string):
            cn_string = self._add_title_to_cn_string(cn_string)
            cn_type = 'other'

        return (cn_string, cn_type)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'item_record'


class ItemRecordProperty(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    item_record = models.OneToOneField(ItemRecord,
                                    unique=True,
                                    null=True,
                                    blank=True)
    call_number = models.CharField(max_length=1000, blank=True)
    call_number_norm = models.CharField(max_length=1000, blank=True)
    barcode = models.CharField(max_length=1000, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'item_record_property'


class ItemStatusProperty(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=1, blank=True, unique=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'item_status_property'


class ItemStatusPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=1, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'item_status_property_myuser'


class ItemStatusPropertyName(ReadOnlyModel):
    item_status_property = models.OneToOneField(ItemStatusProperty,
                                             primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'item_status_property_name'


# class ItemView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     barcode = models.CharField(max_length=1000, blank=True)
#     icode1 = models.IntegerField(null=True, blank=True)
#     icode2 = models.CharField(max_length=1, blank=True)
#     itype = models.ForeignKey('ItypeProperty',
#                               db_column='itype_code_num',
#                               to_field='code_num',
#                               null=True,
#                               blank=True)
#     location = models.ForeignKey('Location',
#                                  db_column='location_code',
#                                  to_field='code',
#                                  null=True,
#                                  blank=True)
#     agency = models.ForeignKey('AgencyProperty',
#                                db_column='agency_code_num',
#                                to_field='code_num',
#                                null=True,
#                                blank=True)
#     item_status = models.ForeignKey('ItemStatusProperty',
#                                     db_column='item_status_code',
#                                     to_field='code',
#                                     null=True,
#                                     blank=True)
#     is_inherit_loc = models.NullBooleanField(null=True, blank=True)
#     price = models.DecimalField(null=True,
#                                 max_digits=30,
#                                 decimal_places=6,
#                                 blank=True)
#     last_checkin_gmt = models.DateTimeField(null=True, blank=True)
#     checkout_total = models.IntegerField(null=True, blank=True)
#     renewal_total = models.IntegerField(null=True, blank=True)
#     last_year_to_date_checkout_total = models.IntegerField(null=True,
#                                                            blank=True)
#     year_to_date_checkout_total = models.IntegerField(null=True, blank=True)
#     is_bib_hold = models.NullBooleanField(null=True, blank=True)
#     copy_num = models.IntegerField(null=True, blank=True)
#     checkout_statistic_group = models.ForeignKey('StatisticGroup',
#                                                  db_column='checkout_statistic_group_code_num',
#                                                  to_field='code_num',
#                                                  null=True,
#                                                  blank=True)
#     last_patron_record = models.ForeignKey('PatronRecord',
#                                            db_column='last_patron_record_metadata_id',
#                                            null=True,
#                                            blank=True)
#     inventory_gmt = models.DateTimeField(null=True, blank=True)
#     checkin_statistic_group = models.ForeignKey('StatisticGroup',
#                                                 db_column='checkin_statistic_group_code_num',
#                                                 to_field='code_num',
#                                                 null=True,
#                                                 blank=True)
#     use3_count = models.IntegerField(null=True, blank=True)
#     last_checkout_gmt = models.DateTimeField(null=True, blank=True)
#     internal_use_count = models.IntegerField(null=True, blank=True)
#     copy_use_count = models.IntegerField(null=True, blank=True)
#     item_message_code = models.CharField(max_length=1, blank=True)
#     opac_message_code = models.CharField(max_length=1, blank=True)
#     virtual_type_code = models.CharField(max_length=1, blank=True)
#     virtual_item_central_code_num = models.IntegerField(null=True, blank=True)
#     holdings_code = models.CharField(max_length=1, blank=True)
#     save_itype = models.ForeignKey('ItypeProperty',
#                                    db_column='save_itype_code_num',
#                                    to_field='code_num',
#                                    null=True,
#                                    blank=True)
#     save_location = models.ForeignKey('Location',
#                                       db_column='save_location_code',
#                                       to_field='code',
#                                       null=True,
#                                       blank=True)
#     save_checkout_total = models.IntegerField(null=True, blank=True)
#     old_location = models.ForeignKey('Location',
#                                      db_column='old_location_code',
#                                      to_field='code',
#                                      null=True,
#                                      blank=True)
#     distance_learning_status = models.SmallIntegerField(null=True, blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'item_view'

class ItypeProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code_num = models.IntegerField(null=True, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    itype_property_category = models.ForeignKey('ItypePropertyCategory',
                                                null=True,
                                                blank=True)
    physical_format = models.ForeignKey('PhysicalFormat', null=True, blank=True)
    target_audience = models.ForeignKey('TargetAudience', null=True, blank=True)
    collection = models.ForeignKey('Collection', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'itype_property'


class ItypePropertyCategory(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(unique=True, null=True, blank=True)
    is_default = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'itype_property_category'


class ItypePropertyCategoryMyuser(ReadOnlyModel):
    code = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    itype_property_category = models.ForeignKey(ItypePropertyCategory,
                                                null=True,
                                                blank=True)
    physical_format = models.ForeignKey('PhysicalFormat', null=True, blank=True)
    target_audience = models.ForeignKey('TargetAudience', null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'itype_property_category_myuser'


class ItypePropertyCategoryName(ReadOnlyModel):
    itype_property_category = models.OneToOneField(ItypePropertyCategory,
                                                primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'itype_property_category_name'


class ItypePropertyMyuser(ReadOnlyModel):
    code = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    itype_property_category = models.ForeignKey(ItypePropertyCategory,
                                                null=True,
                                                blank=True)
    physical_format = models.ForeignKey('PhysicalFormat', null=True, blank=True)
    target_audience = models.ForeignKey('TargetAudience', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'itype_property_myuser'


class ItypePropertyName(ReadOnlyModel):
    itype_property = models.OneToOneField(ItypeProperty, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'itype_property_name'


# We have some statistic_group rows with blank location_codes.
class StatisticGroup(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code_num = models.IntegerField(unique=True, null=True, blank=True)
    location_code = models.CharField(max_length=5, blank=True)
    # location = models.ForeignKey('Location',
    #                              db_column='location_code',
    #                              to_field='code',
    #                              null=True,
    #                              blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'statistic_group'


# We have some statistic_group_myuser rows with blank location_codes.
class StatisticGroupMyuser(ReadOnlyModel):
    code = models.IntegerField(primary_key=True)
    location_code = models.CharField(max_length=5, blank=True)
    # location = models.ForeignKey('Location',
    #                              db_column='location_code',
    #                              to_field='code',
    #                              null=True,
    #                              blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'statistic_group_myuser'


class StatisticGroupName(ReadOnlyModel):
    statistic_group = models.OneToOneField(StatisticGroup, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'statistic_group_name'


class TransitBoxRecord(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record = models.ForeignKey(RecordMetadata, null=True, blank=True)
    barcode = models.CharField(max_length=255, unique=True, blank=True)
    description = models.CharField(max_length=256, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'transit_box_record'


class TransitBoxRecordItemRecord(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    transit_box_record = models.ForeignKey(TransitBoxRecord,
                                           null=True,
                                           blank=True)
    item_record = models.OneToOneField(ItemRecord,
                                    db_column='item_record_metadata_id',
                                    unique=True,
                                    null=True,
                                    blank=True)
    from_location = models.ForeignKey('Location',
                                      db_column='from_location_id',
                                      related_name='from_transitboxrecorditemrecord_set',
                                      null=True,
                                      blank=True)
    to_location = models.ForeignKey('Location',
                                    db_column='to_location_id',
                                    related_name='to_transitboxrecorditemrecord_set',
                                    null=True,
                                    blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'transit_box_record_item_record'


class TransitBoxStatus(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    location = models.ForeignKey('Location', null=True, blank=True)
    arrival_timestamp = models.DateTimeField(null=True, blank=True)
    transit_box_record = models.ForeignKey(TransitBoxRecord,
                                           null=True,
                                           blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'transit_box_status'


class UserDefinedIcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_icode1_myuser'


class UserDefinedIcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_icode2_myuser'


class VolumeRecordItemRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    volume_record = models.ForeignKey('VolumeRecord', null=True, blank=True)
    item_record = models.OneToOneField(ItemRecord,
                                    unique=True,
                                    null=True,
                                    blank=True)
    items_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'volume_record_item_record_link'


# ENTITIES -- LICENSE ---------------------------------------------------------|

# None of our License records have country or language codes.
class LicenseRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    accounting_unit = models.ForeignKey('AccountingUnit',
                                        db_column='accounting_unit_code_num',
                                        to_field='code_num',
                                        blank=True,
                                        null=True)
    confidential_code = models.CharField(max_length=1, blank=True)
    auto_renew_code = models.CharField(max_length=1, blank=True)
    status_code = models.CharField(max_length=1, blank=True)
    type_code = models.CharField(max_length=1, blank=True)
    change_to_code = models.CharField(max_length=1, blank=True)
    breach_procedure_code = models.CharField(max_length=1, blank=True)
    termination_procedure_code = models.CharField(max_length=1, blank=True)
    perpetual_access_code = models.CharField(max_length=1, blank=True)
    archival_provisions_code = models.CharField(max_length=1, blank=True)
    warranty_code = models.CharField(max_length=1, blank=True)
    disability_compliance_code = models.CharField(max_length=1, blank=True)
    performance_requirement_code = models.CharField(max_length=1, blank=True)
    liability_code = models.CharField(max_length=1, blank=True)
    idemnification_code = models.CharField(max_length=1, blank=True)
    law_and_venue_code = models.CharField(max_length=1, blank=True)
    user_confidentiality_code = models.CharField(max_length=1, blank=True)
    suppress_code = models.CharField(max_length=1, blank=True)
    lcode1 = models.CharField(max_length=1, blank=True)
    lcode2 = models.CharField(max_length=1, blank=True)
    lcode3 = models.CharField(max_length=1, blank=True)
    concurrent_users_count = models.IntegerField(null=True, blank=True)
    license_sign_gmt = models.DateTimeField(null=True, blank=True)
    licensor_sign_gmt = models.DateTimeField(null=True, blank=True)
    contract_start_gmt = models.DateTimeField(null=True, blank=True)
    contract_end_gmt = models.DateTimeField(null=True, blank=True)
    breach_cure = models.IntegerField(null=True, blank=True)
    cancellation_notice = models.IntegerField(null=True, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)
    ldate4 = models.DateTimeField(null=True, blank=True)
    # language = models.ForeignKey('LanguageProperty',
    #                              db_column='language_code',
    #                              to_field='code',
    #                              null=True,
    #                              blank=True)
    # country = models.ForeignKey('CountryProperty',
    #                             db_column='country_code',
    #                             to_field='code',
    #                             null=True,
    #                             blank=True)
    llang2 = models.CharField(max_length=3, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'license_record'


# class LicenseView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     accounting_unit = models.ForeignKey('AccountingUnit',
#                                         db_column='accounting_unit_code_num',
#                                         to_field='code_num',
#                                         blank=True,
#                                         null=True)
#     confidential_code = models.CharField(max_length=1, blank=True)
#     auto_renew_code = models.CharField(max_length=1, blank=True)
#     status_code = models.CharField(max_length=1, blank=True)
#     type_code = models.CharField(max_length=1, blank=True)
#     change_to_code = models.CharField(max_length=1, blank=True)
#     breach_procedure_code = models.CharField(max_length=1, blank=True)
#     termination_procedure_code = models.CharField(max_length=1, blank=True)
#     perpetual_access_code = models.CharField(max_length=1, blank=True)
#     archival_provisions_code = models.CharField(max_length=1, blank=True)
#     warranty_code = models.CharField(max_length=1, blank=True)
#     disability_compliance_code = models.CharField(max_length=1, blank=True)
#     performance_requirement_code = models.CharField(max_length=1, blank=True)
#     liability_code = models.CharField(max_length=1, blank=True)
#     idemnification_code = models.CharField(max_length=1, blank=True)
#     law_and_venue_code = models.CharField(max_length=1, blank=True)
#     user_confidentiality_code = models.CharField(max_length=1, blank=True)
#     suppress_code = models.CharField(max_length=1, blank=True)
#     lcode1 = models.CharField(max_length=1, blank=True)
#     lcode2 = models.CharField(max_length=1, blank=True)
#     lcode3 = models.CharField(max_length=1, blank=True)
#     concurrent_users_count = models.IntegerField(null=True, blank=True)
#     license_sign_gmt = models.DateTimeField(null=True, blank=True)
#     licensor_sign_gmt = models.DateTimeField(null=True, blank=True)
#     contract_start_gmt = models.DateTimeField(null=True, blank=True)
#     contract_end_gmt = models.DateTimeField(null=True, blank=True)
#     breach_cure = models.IntegerField(null=True, blank=True)
#     cancellation_notice = models.IntegerField(null=True, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'license_view'

class ResourceRecordLicenseRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    resource_record = models.ForeignKey('ResourceRecord', null=True, blank=True)
    license_record = models.ForeignKey(LicenseRecord, null=True, blank=True)
    licenses_display_order = models.IntegerField(null=True, blank=True)
    resources_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'resource_record_license_record_link'


class UserDefinedLcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_lcode1_myuser'


class UserDefinedLcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_lcode2_myuser'


class UserDefinedLcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_lcode3_myuser'


# ENTITIES -- ORDER -----------------------------------------------------------|

class AcqTypeProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'acq_type_property'


class AcqTypePropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'acq_type_property_myuser'


class AcqTypePropertyName(ReadOnlyModel):
    acq_type_property = models.OneToOneField(AcqTypeProperty, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'acq_type_property_name'


class BibRecordOrderRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    bib_record = models.ForeignKey(BibRecord, null=True, blank=True)
    order_record = models.OneToOneField('OrderRecord',
                                     unique=True,
                                     null=True,
                                     blank=True)
    orders_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_record_order_record_link'


class BillingLocationProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=1, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'billing_location_property'


class BillingLocationPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=1, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'billing_location_property_myuser'


class BillingLocationPropertyName(ReadOnlyModel):
    billing_location_property = models.OneToOneField(BillingLocationProperty,
                                                  primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'billing_location_property_name'


class ClaimActionProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'claim_action_property'


class ClaimActionPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'claim_action_property_myuser'


class ClaimActionPropertyName(ReadOnlyModel):
    claim_action_property = models.OneToOneField(ClaimActionProperty,
                                              primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'claim_action_property_name'


class FormProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'form_property'


class FormPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'form_property_myuser'


class FormPropertyName(ReadOnlyModel):
    form_property = models.OneToOneField(FormProperty, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'form_property_name'


class OrderNoteProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_note_property'


class OrderNotePropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_note_property_myuser'


class OrderNotePropertyName(ReadOnlyModel):
    order_note_property = models.OneToOneField(OrderNoteProperty, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_note_property_name'


class OrderRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    accounting_unit = models.ForeignKey('AccountingUnit',
                                        db_column='accounting_unit_code_num',
                                        to_field='code_num',
                                        blank=True,
                                        null=True)
    acq_type = models.ForeignKey(AcqTypeProperty,
                                 db_column='acq_type_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    catalog_date_gmt = models.DateTimeField(null=True, blank=True)
    claim_action = models.ForeignKey(ClaimActionProperty,
                                     db_column='claim_action_code',
                                     to_field='code',
                                     null=True,
                                     blank=True)
    ocode1 = models.CharField(max_length=1, blank=True)
    ocode2 = models.CharField(max_length=1, blank=True)
    ocode3 = models.CharField(max_length=1, blank=True)
    ocode4 = models.CharField(max_length=1, blank=True)
    estimated_price = models.DecimalField(null=True,
                                          max_digits=30,
                                          decimal_places=6,
                                          blank=True)
    form = models.ForeignKey(FormProperty,
                             db_column='form_code',
                             to_field='code',
                             null=True,
                             blank=True)
    order_date_gmt = models.DateTimeField(null=True, blank=True)
    order_note = models.ForeignKey(OrderNoteProperty,
                                   db_column='order_note_code',
                                   to_field='code',
                                   null=True,
                                   blank=True)
    order_type = models.ForeignKey('OrderTypeProperty',
                                   db_column='order_type_code',
                                   to_field='code',
                                   null=True,
                                   blank=True)
    receiving_action = models.ForeignKey('ReceivingActionProperty',
                                         db_column='receiving_action_code',
                                         to_field='code',
                                         null=True,
                                         blank=True)
    received_date_gmt = models.DateTimeField(null=True, blank=True)
    receiving_location = models.ForeignKey('ReceivingLocationProperty',
                                           db_column='receiving_location_code',
                                           to_field='code',
                                           null=True,
                                           blank=True)
    billing_location = models.ForeignKey(BillingLocationProperty,
                                         db_column='billing_location_code',
                                         to_field='code',
                                         null=True,
                                         blank=True)
    order_status = models.ForeignKey('OrderStatusProperty',
                                     db_column='order_status_code',
                                     to_field='code',
                                     null=True,
                                     blank=True)
    temporary_location = models.ForeignKey('TempLocationProperty',
                                           db_column='temporary_location_code',
                                           to_field='code',
                                           null=True,
                                           blank=True)
    vendor_record = models.ForeignKey('VendorRecord',
                                      db_column='vendor_record_code',
                                      to_field='code',
                                      null=True,
                                      blank=True)
    language = models.ForeignKey('LanguageProperty',
                                 db_column='language_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    blanket_purchase_order_num = models.CharField(max_length=10000, blank=True)
    country = models.ForeignKey('CountryProperty',
                                db_column='country_code',
                                to_field='code',
                                null=True,
                                blank=True)
    volume_count = models.IntegerField(null=True, blank=True)
    fund_allocation_rule_code = models.CharField(max_length=1, blank=True)
    reopen_text = models.CharField(max_length=255, blank=True)
    list_price = models.DecimalField(null=True,
                                     max_digits=30,
                                     decimal_places=6,
                                     blank=True)
    list_price_foreign_amt = models.DecimalField(null=True,
                                                 max_digits=30,
                                                 decimal_places=6,
                                                 blank=True)
    list_price_discount_amt = models.DecimalField(null=True,
                                                  max_digits=30,
                                                  decimal_places=6,
                                                  blank=True)
    list_price_service_charge = models.DecimalField(null=True,
                                                    max_digits=30,
                                                    decimal_places=6,
                                                    blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)
    fund_copies_paid = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_record'


class OrderRecordAddressType(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=1, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_record_address_type'


class OrderRecordCmf(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    order_record = models.ForeignKey(OrderRecord, null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    fund_code = models.CharField(max_length=20, blank=True)
    copies = models.IntegerField(null=True, blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_record_cmf'


class OrderRecordEdifactResponse(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    order_record = models.ForeignKey(OrderRecord, null=True, blank=True)
    code = models.CharField(max_length=20, blank=True)
    message = models.CharField(max_length=512, blank=True)
    event_date_gmt = models.DateTimeField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_record_edifact_response'


class OrderRecordPaid(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    order_record = models.ForeignKey(OrderRecord, null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    paid_date_gmt = models.DateTimeField(null=True, blank=True)
    paid_amount = models.DecimalField(null=True,
                                      max_digits=30,
                                      decimal_places=6,
                                      blank=True)
    foreign_paid_amount = models.DecimalField(null=True,
                                              max_digits=30,
                                              decimal_places=6,
                                              blank=True)
    foreign_code = models.CharField(max_length=10, blank=True)
    voucher_num = models.IntegerField(null=True, blank=True)
    invoice_code = models.CharField(max_length=20, blank=True)
    invoice_date_gmt = models.DateTimeField(null=True, blank=True)
    from_date_gmt = models.DateTimeField(null=True, blank=True)
    to_date_gmt = models.DateTimeField(null=True, blank=True)
    copies = models.IntegerField(null=True, blank=True)
    note = models.CharField(max_length=200, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_record_paid'


class OrderRecordReceived(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    order_record = models.ForeignKey(OrderRecord, null=True, blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    fund = models.ForeignKey('Fund',
                             db_column='fund_code',
                             to_field='fund_code',
                             null=True,
                             blank=True)
    copy_num = models.IntegerField(null=True, blank=True)
    volume_num = models.IntegerField(null=True, blank=True)
    item_record = models.ForeignKey(ItemRecord,
                                    db_column='item_record_metadata_id',
                                    null=True,
                                    blank=True)
    received_date_gmt = models.DateTimeField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_record_received'


class OrderStatusProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_status_property'


class OrderStatusPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_status_property_myuser'


class OrderStatusPropertyName(ReadOnlyModel):
    order_status_property = models.OneToOneField(OrderStatusProperty,
                                              primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_status_property_name'


class OrderTypeProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_type_property'


class OrderTypePropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_type_property_myuser'


class OrderTypePropertyName(ReadOnlyModel):
    order_type_property = models.OneToOneField(OrderTypeProperty, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'order_type_property_name'


# class OrderView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     record_metadata = models.ForeignKey(RecordMetadata,
#                                         db_column='record_id',
#                                         null=True,
#                                         blank=True)
#     accounting_unit = models.ForeignKey('AccountingUnit',
#                                         db_column='accounting_unit_code_num',
#                                         to_field='code_num',
#                                         blank=True,
#                                         null=True)
#     acq_type = models.ForeignKey(AcqTypeProperty,
#                                  db_column='acq_type_code',
#                                  to_field='code',
#                                  null=True,
#                                  blank=True)
#     catalog_date_gmt = models.DateTimeField(null=True, blank=True)
#     claim_action = models.ForeignKey(ClaimActionProperty,
#                                      db_column='claim_action_code',
#                                      to_field='code',
#                                      null=True,
#                                      blank=True)
#     ocode1 = models.CharField(max_length=1, blank=True)
#     ocode2 = models.CharField(max_length=1, blank=True)
#     ocode3 = models.CharField(max_length=1, blank=True)
#     ocode4 = models.CharField(max_length=1, blank=True)
#     estimated_price = models.DecimalField(null=True,
#                                           max_digits=30,
#                                           decimal_places=6,
#                                           blank=True)
#     form = models.ForeignKey(FormProperty,
#                              db_column='form_code',
#                              to_field='code',
#                              null=True,
#                              blank=True)
#     order_date_gmt = models.DateTimeField(null=True, blank=True)
#     order_note = models.ForeignKey(OrderNoteProperty,
#                                    db_column='order_note_code',
#                                    to_field='code',
#                                    null=True,
#                                    blank=True)
#     order_type = models.ForeignKey('OrderTypeProperty',
#                                    db_column='order_type_code',
#                                    to_field='code',
#                                    null=True,
#                                    blank=True)
#     receiving_action = models.ForeignKey('ReceivingActionProperty',
#                                          db_column='receiving_action_code',
#                                          to_field='code',
#                                          null=True,
#                                          blank=True)
#     received_date_gmt = models.DateTimeField(null=True, blank=True)
#     receiving_location = models.ForeignKey('ReceivingLocationProperty',
#                                            db_column='receiving_location_code',
#                                            to_field='code',
#                                            null=True,
#                                            blank=True)
#     billing_location = models.ForeignKey(BillingLocationProperty,
#                                          db_column='billing_location_code',
#                                          to_field='code',
#                                          null=True,
#                                          blank=True)
#     order_status = models.ForeignKey('OrderStatusProperty',
#                                      db_column='order_status_code',
#                                      to_field='code',
#                                      null=True,
#                                      blank=True)
#     temporary_location = models.ForeignKey('TempLocationProperty',
#                                            db_column='temporary_location_code',
#                                            to_field='code',
#                                            null=True,
#                                            blank=True)
#     vendor_record = models.ForeignKey('VendorRecord',
#                                       db_column='vendor_record_code',
#                                       to_field='code',
#                                       null=True,
#                                       blank=True)
#     language = models.ForeignKey('LanguageProperty',
#                                  db_column='language_code',
#                                  to_field='code',
#                                  null=True,
#                                  blank=True)
#     blanket_purchase_order_num = models.CharField(max_length=10000,
#                                                   blank=True)
#     country = models.ForeignKey('CountryProperty',
#                                 db_column='country_code',
#                                 to_field='code',
#                                 null=True,
#                                 blank=True)
#     volume_count = models.IntegerField(null=True, blank=True)
#     fund_allocation_rule_code = models.CharField(max_length=1, blank=True)
#     reopen_text = models.CharField(max_length=255, blank=True)
#     list_price = models.DecimalField(null=True,
#                                      max_digits=30,
#                                      decimal_places=6,
#                                      blank=True)
#     list_price_foreign_amt = models.DecimalField(null=True,
#                                                  max_digits=30,
#                                                  decimal_places=6,
#                                                  blank=True)
#     list_price_discount_amt = models.DecimalField(null=True,
#                                                   max_digits=30,
#                                                   decimal_places=6,
#                                                   blank=True)
#     list_price_service_charge = models.DecimalField(null=True,
#                                                     max_digits=30,
#                                                     decimal_places=6,
#                                                     blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'order_view'

class ReceivingActionProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'receiving_action_property'


class ReceivingActionPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'receiving_action_property_myuser'


class ReceivingActionPropertyName(ReadOnlyModel):
    receiving_action_property = models.OneToOneField(ReceivingActionProperty,
                                                  primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'receiving_action_property_name'


class RecordLock(ReadOnlyModel):
    id = models.OneToOneField('RecordMetadata',
                              primary_key=True,
                              db_column='id')

    class Meta(ReadOnlyModel.Meta):
        db_table = 'record_lock'


class ResourceRecordOrderRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    resource_record = models.ForeignKey('ResourceRecord', null=True, blank=True)
    order_record = models.ForeignKey(OrderRecord, null=True, blank=True)
    orders_display_order = models.IntegerField(null=True, blank=True)
    resources_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'resource_record_order_record_link'


class ResourceRecordOrderRecordRelatedLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    resource_record = models.ForeignKey('ResourceRecord', null=True, blank=True)
    order_record = models.ForeignKey(OrderRecord, null=True, blank=True)
    resources_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'resource_record_order_record_related_link'
        # truncated verbose_name for Django 1.7 39 character limit
        verbose_name = 'resource record order record rel link'


class TempLocationProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'temp_location_property'


class TempLocationPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'temp_location_property_myuser'


class TempLocationPropertyName(ReadOnlyModel):
    temp_location_property = models.OneToOneField(TempLocationProperty,
                                               primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'temp_location_property_name'


class UserDefinedOcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ocode1_myuser'


class UserDefinedOcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ocode2_myuser'


class UserDefinedOcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ocode3_myuser'


class UserDefinedOcode4Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ocode4_myuser'


# ENTITIES -- PATRON ----------------------------------------------------------|

class FirmProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=5, unique=True, blank=True)
    category_code = models.CharField(max_length=1, blank=True)
    contact_name = models.CharField(max_length=255, blank=True)
    contact_addr1 = models.CharField(max_length=255, blank=True)
    contact_addr2 = models.CharField(max_length=255, blank=True)
    contact_addr3 = models.CharField(max_length=255, blank=True)
    contact_addr4 = models.CharField(max_length=255, blank=True)
    telephone = models.CharField(max_length=255, blank=True)
    paid_thru_date = models.DateTimeField(null=True, blank=True)
    payment_info = models.CharField(max_length=255, blank=True)
    note1 = models.CharField(max_length=255, blank=True)
    note2 = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'firm_property'


class FirmPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=5, primary_key=True)
    category_code = models.CharField(max_length=1, blank=True)
    contact_name = models.CharField(max_length=255, blank=True)
    contact_addr1 = models.CharField(max_length=255, blank=True)
    contact_addr2 = models.CharField(max_length=255, blank=True)
    contact_addr3 = models.CharField(max_length=255, blank=True)
    contact_addr4 = models.CharField(max_length=255, blank=True)
    telephone = models.CharField(max_length=255, blank=True)
    paid_thru_date = models.DateTimeField(null=True, blank=True)
    payment_info = models.CharField(max_length=255, blank=True)
    note1 = models.CharField(max_length=255, blank=True)
    note2 = models.CharField(max_length=255, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'firm_property_myuser'


class FirmPropertyName(ReadOnlyModel):
    firm_property = models.OneToOneField(FirmProperty, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'firm_property_name'


class IiiLanguage(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    description = models.CharField(max_length=64, blank=True)
    staff_enabled = models.NullBooleanField(null=True, blank=True)
    public_enabled = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_right_to_left = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_language'


class IiiLanguageMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    description = models.CharField(max_length=64, blank=True)
    staff_enabled = models.NullBooleanField(null=True, blank=True)
    public_enabled = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_right_to_left = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_language_myuser'


class IiiLanguageName(ReadOnlyModel):
    iii_language = models.OneToOneField(IiiLanguage, primary_key=True)
    description = models.CharField(max_length=255, blank=True)
    name_iii_language_id = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_language_name'


class MblockProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'mblock_property'


class MblockPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'mblock_property_myuser'


class MblockPropertyName(ReadOnlyModel):
    mblock_property = models.OneToOneField(MblockProperty, primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'mblock_property_name'


class NotificationMediumProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'notification_medium_property'


class NotificationMediumPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'notification_medium_property_myuser'


class NotificationMediumPropertyName(ReadOnlyModel):
    notification_medium_property = models.OneToOneField(NotificationMediumProperty,
                                                     primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'notification_medium_property_name'


# Our Patron Records don't have firms or agencies. If you do, enable them
# below. Same with iii_language.
class PatronRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True, blank=True)
    ptype = models.ForeignKey('PtypeProperty',
                              db_column='ptype_code',
                              to_field='value',
                              null=True,
                              blank=True)
    home_library_code = models.CharField(max_length=5, blank=True)
    expiration_date_gmt = models.DateTimeField(null=True, blank=True)
    pcode1 = models.CharField(max_length=1, blank=True)
    pcode2 = models.CharField(max_length=1, blank=True)
    pcode3 = models.SmallIntegerField(null=True, blank=True)
    pcode4 = models.IntegerField(null=True, blank=True)
    birth_date_gmt = models.DateField(null=True, blank=True)
    mblock = models.ForeignKey('MblockProperty',
                               db_column='mblock_code',
                               to_field='code',
                               null=True,
                               blank=True)
    # firm = models.ForeignKey('FirmProperty',
    #                          db_column='firm_code',
    #                          to_field='code',
    #                          null=True,
    #                          blank=True)
    block_until_date_gmt = models.DateTimeField(null=True, blank=True)
    # patron_agency = models.ForeignKey('AgencyProperty',
    #                                   db_column='patron_agency_code_num',
    #                                   to_field='code_num',
    #                                   null=True,
    #                                   blank=True)
    # iii_language = models.ForeignKey(IiiLanguage,
    #                                  db_column='iii_language_pref_code',
    #                                  to_field='code',
    #                                  null=True,
    #                                  blank=True)
    checkout_total = models.IntegerField(null=True, blank=True)
    renewal_total = models.IntegerField(null=True, blank=True)
    checkout_count = models.IntegerField(null=True, blank=True)
    patron_message_code = models.CharField(max_length=1, blank=True)
    highest_level_overdue_num = models.IntegerField(null=True, blank=True)
    claims_returned_total = models.IntegerField(null=True, blank=True)
    owed_amt = models.DecimalField(null=True,
                                   max_digits=30,
                                   decimal_places=6,
                                   blank=True)
    itema_count = models.IntegerField(null=True, blank=True)
    itemb_count = models.IntegerField(null=True, blank=True)
    overdue_penalty_count = models.IntegerField(null=True, blank=True)
    ill_checkout_total = models.IntegerField(null=True, blank=True)
    debit_amt = models.DecimalField(null=True,
                                    max_digits=30,
                                    decimal_places=6,
                                    blank=True)
    itemc_count = models.IntegerField(null=True, blank=True)
    itemd_count = models.IntegerField(null=True, blank=True)
    activity_gmt = models.DateTimeField(null=True, blank=True)
    notification_medium = models.ForeignKey(NotificationMediumProperty,
                                            db_column='notification_medium_code',
                                            to_field='code',
                                            null=True,
                                            blank=True)
    registration_count = models.IntegerField(null=True, blank=True)
    registration_total = models.IntegerField(null=True, blank=True)
    attendance_total = models.IntegerField(null=True, blank=True)
    waitlist_count = models.IntegerField(null=True, blank=True)
    is_reading_history_opt_in = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'patron_record'


class PatronRecordAddress(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.ForeignKey(PatronRecord, null=True, blank=True)
    patron_record_address_type = models.ForeignKey('PatronRecordAddressType',
                                                   null=True,
                                                   blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    addr1 = models.CharField(max_length=1000, blank=True)
    addr2 = models.CharField(max_length=1000, blank=True)
    addr3 = models.CharField(max_length=1000, blank=True)
    village = models.CharField(max_length=1000, blank=True)
    city = models.CharField(max_length=1000, blank=True)
    region = models.CharField(max_length=1000, blank=True)
    postal_code = models.CharField(max_length=100, blank=True)
    country = models.CharField(max_length=1000, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'patron_record_address'


class PatronRecordAddressType(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=1, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'patron_record_address_type'


class PatronRecordFullname(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.ForeignKey(PatronRecord, null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    prefix = models.CharField(max_length=50, blank=True)
    first_name = models.CharField(max_length=500, blank=True)
    middle_name = models.CharField(max_length=500, blank=True)
    last_name = models.CharField(max_length=500, blank=True)
    suffix = models.CharField(max_length=50, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'patron_record_fullname'


class PatronRecordPhone(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.ForeignKey(PatronRecord, null=True, blank=True)
    patron_record_phone_type = models.ForeignKey('PatronRecordPhoneType',
                                                 null=True,
                                                 blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    phone_number = models.CharField(max_length=200, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'patron_record_phone'


class PatronRecordPhoneType(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=1, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'patron_record_phone_type'


# class PatronView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     barcode = models.CharField(max_length=512, blank=True)
#     ptype = models.ForeignKey('PtypeProperty',
#                               db_column='ptype_code',
#                               to_field='value',
#                               null=True,
#                               blank=True)
#     home_library_code = models.CharField(max_length=5, blank=True)
#     expiration_date_gmt = models.DateTimeField(null=True, blank=True)
#     pcode1 = models.CharField(max_length=1, blank=True)
#     pcode2 = models.CharField(max_length=1, blank=True)
#     pcode3 = models.SmallIntegerField(null=True, blank=True)
#     pcode4 = models.IntegerField(null=True, blank=True)
#     birth_date_gmt = models.DateField(null=True, blank=True)
#     mblock = models.ForeignKey('MblockProperty',
#                                db_column='mblock_code',
#                                to_field='code',
#                                null=True,
#                                blank=True)
#     firm = models.ForeignKey('FirmProperty',
#                              db_column='firm_code',
#                              to_field='code',
#                              null=True,
#                              blank=True)
#     block_until_date_gmt = models.DateTimeField(null=True, blank=True)
#     patron_agency = models.ForeignKey('AgencyProperty',
#                                       db_column='patron_agency_code_num',
#                                       to_field='code_num',
#                                       null=True,
#                                       blank=True)
#     iii_language = models.ForeignKey(IiiLanguage,
#                                      db_column='iii_language_pref_code',
#                                      to_field='code',
#                                      null=True,
#                                      blank=True)
#     checkout_total = models.IntegerField(null=True, blank=True)
#     renewal_total = models.IntegerField(null=True, blank=True)
#     checkout_count = models.IntegerField(null=True, blank=True)
#     patron_message_code = models.CharField(max_length=1, blank=True)
#     highest_level_overdue_num = models.IntegerField(null=True, blank=True)
#     claims_returned_total = models.IntegerField(null=True, blank=True)
#     owed_amt = models.DecimalField(null=True,
#                                    max_digits=30,
#                                    decimal_places=6,
#                                    blank=True)
#     itema_count = models.IntegerField(null=True, blank=True)
#     itemb_count = models.IntegerField(null=True, blank=True)
#     overdue_penalty_count = models.IntegerField(null=True, blank=True)
#     ill_checkout_total = models.IntegerField(null=True, blank=True)
#     debit_amt = models.DecimalField(null=True,
#                                     max_digits=30,
#                                     decimal_places=6,
#                                     blank=True)
#     itemc_count = models.IntegerField(null=True, blank=True)
#     itemd_count = models.IntegerField(null=True, blank=True)
#     activity_gmt = models.DateTimeField(null=True, blank=True)
#     notification_medium = models.ForeignKey(NotificationMediumProperty,
#                                             db_column='notification_medium_code',
#                                             to_field='code',
#                                             null=True,
#                                             blank=True)
#     registration_count = models.IntegerField(null=True, blank=True)
#     registration_total = models.IntegerField(null=True, blank=True)
#     attendance_total = models.IntegerField(null=True, blank=True)
#     waitlist_count = models.IntegerField(null=True, blank=True)
#     is_reading_history_opt_in = models.NullBooleanField(null=True,
#                                                         blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'patron_view'

# If you use agencies, enable ptype_agency.
class Pblock(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    ptype = models.ForeignKey('PtypeProperty',
                              db_column='ptype_code_num',
                              to_field='value',
                              null=True,
                              blank=True)
    # ptype_agency = models.ForeignKey('AgencyProperty',
    #                                  db_column='ptype_agency_code_num',
    #                                  to_field='code_num',
    #                                  null=True,
    #                                  blank=True)
    is_expiration_date_active = models.NullBooleanField(null=True, blank=True)
    max_owed_amt = models.DecimalField(null=True,
                                       max_digits=30,
                                       decimal_places=6,
                                       blank=True)
    max_overdue_num = models.IntegerField(null=True, blank=True)
    max_item_num = models.IntegerField(null=True, blank=True)
    max_hold_num = models.IntegerField(null=True, blank=True)
    max_ill_item_num = models.IntegerField(null=True, blank=True)
    max_ill_item_per_period_num = models.IntegerField(null=True, blank=True)
    max_itema_num = models.IntegerField(null=True, blank=True)
    max_itemb_num = models.IntegerField(null=True, blank=True)
    max_itemc_num = models.IntegerField(null=True, blank=True)
    max_itemd_num = models.IntegerField(null=True, blank=True)
    max_registration_num = models.IntegerField(null=True, blank=True)
    max_penalty_point_num = models.IntegerField(null=True, blank=True)
    penalty_point_days = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'pblock'


class PtypeProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    value = models.SmallIntegerField(null=True, unique=True, blank=True)
    tagging_allowed = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_force_right_result_exact_allowed = models.NullBooleanField(null=True,
                                                                  blank=True)
    is_comment_auto_approved = models.NullBooleanField(null=True, blank=True)
    ptype_property_category = models.ForeignKey('PtypePropertyCategory',
                                                db_column='ptype_category_id',
                                                null=True,
                                                blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'ptype_property'


class PtypePropertyCategory(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_default = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'ptype_property_category'


class PtypePropertyCategoryMyuser(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_default = models.NullBooleanField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'ptype_property_category_myuser'


class PtypePropertyCategoryName(ReadOnlyModel):
    ptype_property_category = models.OneToOneField(PtypePropertyCategory,
                                                db_column='ptype_category_id',
                                                primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'ptype_property_category_name'


class PtypePropertyMyuser(ReadOnlyModel):
    value = models.SmallIntegerField(primary_key=True)
    tagging_allowed = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_force_right_result_exact_allowed = models.NullBooleanField(null=True,
                                                                  blank=True)
    is_comment_auto_approved = models.NullBooleanField(null=True, blank=True)
    ptype_property_category = models.ForeignKey(PtypePropertyCategory,
                                                db_column='ptype_category_id',
                                                null=True,
                                                blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'ptype_property_myuser'


class PtypePropertyName(ReadOnlyModel):
    ptype_property = models.OneToOneField(PtypeProperty,
                                       db_column='ptype_id',
                                       primary_key=True)
    description = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'ptype_property_name'


class UserDefinedPcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_pcode1_myuser'


class UserDefinedPcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_pcode2_myuser'


class UserDefinedPcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_pcode3_myuser'


class UserDefinedPcode4Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_pcode4_myuser'


# ENTITIES -- PROGRAM ---------------------------------------------------------|

class GtypeProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code_num = models.IntegerField(null=True, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'gtype_property'


class GtypePropertyMyuser(ReadOnlyModel):
    code = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'gtype_property_myuser'


class GtypePropertyName(ReadOnlyModel):
    gtype_property = models.OneToOneField(GtypeProperty, primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'gtype_property_name'


class ProgramRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    locations = models.ManyToManyField('Location',
                                       through='ProgramRecordLocation',
                                       blank=True)
    program_name = models.CharField(max_length=1024, blank=True)
    reg_allowed_code = models.CharField(max_length=1, blank=True)
    allocation_rule_code = models.CharField(max_length=1, blank=True)
    cost = models.DecimalField(null=True,
                               max_digits=30,
                               decimal_places=6,
                               blank=True)
    eligibility_code = models.CharField(max_length=1, blank=True)
    publication_start_date_gmt = models.DateTimeField(null=True, blank=True)
    publication_end_date_gmt = models.DateTimeField(null=True, blank=True)
    tickler_days_to_start = models.IntegerField(null=True, blank=True)
    min_alert_days_to_start = models.IntegerField(null=True, blank=True)
    max_alert_seats_open = models.IntegerField(null=True, blank=True)
    reg_per_patron = models.IntegerField(null=True, blank=True)
    program_type = models.ForeignKey('GtypeProperty',
                                     db_column='program_type_code',
                                     to_field='code_num',
                                     null=True,
                                     blank=True)
    auto_transfer_code = models.CharField(max_length=1, blank=True)
    is_right_result_exact = models.NullBooleanField(null=True, blank=True)
    gcode1 = models.CharField(max_length=1, blank=True)
    gcode2 = models.CharField(max_length=1, blank=True)
    gcode3 = models.CharField(max_length=1, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'program_record'


class ProgramRecordLocation(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    program_record = models.ForeignKey(ProgramRecord, null=True, blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'program_record_location'


# class ProgramView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     program_name = models.CharField(max_length=1024, blank=True)
#     reg_allowed_code = models.CharField(max_length=1, blank=True)
#     allocation_rule_code = models.CharField(max_length=1, blank=True)
#     cost = models.DecimalField(null=True,
#                                max_digits=30,
#                                decimal_places=6,
#                                blank=True)
#     eligibility_code = models.CharField(max_length=1, blank=True)
#     publication_start_date_gmt = models.DateTimeField(null=True, blank=True)
#     publication_end_date_gmt = models.DateTimeField(null=True, blank=True)
#     tickler_days_to_start = models.IntegerField(null=True, blank=True)
#     min_alert_days_to_start = models.IntegerField(null=True, blank=True)
#     max_alert_seats_open = models.IntegerField(null=True, blank=True)
#     reg_per_patron = models.IntegerField(null=True, blank=True)
#     program_type = models.ForeignKey('GtypeProperty',
#                                      db_column='program_type_code',
#                                      to_field='code_num',
#                                      null=True,
#                                      blank=True)
#     auto_transfer_code = models.CharField(max_length=1, blank=True)
#     is_right_result_exact = models.NullBooleanField(null=True, blank=True)
#     gcode1 = models.CharField(max_length=1, blank=True)
#     gcode2 = models.CharField(max_length=1, blank=True)
#     gcode3 = models.CharField(max_length=1, blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'program_view'

class UserDefinedGcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_gcode1_myuser'


class UserDefinedGcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_gcode2_myuser'


class UserDefinedGcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_gcode3_myuser'


# ENTITIES -- RESOURCE --------------------------------------------------------|

# below: language, country, and location don't appear in any of our resource
# records. If you have them, feel free to uncomment them to enable those
# relationships.
class ResourceRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    holding_records = models.ManyToManyField(HoldingRecord,
                                             through='ResourceRecordHoldingRecordRelatedLink',
                                             blank=True)
    license_records = models.ManyToManyField(LicenseRecord,
                                             through='ResourceRecordLicenseRecordLink',
                                             blank=True)
    order_records = models.ManyToManyField(OrderRecord,
                                           through='ResourceRecordOrderRecordLink',
                                           blank=True)
    related_order_records = models.ManyToManyField(OrderRecord,
                                                   through='ResourceRecordOrderRecordRelatedLink',
                                                   related_name='related_resourcerecord_set',
                                                   blank=True)
    is_right_result_exact = models.NullBooleanField(null=True, blank=True)
    rights_code = models.CharField(max_length=1, blank=True)
    suppress_code = models.CharField(max_length=1, blank=True)
    ecode1 = models.CharField(max_length=1, blank=True)
    ecode2 = models.CharField(max_length=1, blank=True)
    ecode3 = models.CharField(max_length=1, blank=True)
    ecode4 = models.CharField(max_length=1, blank=True)
    resource_status_code = models.CharField(max_length=1, blank=True)
    package_code = models.CharField(max_length=1, blank=True)
    trial_begin_gmt = models.DateTimeField(null=True, blank=True)
    trial_end_gmt = models.DateTimeField(null=True, blank=True)
    renewal_gmt = models.DateTimeField(null=True, blank=True)
    registration_gmt = models.DateTimeField(null=True, blank=True)
    activation_gmt = models.DateTimeField(null=True, blank=True)
    edate5_gmt = models.DateTimeField(null=True, blank=True)
    edate6_gmt = models.DateTimeField(null=True, blank=True)
    # language = models.ForeignKey('LanguageProperty',
    #                              db_column='language_code',
    #                              to_field='code',
    #                              null=True,
    #                              blank=True)
    # country = models.ForeignKey('CountryProperty',
    #                             db_column='country_code',
    #                             to_field='code',
    #                             null=True,
    #                             blank=True)
    access_provider = models.ForeignKey(ContactRecord,
                                        db_column='access_provider_code',
                                        to_field='code',
                                        null=True,
                                        blank=True)
    # location = models.ForeignKey('Location',
    #                              db_column='location_code',
    #                              to_field='code',
    #                              null=True,
    #                              blank=True)
    publisher_code = models.CharField(max_length=5, blank=True)
    licensor_code = models.CharField(max_length=5, blank=True)
    copyright_holder_code = models.CharField(max_length=5, blank=True)
    data_provider_code = models.CharField(max_length=5, blank=True)
    consortium_code = models.CharField(max_length=5, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    def get_url(self):
        """
        Many (if not all) e-resource URLs have " target="_blank"
        appended, and this strips that to return just the URL.
        """
        ret = None
        url = self.record_metadata.varfield_set.filter(varfield_type_code='y')
        if url.count() > 0:
            ret = re.sub(r'^([^"]+)((".*)|$)$', r'\1', url[0].field_content)
        return ret

    def get_bib(self):
        """
        Returns the BibRecord model corresponding to a bib record in
        the system with an 856|u that matches the y-tagged URL for
        this resource. Essentially, since there's no actual link
        between a resource record and the bib record that represents
        that same resource, this tries to find a match.
        """
        ret = None
        url = self.get_url()
        if url is not None:
            title = self.record_metadata.varfield_set.filter(
                varfield_type_code='t')[0].field_content.lower()
            bib_url_filter = {
                'record_metadata__subfield__marc_tag': '856',
                'record_metadata__subfield__tag': 'u',
                'record_metadata__subfield__content': url
            }
            bib_title_filter = {
                'record_metadata__subfield__marc_tag': '245',
                'record_metadata__subfield__tag': 'a',
                'record_metadata__subfield__content__iexact': title
            }
            try:
                bib = BibRecord.objects.filter(**bib_url_filter).filter(
                    **bib_title_filter)[0]
            except IndexError:
                pass
            else:
                ret = bib
        return ret

    class Meta(ReadOnlyModel.Meta):
        db_table = 'resource_record'


# class ResourceView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     holding_records = models.ManyToManyField(
#         HoldingRecord,
#         through='ResourceRecordHoldingRecordRelatedLink',
#         null=True,
#         blank=True)
#     license_records = models.ManyToManyField(
#         LicenseRecord,
#         through='ResourceRecordLicenseRecordLink',
#         null=True,
#         blank=True)
#     hard_linked_order_records = models.ManyToManyField(
#         LicenseRecord,
#         through='ResourceRecordOrderRecordLink',
#         null=True,
#         blank=True)
#     soft_linked_order_records = models.ManyToManyField(
#         LicenseRecord,
#         through='ResourceRecordOrderRecordRelatedLink',
#         null=True,
#         blank=True)
#     is_right_result_exact = models.NullBooleanField(null=True, blank=True)
#     rights_code = models.CharField(max_length=1, blank=True)
#     suppress_code = models.CharField(max_length=1, blank=True)
#     ecode1 = models.CharField(max_length=1, blank=True)
#     ecode2 = models.CharField(max_length=1, blank=True)
#     ecode3 = models.CharField(max_length=1, blank=True)
#     ecode4 = models.CharField(max_length=1, blank=True)
#     resource_status_code = models.CharField(max_length=1, blank=True)
#     package_code = models.CharField(max_length=1, blank=True)
#     trial_begin_gmt = models.DateTimeField(null=True, blank=True)
#     trial_end_gmt = models.DateTimeField(null=True, blank=True)
#     renewal_gmt = models.DateTimeField(null=True, blank=True)
#     registration_gmt = models.DateTimeField(null=True, blank=True)
#     activation_gmt = models.DateTimeField(null=True, blank=True)
#     edate5_gmt = models.DateTimeField(null=True, blank=True)
#     edate6_gmt = models.DateTimeField(null=True, blank=True)
#     language = models.ForeignKey('LanguageProperty',
#                                  db_column='language_code',
#                                  to_field='code',
#                                  null=True,
#                                  blank=True)
#     country = models.ForeignKey('CountryProperty',
#                                 db_column='country_code',
#                                 to_field='code',
#                                 null=True,
#                                 blank=True)
#     access_provider = models.ForeignKey(ContactRecord,
#                                         db_column='access_provider_code',
#                                         to_field='code',
#                                         null=True,
#                                         blank=True)
#     location = models.ForeignKey('Location',
#                                  db_column='location_code',
#                                  to_field='code',
#                                  null=True,
#                                  blank=True)
#     publisher_code = models.CharField(max_length=5, blank=True)
#     licensor_code = models.CharField(max_length=5, blank=True)
#     copyright_holder_code = models.CharField(max_length=5, blank=True)
#     data_provider_code = models.CharField(max_length=5, blank=True)
#     consortium_code = models.CharField(max_length=5, blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'resource_view'

class UserDefinedEcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ecode1_myuser'


class UserDefinedEcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ecode2_myuser'


class UserDefinedEcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ecode3_myuser'


class UserDefinedEcode4Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_ecode4_myuser'


# ENTITIES -- SECTION ---------------------------------------------------------|

class SectionRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    registered_patrons = models.ManyToManyField(PatronRecord,
                                                through='SectionRegistrationSeat',
                                                blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    status_code = models.CharField(max_length=1, blank=True)
    program_record = models.ForeignKey(ProgramRecord, null=True, blank=True)
    section_display_order = models.IntegerField(null=True, blank=True)
    min_seats = models.IntegerField(null=True, blank=True)
    max_seats = models.IntegerField(null=True, blank=True)
    reg_open_date_gmt = models.DateTimeField(null=True, blank=True)
    reg_close_date_gmt = models.DateTimeField(null=True, blank=True)
    ecommerce_code = models.CharField(max_length=1, blank=True)
    max_alert_sent_date_gmt = models.DateTimeField(null=True, blank=True)
    tickler_sent_date_gmt = models.DateTimeField(null=True, blank=True)
    min_alert_sent_date_gmt = models.DateTimeField(null=True, blank=True)
    max_waitlist_num = models.IntegerField(null=True, blank=True)
    zcode1 = models.CharField(max_length=1, blank=True)
    zcode2 = models.CharField(max_length=1, blank=True)
    zcode3 = models.CharField(max_length=1, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'section_record'


class SectionRecordSession(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    section_record = models.ForeignKey(SectionRecord, null=True, blank=True)
    start_date = models.DateTimeField(null=True, blank=True)
    duration_minutes = models.IntegerField(null=True, blank=True)
    session_display_order = models.IntegerField(null=True, blank=True)
    start_date_str = models.CharField(max_length=14, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'section_record_session'


class SectionRegistrationSeat(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    section_record = models.ForeignKey(SectionRecord, null=True, blank=True)
    patron_record = models.ForeignKey(PatronRecord, null=True, blank=True)
    reg_date_gmt = models.DateTimeField(null=True, blank=True)
    is_registered = models.NullBooleanField(null=True, blank=True)
    seat_note = models.CharField(max_length=255, blank=True)
    payment = models.ForeignKey('Payment', null=True, blank=True)
    reg_date = models.CharField(max_length=14, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'section_registration_seat'


# class SectionView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     patron_records = models.ManyToManyField(PatronRecord,
#                                             through='SectionRegistrationSeat',
#                                             null=True,
#                                             blank=True)
#     location = models.ForeignKey('Location',
#                                  db_column='location_code',
#                                  to_field='code',
#                                  null=True,
#                                  blank=True)
#     status_code = models.CharField(max_length=1, blank=True)
#     program_record_id = models.BigIntegerField(null=True, blank=True)
#     section_display_order = models.IntegerField(null=True, blank=True)
#     min_seats = models.IntegerField(null=True, blank=True)
#     max_seats = models.IntegerField(null=True, blank=True)
#     reg_open_date_gmt = models.DateTimeField(null=True, blank=True)
#     reg_close_date_gmt = models.DateTimeField(null=True, blank=True)
#     ecommerce_code = models.CharField(max_length=1, blank=True)
#     max_alert_sent_date_gmt = models.DateTimeField(null=True, blank=True)
#     tickler_sent_date_gmt = models.DateTimeField(null=True, blank=True)
#     min_alert_sent_date_gmt = models.DateTimeField(null=True, blank=True)
#     max_waitlist_num = models.IntegerField(null=True, blank=True)
#     zcode1 = models.CharField(max_length=1, blank=True)
#     zcode2 = models.CharField(max_length=1, blank=True)
#     zcode3 = models.CharField(max_length=1, blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'section_view'

class SessionAttendance(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    section_record_session = models.ForeignKey(SectionRecordSession,
                                               null=True,
                                               blank=True)
    section_registration_seat = models.ForeignKey(SectionRegistrationSeat,
                                                  null=True,
                                                  blank=True)
    patron_record = models.ForeignKey(PatronRecord, null=True, blank=True)
    total_attended = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'session_attendance'


class UserDefinedZcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_zcode1_myuser'


class UserDefinedZcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_zcode2_myuser'


class UserDefinedZcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_zcode3_myuser'


# ENTITIES -- VENDOR ----------------------------------------------------------|

class UserDefinedVcode1Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_vcode1_myuser'


class UserDefinedVcode2Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_vcode2_myuser'


class UserDefinedVcode3Myuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory',
                                              null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_vcode3_myuser'


# Our vendor_record rows all have blank language codes.
class VendorRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    accounting_unit = models.ForeignKey('AccountingUnit',
                                        db_column='accounting_unit_code_num',
                                        to_field='code_num',
                                        blank=True,
                                        null=True)
    code = models.CharField(max_length=5, unique=True, blank=True)
    claim_cycle_code = models.CharField(max_length=1, blank=True)
    vcode1 = models.CharField(max_length=1, blank=True)
    vcode2 = models.CharField(max_length=1, blank=True)
    vcode3 = models.CharField(max_length=1, blank=True)
    order_cnt = models.IntegerField(null=True, blank=True)
    claim_cnt = models.IntegerField(null=True, blank=True)
    cancel_cnt = models.IntegerField(null=True, blank=True)
    receipt_cnt = models.IntegerField(null=True, blank=True)
    invoice_cnt = models.IntegerField(null=True, blank=True)
    orders_claimed_cnt = models.IntegerField(null=True, blank=True)
    copies_received_cnt = models.IntegerField(null=True, blank=True)
    order_total_amt = models.DecimalField(null=True,
                                          max_digits=30,
                                          decimal_places=6,
                                          blank=True)
    invoice_total_amt = models.DecimalField(null=True,
                                            max_digits=30,
                                            decimal_places=6,
                                            blank=True)
    estimated_received_price_amt = models.DecimalField(null=True,
                                                       max_digits=30,
                                                       decimal_places=6,
                                                       blank=True)
    estimated_cancelled_price_amt = models.DecimalField(null=True,
                                                        max_digits=30,
                                                        decimal_places=6,
                                                        blank=True)
    average_weeks = models.IntegerField(null=True, blank=True)
    discount = models.IntegerField(null=True, blank=True)
    vendor_message_code = models.CharField(max_length=3, blank=True)
    # language = models.ForeignKey('LanguageProperty',
    #                              db_column='language_code',
    #                              to_field='code',
    #                              null=True,
    #                              blank=True)
    gir_code = models.IntegerField(null=True, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'vendor_record'


class VendorRecordAddress(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    vendor_record = models.ForeignKey(VendorRecord, null=True, blank=True)
    vendor_record_address_type = models.ForeignKey('VendorRecordAddressType',
                                                   null=True,
                                                   blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    addr1 = models.CharField(max_length=1000, blank=True)
    addr2 = models.CharField(max_length=1000, blank=True)
    addr3 = models.CharField(max_length=1000, blank=True)
    village = models.CharField(max_length=1000, blank=True)
    city = models.CharField(max_length=1000, blank=True)
    region = models.CharField(max_length=1000, blank=True)
    postal_code = models.CharField(max_length=100, blank=True)
    country = models.CharField(max_length=1000, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'vendor_record_address'


class VendorRecordAddressType(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=1, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'vendor_record_address_type'


# class VendorView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     id = models.BigIntegerField(primary_key=True)
#     code = models.CharField(max_length=5, blank=True)
#     claim_cycle_code = models.CharField(max_length=1, blank=True)
#     vcode1 = models.CharField(max_length=1, blank=True)
#     vcode2 = models.CharField(max_length=1, blank=True)
#     vcode3 = models.CharField(max_length=1, blank=True)
#     order_cnt = models.IntegerField(null=True, blank=True)
#     claim_cnt = models.IntegerField(null=True, blank=True)
#     cancel_cnt = models.IntegerField(null=True, blank=True)
#     receipt_cnt = models.IntegerField(null=True, blank=True)
#     invoice_cnt = models.IntegerField(null=True, blank=True)
#     orders_claimed_cnt = models.IntegerField(null=True, blank=True)
#     copies_received_cnt = models.IntegerField(null=True, blank=True)
#     order_total_amt = models.DecimalField(null=True,
#                                           max_digits=30,
#                                           decimal_places=6,
#                                           blank=True)
#     invoice_total_amt = models.DecimalField(null=True,
#                                             max_digits=30,
#                                             decimal_places=6,
#                                             blank=True)
#     estimated_received_price_amt = models.DecimalField(null=True,
#                                                        max_digits=30,
#                                                        decimal_places=6,
#                                                        blank=True)
#     estimated_cancelled_price_amt = models.DecimalField(null=True,
#                                                         max_digits=30,
#                                                         decimal_places=6,
#                                                         blank=True)
#     average_weeks = models.IntegerField(null=True, blank=True)
#     discount = models.IntegerField(null=True, blank=True)
#     vendor_message_code = models.CharField(max_length=3, blank=True)
#     language = models.ForeignKey('LanguageProperty',
#                                  db_column='language_code',
#                                  to_field='code',
#                                  null=True,
#                                  blank=True)
#     gir_code = models.IntegerField(null=True, blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'vendor_view'

# ENTITIES -- VOLUME ----------------------------------------------------------|

class BibRecordVolumeRecordLink(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    bib_record = models.ForeignKey(BibRecord, null=True, blank=True)
    volume_record = models.OneToOneField('VolumeRecord',
                                      unique=True,
                                      null=True,
                                      blank=True)
    volumes_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bib_record_volume_record_link'


class VolumeRecord(MainRecordTypeModel):
    id = models.BigIntegerField(primary_key=True)
    record_metadata = models.ForeignKey(RecordMetadata,
                                        db_column='record_id',
                                        null=True,
                                        blank=True)
    sort_order = models.IntegerField(null=True, blank=True)
    is_suppressed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'volume_record'


# class VolumeView(ReadOnlyModel):
#     id = models.BigIntegerField(primary_key=True)
#     record_type = models.ForeignKey(RecordType,
#                                     db_column='record_type_code',
#                                     to_field='code')
#     record_num = models.IntegerField(null=True, blank=True)
#     sort_order = models.IntegerField(null=True, blank=True)
#     is_suppressed = models.NullBooleanField(null=True, blank=True)
#     record_creation_date_gmt = models.DateTimeField(null=True, blank=True)
#
#     class Meta(ReadOnlyModel.Meta):
#         db_table = 'volume_view'

# TRANSACTIONS -- ACQUISITIONS ------------------------------------------------|

class AccountingTransaction(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_unit = models.ForeignKey('AccountingUnit', null=True, blank=True)
    fund_master = models.ForeignKey('FundMaster', null=True, blank=True)
    voucher_num = models.IntegerField(null=True, blank=True)
    voucher_seq_num = models.IntegerField(null=True, blank=True)
    posted_date = models.DateTimeField(null=True, blank=True)
    amt_type = models.IntegerField(null=True, blank=True)
    amt = models.DecimalField(null=True,
                              max_digits=30,
                              decimal_places=6,
                              blank=True)
    note = models.CharField(max_length=255, blank=True)
    source_name = models.CharField(max_length=50, blank=True)
    last_updated_gmt = models.DateTimeField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_transaction'


class AccountingTransactionIllExpenditure(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_transaction = models.ForeignKey(AccountingTransaction,
                                               null=True,
                                               blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_transaction_ill_expenditure'


class AccountingTransactionInvoiceEncumbrance(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_transaction = models.ForeignKey(AccountingTransaction,
                                               null=True,
                                               blank=True)
    invoice_record = models.ForeignKey(InvoiceRecord,
                                       db_column='invoice_record_metadata_id',
                                       null=True,
                                       blank=True)
    invoice_date = models.DateTimeField(null=True, blank=True)
    order_record = models.ForeignKey(OrderRecord,
                                     db_column='order_record_metadata_id',
                                     null=True,
                                     blank=True)
    bib_record = models.ForeignKey(BibRecord,
                                   db_column='bib_record_metadata_id',
                                   null=True,
                                   blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    copies = models.IntegerField(null=True, blank=True)
    foreign_currency_code = models.CharField(max_length=20, blank=True)
    foreign_currency_amt = models.DecimalField(null=True,
                                               max_digits=30,
                                               decimal_places=6,
                                               blank=True)
    xy_note = models.CharField(max_length=255, blank=True)
    subscription_from_date = models.DateTimeField(null=True, blank=True)
    subscription_to_date = models.DateTimeField(null=True, blank=True)
    invoice_record_line_item_num = models.IntegerField(null=True, blank=True)
    vendor_record = models.ForeignKey(VendorRecord,
                                      db_column='vendor_record_metadata_id',
                                      null=True,
                                      blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_transaction_invoice_encumbrance'
        # truncated verbose_name for Django 1.7 39 character limit
        verbose_name = 'accounting transact invoice encumbrance'


class AccountingTransactionInvoiceExpenditure(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_transaction = models.ForeignKey(AccountingTransaction,
                                               null=True,
                                               blank=True)
    invoice_record = models.ForeignKey(InvoiceRecord,
                                       db_column='invoice_record_metadata_id',
                                       null=True,
                                       blank=True)
    invoice_date = models.DateTimeField(null=True, blank=True)
    order_record = models.ForeignKey(OrderRecord,
                                     db_column='order_record_metadata_id',
                                     null=True,
                                     blank=True)
    bib_record = models.ForeignKey(BibRecord,
                                   db_column='bib_record_metadata_id',
                                   null=True,
                                   blank=True)
    subfund = models.ForeignKey('FundSummarySubfund',
                                db_column='subfund_code',
                                to_field='code',
                                null=True,
                                blank=True)
    location = models.ForeignKey('Location',
                                 db_column='location_code',
                                 to_field='code',
                                 null=True,
                                 blank=True)
    copies = models.IntegerField(null=True, blank=True)
    tax_amt = models.DecimalField(null=True,
                                  max_digits=30,
                                  decimal_places=6,
                                  blank=True)
    foreign_currency_code = models.CharField(max_length=20, blank=True)
    foreign_currency_amt = models.DecimalField(null=True,
                                               max_digits=30,
                                               decimal_places=6,
                                               blank=True)
    foreign_currency_tax_amt = models.DecimalField(null=True,
                                                   max_digits=30,
                                                   decimal_places=6,
                                                   blank=True)
    xy_note = models.CharField(max_length=255, blank=True)
    use_tax_amt = models.DecimalField(null=True, max_digits=30,
                                      decimal_places=6, blank=True)
    ship_amt = models.DecimalField(null=True, max_digits=30, decimal_places=6,
                                   blank=True)
    discount_amt = models.DecimalField(null=True, max_digits=30,
                                       decimal_places=6, blank=True)
    service_charge_amt = models.DecimalField(null=True, max_digits=30,
                                             decimal_places=6, blank=True)
    subscription_from_date = models.DateTimeField(null=True, blank=True)
    subscription_to_date = models.DateTimeField(null=True, blank=True)
    invoice_record_line_item_num = models.IntegerField(null=True, blank=True)
    vendor_record = models.ForeignKey(VendorRecord,
                                      db_column='vendor_record_metadata_id',
                                      null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_transaction_invoice_expenditure'
        # truncated verbose_name for Django 1.7 39 character limit
        verbose_name = 'accounting transact invoice expenditure'


class AccountingTransactionManualAppropriation(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_transaction = models.ForeignKey(AccountingTransaction, null=True,
                                               blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_transaction_manual_appropriation'
        # truncated verbose_name for Django 1.7 39 character limit
        verbose_name = 'accounting transac manual appropriation'


class AccountingTransactionManualEncumbrance(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_transaction = models.ForeignKey(AccountingTransaction, null=True,
                                               blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_transaction_manual_encumbrance'
        # truncated verbose_name for Django 1.7 39 character limit
        verbose_name = 'accounting transact manual encumbrance'


class AccountingTransactionManualExpenditure(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_transaction = models.ForeignKey(AccountingTransaction, null=True,
                                               blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_transaction_manual_expenditure'
        # truncated verbose_name for Django 1.7 39 character limit
        verbose_name = 'accounting transact manual expenditure'


class AccountingTransactionOrderCancellation(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_transaction = models.ForeignKey(AccountingTransaction, null=True,
                                               blank=True)
    order_record = models.ForeignKey(OrderRecord,
                                     db_column='order_record_metadata_id',
                                     null=True, blank=True)
    bib_record = models.ForeignKey(BibRecord,
                                   db_column='bib_record_metadata_id',
                                   null=True, blank=True)
    location = models.ForeignKey('Location', db_column='location_code',
                                 to_field='code', null=True, blank=True)
    copies = models.IntegerField(null=True, blank=True)
    foreign_currency_code = models.CharField(max_length=20, blank=True)
    foreign_currency_amt = models.DecimalField(null=True, max_digits=30,
                                               decimal_places=6, blank=True)
    subscription_from_date = models.DateTimeField(null=True, blank=True)
    subscription_to_date = models.DateTimeField(null=True, blank=True)
    vendor_record = models.ForeignKey(VendorRecord,
                                      db_column='vendor_record_metadata_id',
                                      null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_transaction_order_cancellation'
        # truncated verbose_name for Django 1.7 39 character limit
        verbose_name = 'accounting transact order cancellation'


class AccountingTransactionOrderEncumbrance(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_transaction = models.ForeignKey(AccountingTransaction, null=True,
                                               blank=True)
    order_record = models.ForeignKey(OrderRecord,
                                     db_column='order_record_metadata_id',
                                     null=True, blank=True)
    bib_record = models.ForeignKey(BibRecord,
                                   db_column='bib_record_metadata_id',
                                   null=True, blank=True)
    location = models.ForeignKey('Location', db_column='location_code',
                                 to_field='code', null=True, blank=True)
    copies = models.IntegerField(null=True, blank=True)
    foreign_currency_code = models.CharField(max_length=20, blank=True)
    foreign_currency_amt = models.DecimalField(null=True, max_digits=30,
                                               decimal_places=6, blank=True)
    subscription_from_date = models.DateTimeField(null=True, blank=True)
    subscription_to_date = models.DateTimeField(null=True, blank=True)
    vendor_record = models.ForeignKey(VendorRecord,
                                      db_column='vendor_record_metadata_id',
                                      null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_transaction_order_encumbrance'
        # truncated verbose_name for Django 1.7 39 character limit
        verbose_name = 'accounting transact order encumbrance'


class FundMaster(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_unit = models.ForeignKey('AccountingUnit', null=True, blank=True)
    code_num = models.IntegerField(null=True, blank=True)
    code = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fund_master'


class FundProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    fund_master = models.ForeignKey(FundMaster, null=True, blank=True)
    fund_type = models.ForeignKey('FundType', null=True, blank=True)
    external_fund_property = models.ForeignKey('ExternalFundProperty',
                                               null=True, blank=True)
    warning_percent = models.IntegerField(null=True, blank=True)
    discount_percent = models.IntegerField(null=True, blank=True)
    user_code1 = models.CharField(max_length=255, blank=True)
    user_code2 = models.CharField(max_length=255, blank=True)
    user_code3 = models.CharField(max_length=255, blank=True)
    is_active = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fund_property'


class FundSummary(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    fund_property = models.OneToOneField(FundProperty, unique=True, null=True,
                                      blank=True)
    appropriation = models.IntegerField(null=True, blank=True)
    expenditure = models.IntegerField(null=True, blank=True)
    encumbrance = models.IntegerField(null=True, blank=True)
    num_orders = models.IntegerField(null=True, blank=True)
    num_payments = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fund_summary'


class FundType(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=255, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fund_type'


# TRANSACTIONS -- CIRCULATION -------------------------------------------------|

class Booking(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    item_record = models.ForeignKey(ItemRecord, null=True, blank=True)
    patron_record = models.ForeignKey(PatronRecord, null=True, blank=True)
    created_gmt = models.DateTimeField(null=True, blank=True)
    start_gmt = models.DateTimeField(null=True, blank=True)
    end_gmt = models.DateTimeField(null=True, blank=True)
    type_code = models.CharField(max_length=1, blank=True)
    prep_period = models.IntegerField(null=True, blank=True)
    location = models.ForeignKey('Location', db_column='location_code',
                                 to_field='code', null=True, blank=True)
    delivery_code = models.SmallIntegerField(null=True, blank=True)
    location_note = models.CharField(max_length=19, blank=True)
    note = models.CharField(max_length=1000, blank=True)
    event_name = models.CharField(max_length=1000, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'booking'


class Checkout(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.ForeignKey('PatronRecord', null=True, blank=True)
    item_record = models.OneToOneField('ItemRecord', unique=True, null=True,
                                    blank=True)
    items_display_order = models.IntegerField(null=True, blank=True)
    due_gmt = models.DateTimeField(null=True, blank=True)
    loanrule_code_num = models.IntegerField(null=True, blank=True)
    checkout_gmt = models.DateTimeField(null=True, blank=True)
    renewal_count = models.IntegerField(null=True, blank=True)
    overdue_count = models.IntegerField(null=True, blank=True)
    overdue_gmt = models.DateTimeField(null=True, blank=True)
    recall_gmt = models.DateTimeField(null=True, blank=True)
    ptype = models.ForeignKey(PtypeProperty, db_column='ptype',
                              to_field='value', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'checkout'


class ColagencyCriteria(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    home_libraries = models.ManyToManyField('Location',
                                            through='ColagencyCriteriaHomeLibraries',
                                            blank=True)
    ptypes = models.ManyToManyField(PtypeProperty,
                                    through='ColagencyCriteriaPtypes',
                                    blank=True)
    name = models.CharField(max_length=36, unique=True, blank=True)
    minimum_owed_amt = models.DecimalField(null=True, max_digits=30,
                                           decimal_places=6, blank=True)
    start_date_gmt = models.DateTimeField(null=True, blank=True)
    end_date_gmt = models.DateTimeField(null=True, blank=True)
    grace_period = models.IntegerField(null=True, blank=True)
    minimum_days_overdue = models.IntegerField(null=True, blank=True)
    remove_if_less_than_amt = models.DecimalField(null=True, max_digits=30,
                                                  decimal_places=6, blank=True)
    agency_fee_amt = models.DecimalField(null=True, max_digits=30,
                                         decimal_places=6, blank=True)
    email_source = models.CharField(max_length=201, blank=True)
    email_to = models.CharField(max_length=201, blank=True)
    email_cc = models.CharField(max_length=201, blank=True)
    email_subject = models.CharField(max_length=201, blank=True)
    auto_new_submission = models.NullBooleanField(null=True, blank=True)
    auto_update_submission = models.CharField(max_length=13, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'colagency_criteria'


class ColagencyCriteriaHomeLibraries(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    colagency = models.ForeignKey(ColagencyCriteria, null=True, blank=True)
    home_library = models.ForeignKey('Location', db_column='home_library',
                                     to_field='code', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'colagency_criteria_home_libraries'


class ColagencyCriteriaPtypes(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    colagency = models.ForeignKey(ColagencyCriteria, null=True, blank=True)
    ptype = models.ForeignKey(PtypeProperty, db_column='ptype',
                              to_field='value', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'colagency_criteria_ptypes'


class ColagencyPatron(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.OneToOneField(PatronRecord,
                                      db_column='patron_record_metadata_id',
                                      unique=True, null=True, blank=True)
    status = models.CharField(max_length=15, blank=True)
    time_removed_gmt = models.DateTimeField(null=True, blank=True)
    time_report_last_run_gmt = models.DateTimeField(null=True, blank=True)
    colagency = models.ForeignKey(ColagencyCriteria,
                                  db_column='colagency_criteria_metadata_id',
                                  null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'colagency_patron'


class Fine(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.ForeignKey(PatronRecord, null=True, blank=True)
    assessed_gmt = models.DateTimeField(null=True, blank=True)
    invoice_num = models.IntegerField(null=True, blank=True)
    item_charge_amt = models.DecimalField(null=True, max_digits=30,
                                          decimal_places=6, blank=True)
    processing_fee_amt = models.DecimalField(null=True, max_digits=30,
                                             decimal_places=6, blank=True)
    billing_fee_amt = models.DecimalField(null=True, max_digits=30,
                                          decimal_places=6, blank=True)
    charge_code = models.CharField(max_length=1, blank=True)
    charge_location = models.ForeignKey('Location',
                                        db_column='charge_location_code',
                                        to_field='code', null=True, blank=True)
    paid_gmt = models.DateTimeField(null=True, blank=True)
    terminal_num = models.IntegerField(null=True, blank=True)
    paid_amt = models.DecimalField(null=True, max_digits=30, decimal_places=6,
                                   blank=True)
    initials = models.CharField(max_length=12, blank=True)
    created_code = models.CharField(max_length=1, blank=True)
    is_print_bill = models.NullBooleanField(null=True, blank=True)
    description = models.CharField(max_length=100, blank=True)
    item_record = models.ForeignKey(ItemRecord,
                                    db_column='item_record_metadata_id',
                                    null=True, blank=True)
    checkout_gmt = models.DateTimeField(null=True, blank=True)
    due_gmt = models.DateTimeField(null=True, blank=True)
    returned_gmt = models.DateTimeField(null=True, blank=True)
    loanrule_code_num = models.IntegerField(null=True, blank=True)
    title = models.CharField(max_length=82, blank=True)
    original_patron_record = models.ForeignKey(PatronRecord,
                                               related_name='original_fine_set',
                                               db_column='original_patron_record_metadata_id',
                                               null=True, blank=True)
    original_transfer_gmt = models.DateTimeField(null=True, blank=True)
    previous_invoice_num = models.IntegerField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fine'


class FinesPaid(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    fine_assessed_date_gmt = models.DateTimeField(null=True, blank=True)
    patron_record = models.ForeignKey(PatronRecord,
                                      db_column='patron_record_metadata_id',
                                      null=True, blank=True)
    item_charge_amt = models.DecimalField(null=True, max_digits=30,
                                          decimal_places=6, blank=True)
    processing_fee_amt = models.DecimalField(null=True, max_digits=30,
                                             decimal_places=6, blank=True)
    billing_fee_amt = models.DecimalField(null=True, max_digits=30,
                                          decimal_places=6, blank=True)
    charge_type_code = models.CharField(max_length=1, blank=True)
    charge_location = models.ForeignKey('Location',
                                        db_column='charge_location_code',
                                        to_field='code', null=True, blank=True)
    paid_date_gmt = models.DateTimeField(null=True, blank=True)
    tty_num = models.IntegerField(null=True, blank=True)
    last_paid_amt = models.DecimalField(null=True, max_digits=30,
                                        decimal_places=6, blank=True)
    iii_user_name = models.CharField(max_length=255, blank=True)
    fine_creation_mode_code = models.CharField(max_length=1, blank=True)
    print_bill_code = models.CharField(max_length=1, blank=True)
    item_record = models.ForeignKey(ItemRecord,
                                    db_column='item_record_metadata_id',
                                    null=True, blank=True)
    checked_out_date_gmt = models.DateTimeField(null=True, blank=True)
    due_date_gmt = models.DateTimeField(null=True, blank=True)
    returned_date_gmt = models.DateTimeField(null=True, blank=True)
    loan_rule_code_num = models.IntegerField(null=True, blank=True)
    description = models.CharField(max_length=100, blank=True)
    paid_now_amt = models.DecimalField(null=True, max_digits=30,
                                       decimal_places=6, blank=True)
    payment_status_code = models.CharField(max_length=1, blank=True)
    payment_type_code = models.CharField(max_length=1, blank=True)
    payment_note = models.CharField(max_length=150, blank=True)
    transaction_id = models.IntegerField(null=True, blank=True)
    invoice_num = models.IntegerField(null=True, blank=True)
    old_invoice_num = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fines_paid'


class Hold(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.ForeignKey(PatronRecord, null=True, blank=True)
    record = models.ForeignKey(RecordMetadata, null=True, blank=True)
    placed_gmt = models.DateTimeField(null=True, blank=True)
    is_frozen = models.NullBooleanField(null=True, blank=True)
    delay_days = models.IntegerField(null=True, blank=True)
    location = models.ForeignKey('Location', db_column='location_code',
                                 to_field='code', null=True, blank=True)
    expires_gmt = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=1, blank=True)
    is_ir = models.NullBooleanField(null=True, blank=True)
    pickup_location = models.ForeignKey('Location',
                                        db_column='pickup_location_code',
                                        to_field='code',
                                        related_name='pickup_hold_set',
                                        null=True, blank=True)
    is_ill = models.NullBooleanField(null=True, blank=True)
    note = models.CharField(max_length=128, blank=True)
    ir_pickup_location = models.ForeignKey('Location',
                                           db_column='ir_pickup_location_code',
                                           to_field='code',
                                           related_name='irpickup_hold_set',
                                           null=True, blank=True)
    ir_print_name = models.CharField(max_length=255, blank=True)
    ir_delivery_stop_name = models.CharField(max_length=255, blank=True)
    is_ir_converted_request = models.NullBooleanField(null=True, blank=True)
    patron_records_display_order = models.IntegerField(null=True, blank=True)
    records_display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'hold'


class ItemCircHistory(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    item_record = models.ForeignKey(ItemRecord,
                                    db_column='item_record_metadata_id',
                                    null=True, blank=True)
    patron_record = models.ForeignKey(PatronRecord,
                                      db_column='patron_record_metadata_id',
                                      null=True, blank=True)
    checkout_gmt = models.DateTimeField(null=True, blank=True)
    checkin_gmt = models.DateTimeField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'item_circ_history'


class PatronsToExclude(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.OneToOneField(PatronRecord,
                                      db_column='patron_record_metadata_id',
                                      unique=True, null=True, blank=True)
    time_added_to_table_gmt = models.DateTimeField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'patrons_to_exclude'


class Payment(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    pmt_date_gmt = models.DateTimeField(null=True, blank=True)
    amt_paid = models.DecimalField(null=True, max_digits=30, decimal_places=6,
                                   blank=True)
    pmt_type_code = models.CharField(max_length=20, blank=True)
    pmt_note = models.CharField(max_length=255, blank=True)
    patron_record = models.OneToOneField(PatronRecord,
                                      db_column='patron_record_metadata_id',
                                      unique=True, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'payment'


class ReadingHistory(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    bib_record = models.ForeignKey(BibRecord,
                                   db_column='bib_record_metadata_id',
                                   null=True, blank=True)
    item_record = models.ForeignKey(ItemRecord,
                                    db_column='item_record_metadata_id',
                                    null=True, blank=True)
    patron_record = models.ForeignKey(PatronRecord,
                                      db_column='patron_record_metadata_id',
                                      null=True, blank=True)
    checkout_gmt = models.DateTimeField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'reading_history'


class Request(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.ForeignKey(PatronRecord, null=True, blank=True)
    item_record = models.ForeignKey(ItemRecord, null=True, blank=True)
    items_display_order = models.IntegerField(null=True, blank=True)
    ptype = models.SmallIntegerField(null=True, blank=True)
    patrons_display_order = models.IntegerField(null=True, blank=True)
    request_gmt = models.DateTimeField(null=True, blank=True)
    pickup_anywhere_location = models.ForeignKey('Location',
                                                 db_column='pickup_anywhere_location_code',
                                                 to_field='code',
                                                 related_name='pickupanywhere_request_set',
                                                 null=True, blank=True)
    central_location = models.ForeignKey('Location',
                                         db_column='central_location_code',
                                         to_field='code',
                                         related_name='central_request_set',
                                         null=True, blank=True)
    transaction_num = models.IntegerField(null=True, blank=True)
    remote_patron_record = models.ForeignKey(PatronRecord,
                                             db_column='remote_patron_record_key',
                                             related_name='remote_request_set',
                                             null=True, blank=True)
    dl_pickup_location_code_num = models.IntegerField()

    class Meta(ReadOnlyModel.Meta):
        db_table = 'request'


class ReturnedBilledItem(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    patron_record = models.ForeignKey(PatronRecord,
                                      db_column='patron_record_metadata_id',
                                      null=True, blank=True)
    item_cost_amt = models.DecimalField(null=True, max_digits=30,
                                        decimal_places=6, blank=True)
    checked_in_time_gmt = models.DateTimeField(null=True, blank=True)
    invoice_number = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'returned_billed_item'


class TitlePagingReport(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    prepared_date_gmt = models.DateTimeField(null=True, blank=True)
    location_type = models.IntegerField(null=True, blank=True)
    location = models.ForeignKey('Location', db_column='location_code',
                                 to_field='code', null=True, blank=True)
    location_group_code_num = models.IntegerField(null=True, blank=True)
    longname = models.CharField(max_length=200, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'title_paging_report'


class TitlePagingReportEntry(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    title_paging_report = models.ForeignKey(TitlePagingReport, null=True,
                                            blank=True)
    record_metadata = models.ForeignKey(RecordMetadata, null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    title = models.CharField(max_length=200, blank=True)
    call_number = models.CharField(max_length=200, blank=True)
    is_processed = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'title_paging_report_entry'


class TitlePagingReportEntryItem(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    title_paging_report_entry = models.ForeignKey(TitlePagingReportEntry,
                                                  null=True, blank=True)
    record_metadata = models.ForeignKey(RecordMetadata, null=True, blank=True)
    scanned_date_gmt = models.DateTimeField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'title_paging_report_entry_item'


class TitlePagingReportEntryPatron(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    title_paging_report_entry = models.ForeignKey(TitlePagingReportEntry,
                                                  null=True, blank=True)
    record_metadata = models.ForeignKey(RecordMetadata, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'title_paging_report_entry_patron'


# MASTER DATA -- CATALOGING ---------------------------------------------------|

class B2MCategory(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=20, unique=True, blank=True)
    is_staff_enabled = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'b2m_category'


class B2MCategoryMyuser(ReadOnlyModel):
    code = models.CharField(max_length=20, primary_key=True)
    is_staff_enabled = models.NullBooleanField(null=True, blank=True)
    name = models.CharField(max_length=60, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'b2m_category_myuser'


class B2MCategoryName(ReadOnlyModel):
    b2m_category = models.OneToOneField(B2MCategory, primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=60, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'b2m_category_name'


class M2BmapCategory(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    original_delimiter = models.CharField(max_length=1, blank=True)
    is_case_sensitive = models.NullBooleanField(null=True, blank=True)
    is_bar_subfield = models.NullBooleanField(null=True, blank=True)
    is_chinese = models.NullBooleanField(null=True, blank=True)
    is_stop_on_map = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'm2bmap_category'


class M2BmapEntry(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    m2bmap_category = models.ForeignKey(M2BmapCategory, null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    comparison = models.CharField(max_length=200, blank=True)
    replacement = models.CharField(max_length=200, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'm2bmap_entry'


class MarcExportFormat(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=20, unique=True, blank=True)
    code_name = models.CharField(max_length=20, blank=True)
    is_staff_enabled = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'marc_export_format'


class MarcPreference(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    iii_user_name = models.CharField(max_length=255, blank=True)
    diacritic_category = models.ForeignKey('DiacriticCategory', null=True,
                                           blank=True)
    marc_export_format_id = models.BigIntegerField(null=True, blank=True)
    b2m_category = models.ForeignKey(B2MCategory, null=True,
                                     db_column='b2m_category_code',
                                     to_field='code', blank=True)
    is_default_preference = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'marc_preference'


# MASTER DATA -- CATEGORIZATION -----------------------------------------------|

class Collection(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    is_default = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    avg_width = models.FloatField(null=True, blank=True)
    avg_cost = models.DecimalField(null=True, max_digits=30, decimal_places=6,
                                   blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'collection'


# CollectionMyuser us not in the Sierra docs, but this seems the appropriate place for it.
class CollectionMyuser(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    is_default = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'collection_myuser'


class EadHierarchy(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    bib_record = models.OneToOneField(BibRecord, unique=True, null=True,
                                   blank=True)
    entry = models.CharField(max_length=255, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'ead_hierarchy'


class PhysicalFormat(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    is_default = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'physical_format'


class PhysicalFormatMyuser(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    is_default = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'physical_format_myuser'


class PhysicalFormatName(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    physical_format = models.ForeignKey(PhysicalFormat, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'physical_format_name'


class ScatCategory(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code_num = models.IntegerField(null=True, blank=True)
    scat_section = models.ForeignKey('ScatSection', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'scat_category'


class ScatCategoryMyuser(ReadOnlyModel):
    code = models.IntegerField(primary_key=True)
    scat_section = models.ForeignKey('ScatSection', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'scat_category_myuser'


class ScatCategoryName(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    scat_category = models.ForeignKey(ScatCategory, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey('LanguageProperty',
                                     db_column='iii_language_code',
                                     to_field='code', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'scat_category_name'


class ScatRange(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    line_num = models.IntegerField(null=True, blank=True)
    start_letter_str = models.CharField(max_length=20, blank=True)
    end_letter_str = models.CharField(max_length=20, blank=True)
    start_num_str = models.CharField(max_length=10, blank=True)
    end_num_str = models.CharField(max_length=10, blank=True)
    scat_category = models.ForeignKey(ScatCategory, null=True, blank=True)
    free_text_type = models.CharField(max_length=1, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'scat_range'


class ScatSection(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code_num = models.IntegerField(unique=True, null=True, blank=True)
    index_tag = models.CharField(max_length=20, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'scat_section'


class ScatSectionMyuser(ReadOnlyModel):
    code = models.IntegerField(primary_key=True)
    index_tag = models.CharField(max_length=20, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'scat_section_myuser'


class ScatSectionName(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    scat_section = models.ForeignKey(ScatSection, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey(IiiLanguage, db_column='iii_language_code',
                                     to_field='code', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'scat_section_name'


class TargetAudience(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    is_default = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'target_audience'


class TargetAudienceMyuser(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    is_default = models.NullBooleanField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'target_audience_myuser'


class TargetAudienceName(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    target_audience = models.ForeignKey(TargetAudience, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'target_audience_name'


# MASTER DATA -- FUND ---------------------------------------------------------|

class AccountingUnit(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code_num = models.IntegerField(unique=True, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_unit'


class AccountingUnitMyuser(ReadOnlyModel):
    code = models.OneToOneField(AccountingUnit, db_column='code',
                             to_field='code_num', primary_key=True)
    name = models.CharField(max_length=20, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_unit_myuser'


class AccountingUnitName(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    accounting_unit = models.ForeignKey(AccountingUnit, null=True, blank=True)
    name = models.CharField(max_length=20, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'accounting_unit_name'


class ExternalFundProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    accounting_unit = models.ForeignKey(AccountingUnit, null=True, blank=True)
    code_num = models.IntegerField(null=True, blank=True, unique=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'external_fund_property'


class ExternalFundPropertyMyuser(ReadOnlyModel):
    code = models.IntegerField(primary_key=True)
    accounting_unit = models.ForeignKey(AccountingUnit, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'external_fund_property_myuser'


class ExternalFundPropertyName(ReadOnlyModel):
    external_fund_property = models.OneToOneField(ExternalFundProperty,
                                               primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'external_fund_property_name'


class Fund(ReadOnlyModel):
    accounting_unit = models.ForeignKey(AccountingUnit, db_column='acct_unit',
                                        to_field='code_num', null=True,
                                        blank=True)
    fund_type = models.ForeignKey(FundType, db_column='fund_type',
                                  to_field='code', null=True, blank=True)
    fund_code = models.CharField(max_length=255, primary_key=True)
    external_fund = models.IntegerField(null=True, blank=True)
    appropriation = models.IntegerField(null=True, blank=True)
    expenditure = models.IntegerField(null=True, blank=True)
    encumbrance = models.IntegerField(null=True, blank=True)
    num_orders = models.IntegerField(null=True, blank=True)
    num_payments = models.IntegerField(null=True, blank=True)
    warning_percent = models.IntegerField(null=True, blank=True)
    discount_percent = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fund'


class FundMyuser(ReadOnlyModel):
    accounting_unit = models.ForeignKey(AccountingUnit, db_column='acct_unit',
                                        to_field='code_num', null=True,
                                        blank=True)
    fund_type = models.ForeignKey(FundType, db_column='fund_type',
                                  to_field='code', null=True, blank=True)
    fund_master = models.OneToOneField(FundMaster, primary_key=True)
    fund_code = models.CharField(max_length=255, blank=True)
    external_fund_code_num = models.IntegerField(null=True, blank=True)
    appropriation = models.IntegerField(null=True, blank=True)
    expenditure = models.IntegerField(null=True, blank=True)
    encumbrance = models.IntegerField(null=True, blank=True)
    num_orders = models.IntegerField(null=True, blank=True)
    num_payments = models.IntegerField(null=True, blank=True)
    warning_percent = models.IntegerField(null=True, blank=True)
    discount_percent = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)
    note1 = models.CharField(max_length=255, blank=True)
    note2 = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fund_myuser'


class FundPropertyName(ReadOnlyModel):
    fund_property = models.OneToOneField(FundProperty, primary_key=True)
    iii_language = models.ForeignKey('IiiLanguage', null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)
    note1 = models.CharField(max_length=255, blank=True)
    note2 = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fund_property_name'


class FundSummarySubfund(ReadOnlyModel):
    fund_summary = models.ForeignKey(FundSummary, null=True, blank=True)
    code = models.CharField(max_length=255, primary_key=True)
    value = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fund_summary_subfund'


class FundTypeSummary(ReadOnlyModel):
    accounting_unit = models.ForeignKey(AccountingUnit, null=True, blank=True)
    fund_type = models.OneToOneField(FundType, primary_key=True)
    last_lien_num = models.IntegerField(null=True, blank=True)
    last_voucher_num = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'fund_type_summary'


# MASTER DATA -- LOCATION -----------------------------------------------------|

class AgencyPropertyLocationGroup(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    agency_property_code_num = models.ForeignKey('AgencyProperty',
                                                 db_column='agency_property_code_num',
                                                 to_field='code_num', null=True,
                                                 blank=True)
    location_group_port_number = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'agency_property_location_group'


class Branch(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    address = models.CharField(max_length=300, null=True, blank=True)
    email_source = models.CharField(max_length=255, null=True, blank=True)
    email_reply_to = models.CharField(max_length=255, null=True, blank=True)
    address_latitude = models.CharField(max_length=32, null=True, blank=True)
    address_longitude = models.CharField(max_length=32, null=True, blank=True)
    code_num = models.IntegerField(unique=True, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'branch'


class BranchChange(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    branch = models.ForeignKey(Branch, null=True, db_column='branch_code_num',
                               to_field='code_num', blank=True)
    description = models.CharField(max_length=1000, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'branch_change'


class BranchMyuser(ReadOnlyModel):
    code = models.IntegerField(primary_key=True)
    address = models.CharField(max_length=300, null=True, blank=True)
    email_source = models.CharField(max_length=255, null=True, blank=True)
    email_reply_to = models.CharField(max_length=255, null=True, blank=True)
    address_latitude = models.CharField(max_length=32, null=True, blank=True)
    address_longitude = models.CharField(max_length=32, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'branch_myuser'


class BranchName(ReadOnlyModel):
    branch = models.OneToOneField(Branch, primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'branch_name'


class Location(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=5, unique=True, blank=True)
    branch = models.ForeignKey(Branch, null=True, db_column='branch_code_num',
                               to_field='code_num', blank=True)
    parent_location = models.ForeignKey('self',
                                        db_column='parent_location_code',
                                        to_field='code', null=True, blank=True)
    is_public = models.NullBooleanField(null=True, blank=True)
    is_requestable = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'location'


class LocationChange(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    location_code = models.CharField(max_length=5)
    description = models.CharField(max_length=1000, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'location_change'


class LocationMyuser(ReadOnlyModel):
    location = models.OneToOneField(Location, db_column='code', to_field='code',
                                    primary_key=True)
    branch = models.ForeignKey(Branch, null=True, db_column='branch_code_num',
                               to_field='code_num', blank=True)
    parent_location = models.ForeignKey('Location',
                                        db_column='parent_location_code',
                                        to_field='code', related_name='+',
                                        null=True, blank=True)
    is_public = models.NullBooleanField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'location_myuser'


class LocationName(ReadOnlyModel):
    location = models.OneToOneField(Location, primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'location_name'


class LocationPropertyType(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=255, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    default_value = models.CharField(max_length=1024, blank=True)
    is_single_value = models.NullBooleanField(null=True, blank=True)
    is_enabled = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'location_property_type'


class LocationPropertyTypeMyuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    default_value = models.CharField(max_length=1024, blank=True)
    is_single_value = models.NullBooleanField(null=True, blank=True)
    is_enabled = models.NullBooleanField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'location_property_type_myuser'


class LocationPropertyTypeName(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    location_property_type = models.ForeignKey(LocationPropertyType, null=True,
                                               blank=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'location_property_type_name'


class LocationPropertyValue(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    location_property_type = models.ForeignKey(LocationPropertyType, null=True,
                                               blank=True)
    location = models.ForeignKey(Location, null=True, blank=True)
    value = models.CharField(max_length=1024, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'location_property_value'


# MASTER DATA -- PROPERTIES ---------------------------------------------------|

class AgencyProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code_num = models.IntegerField(unique=True, null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'agency_property'


class AgencyPropertyMyuser(ReadOnlyModel):
    code = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'agency_property_myuser'


class AgencyPropertyName(ReadOnlyModel):
    agency_property = models.OneToOneField(AgencyProperty, primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'agency_property_name'


class CountryProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'country_property'


class CountryPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'country_property_myuser'


class CountryPropertyName(ReadOnlyModel):
    country_property = models.OneToOneField(CountryProperty, primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'country_property_name'


# ItypeProperty is under ITEMS

class LanguageProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=3, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_public = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'language_property'


class LanguagePropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=3, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    is_public = models.NullBooleanField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'language_property_myuser'


class LanguagePropertyName(ReadOnlyModel):
    language_property = models.OneToOneField(LanguageProperty, primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'language_property_name'


# MaterialProperty is under BIB

class ReceivingLocationProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=1, unique=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'receiving_location_property'


class ReceivingLocationPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=1, primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'receiving_location_property_myuser'


class ReceivingLocationPropertyName(ReadOnlyModel):
    receiving_location_property = models.OneToOneField(ReceivingLocationProperty,
                                                    primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'receiving_location_property_name'


class UserDefinedCategory(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=255, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_category'


class UserDefinedProperty(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    user_defined_category = models.ForeignKey(UserDefinedCategory, null=True,
                                              blank=True)
    code = models.CharField(max_length=255, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_property'


class UserDefinedPropertyMyuser(ReadOnlyModel):
    code = models.CharField(max_length=255, primary_key=True)
    user_defined_category = models.ForeignKey('UserDefinedCategory', null=True,
                                              blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_property_myuser'


class UserDefinedPropertyName(ReadOnlyModel):
    user_defined_property = models.OneToOneField(UserDefinedProperty,
                                              primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'user_defined_property_name'


# MASTER DATA -- SYSTEM CONFIGURATION -----------------------------------------|

class BackupAdmin(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=128, blank=True)
    email = models.CharField(max_length=128, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'backup_admin'


class DarcServiceView(ReadOnlyModel):
    group_name = models.CharField(max_length=30, blank=True)
    service_name = models.CharField(max_length=30, primary_key=True)
    param = models.CharField(max_length=30, blank=True)
    value = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'darc_service_view'


class DiacriticCategory(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=255, blank=True)
    is_unicode_style = models.NullBooleanField(null=True, blank=True)
    is_big5 = models.NullBooleanField(null=True, blank=True)
    is_cccii = models.NullBooleanField(null=True, blank=True)
    is_eacc = models.NullBooleanField(null=True, blank=True)
    is_thai = models.NullBooleanField(null=True, blank=True)
    is_winpage = models.NullBooleanField(null=True, blank=True)
    is_multibyte = models.NullBooleanField(null=True, blank=True)
    is_decomposed_character_used = models.NullBooleanField(null=True,
                                                           blank=True)
    is_general_rule_enabled = models.NullBooleanField(null=True, blank=True)
    is_staff_enabled = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'diacritic_category'


class DiacriticMapping(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    diacritic = models.CharField(max_length=255, blank=True)
    letter = models.CharField(max_length=1, blank=True)
    description = models.CharField(max_length=255, blank=True)
    mapped_string = models.CharField(max_length=255, blank=True)
    is_preferred = models.NullBooleanField(null=True, blank=True)
    diacritic_category = models.ForeignKey(DiacriticCategory, null=True,
                                           blank=True)
    width = models.IntegerField(null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'diacritic_mapping'


class KeywordSynonym(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    keyword = models.CharField(max_length=255, blank=True)
    synonym = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'keyword_synonym'


class NetworkAccessView(ReadOnlyModel):
    port = models.IntegerField(primary_key=True)
    is_enabled = models.NullBooleanField(null=True, blank=True)
    is_super_user = models.NullBooleanField(null=True, blank=True)
    ip_range = models.CharField(max_length=255, blank=True)
    is_accessible = models.NullBooleanField(null=True, blank=True)
    login_name = models.CharField(max_length=128, blank=True)
    service_level = models.IntegerField(null=True, blank=True)
    comment = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'network_access_view'


class OaiCrosswalk(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    marc_type = models.CharField(max_length=1, blank=True)
    metadata_prefix = models.CharField(max_length=32, blank=True)
    name = models.CharField(max_length=32, unique=True, blank=True)
    description = models.CharField(max_length=255, blank=True)
    is_system = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'oai_crosswalk'


class OaiCrosswalkField(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    oai_crosswalk = models.ForeignKey(OaiCrosswalk, null=True, blank=True)
    element_name = models.CharField(max_length=100, blank=True)
    varfield_type_code = models.CharField(max_length=1, blank=True)
    marc_tag = models.CharField(max_length=3, blank=True)
    subfields = models.CharField(max_length=26, blank=True)
    is_add_subfield = models.NullBooleanField(null=True, blank=True)
    is_varfield = models.NullBooleanField(null=True, blank=True)
    fixnum = models.IntegerField(null=True, blank=True)
    order_num = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'oai_crosswalk_field'


class RequestRule(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record_type = models.ForeignKey(RecordType, db_column='record_type_code',
                                    to_field='code')
    query = models.TextField(blank=True)
    sql_query = models.TextField(blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'request_rule'


class RequestRuleComment(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    request_rule = models.ForeignKey(RequestRule, null=True, blank=True)
    comment = models.TextField(blank=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'request_rule_comment'


class SuiteBehaviorView(ReadOnlyModel):
    suite = models.CharField(max_length=20, blank=True)
    app = models.CharField(max_length=20, blank=True)
    code = models.CharField(max_length=255, primary_key=True)
    value = models.CharField(max_length=255, blank=True)
    type = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'suite_behavior_view'


class SuiteMessageView(ReadOnlyModel):
    suite = models.CharField(max_length=20, blank=True)
    app = models.CharField(max_length=20, blank=True)
    code = models.CharField(max_length=100, primary_key=True)
    lang = models.CharField(max_length=3, blank=True)
    value = models.CharField(max_length=1000, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'suite_message_view'


class SuiteSkinView(ReadOnlyModel):
    suite = models.CharField(max_length=20, blank=True)
    app = models.CharField(max_length=20, blank=True)
    code = models.CharField(max_length=255, primary_key=True)
    lang = models.CharField(max_length=3, blank=True)
    type = models.CharField(max_length=255, blank=True)
    value = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'suite_skin_view'


class Wamreport(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    logged_gmt = models.DateTimeField(null=True, blank=True)
    requesting_ip = models.CharField(max_length=32, blank=True)
    requesting_port = models.IntegerField(null=True, blank=True)
    requesting_iii_name = models.CharField(max_length=255, blank=True)
    dest_port = models.IntegerField(null=True, blank=True)
    dest_code = models.CharField(max_length=8, blank=True)
    response_category_code_num = models.IntegerField(null=True, blank=True)
    patron_record = models.ForeignKey(PatronRecord,
                                      db_column='patron_record_metadata_id',
                                      null=True,
                                      blank=True)
    ptype = models.ForeignKey(PtypeProperty,
                              db_column='ptype_code_num',
                              to_field='value',
                              null=True,
                              blank=True)
    pcode1 = models.CharField(max_length=1, blank=True)
    pcode2 = models.CharField(max_length=1, blank=True)
    pcode3_code_num = models.IntegerField(null=True, blank=True)
    pcode4_code_num = models.IntegerField(null=True, blank=True)
    rejection_reason_code_num = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'wamreport'


class ZipCodeInfo(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    zip_code = models.CharField(max_length=32, blank=True)
    latitude = models.CharField(max_length=32, blank=True)
    longitude = models.CharField(max_length=32, blank=True)
    country = models.ForeignKey(CountryProperty,
                                db_column='country_code',
                                to_field='code',
                                null=True,
                                blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'zip_code_info'


# MODELS FOR UNDOCUMENTED TABLES ----------------------------------------------|

# BoolInfo stores Review File (Create Lists) metadata. One row per review file.
class BoolInfo(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    name = models.CharField(max_length=512, blank=True)
    max = models.IntegerField(null=True, blank=True)
    count = models.IntegerField(null=True, blank=True)
    record_type = models.ForeignKey(RecordType,
                                    db_column='record_type_code',
                                    to_field='code',
                                    null=True,
                                    blank=True)
    record_range = models.CharField(max_length=512, blank=True)
    bool_gmt = models.DateTimeField(null=True, blank=True)
    bool_query = models.TextField(blank=True)
    sql_query = models.TextField(blank=True)
    is_lookup_call = models.NullBooleanField(null=True, blank=True)
    is_lookup_880 = models.NullBooleanField(null=True, blank=True)
    is_search_holdings = models.NullBooleanField(null=True, blank=True)
    sorter_spec = models.TextField(blank=True)
    lister_spec = models.TextField(blank=True)
    status_code = models.CharField(max_length=1, blank=True)
    iii_user_name = models.CharField(max_length=255, blank=True)
    list_export_spec = models.TextField(blank=True)
    owner_iii_user_name = models.CharField(max_length=255, blank=True)
    is_store_field = models.NullBooleanField(null=True, blank=True)
    is_card_search = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bool_info'


# BoolSet appears to store lists of records in non-empty review files.
class BoolSet(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    record = models.ForeignKey(RecordMetadata,
                               db_column='record_metadata_id',
                               null=True,
                               blank=True)
    bool_info = models.ForeignKey(BoolInfo, null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)
    field_key = models.CharField(max_length=255, blank=True)
    occ_num = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'bool_set'


# CircTrans is empty in our system. Not sure what it stores.
class CircTrans(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    transaction_gmt = models.DateTimeField(null=True, blank=True)
    application_name = models.CharField(max_length=256, blank=True)
    source_code = models.CharField(max_length=256, blank=True)
    op_code = models.CharField(max_length=256, blank=True)
    patron_record_id = models.BigIntegerField(null=True, blank=True)
    item_record_id = models.BigIntegerField(null=True, blank=True)
    volume_record_id = models.BigIntegerField(null=True, blank=True)
    bib_record_id = models.BigIntegerField(null=True, blank=True)
    stat_group_code_num = models.IntegerField(null=True, blank=True)
    due_date_gmt = models.DateTimeField(null=True, blank=True)
    count_type_code_num = models.IntegerField(null=True, blank=True)
    itype = models.ForeignKey(ItypeProperty,
                              db_column='itype_code_num',
                              to_field='code_num',
                              null=True,
                              blank=True)
    icode1 = models.IntegerField(null=True, blank=True)
    icode2 = models.CharField(max_length=10, blank=True)
    item_location = models.ForeignKey('Location',
                                      db_column='item_location_code',
                                      to_field='code',
                                      null=True,
                                      blank=True)
    # item_agency = models.ForeignKey('AgencyProperty',
    #                                 db_column='item_agency_code_num',
    #                                 to_field='code_num',
    #                                 null=True,
    #                                 blank=True)
    ptype = models.ForeignKey(PtypeProperty,
                              db_column='ptype_code',
                              to_field='value',
                              null=True,
                              blank=True)
    pcode1 = models.CharField(max_length=1, blank=True)
    pcode2 = models.CharField(max_length=1, blank=True)
    pcode3 = models.IntegerField(null=True, blank=True)
    pcode4 = models.IntegerField(null=True, blank=True)
    patron_home_library_code = models.CharField(max_length=5, blank=True)
    # patron_agency = models.ForeignKey('AgencyProperty', db_column='patron_agency_code_num', to_field='code_num', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'circ_trans'


# Not sure, we have a couple of rows in our counter table. Possibly stores counts of various things?
class Counter(ReadOnlyModel):
    code = models.CharField(max_length=100, primary_key=True)
    num = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'counter'


# Looks like this stores a list of the functions used in the "FUNCTIONS" menu in the SDA.
class Function(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    code = models.CharField(max_length=255, unique=True, blank=True)
    function_category = models.ForeignKey('FunctionCategory', null=True,
                                          blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'function'


# Function categories linked from the Function table.
class FunctionCategory(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    code = models.CharField(max_length=255, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'function_category'


# IiiRole and following -- Not sure. Looks like a list of processes.
class IiiRole(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=255, unique=True, blank=True)
    iii_role_category = models.ForeignKey('IiiRoleCategory', null=True,
                                          blank=True)
    is_disabled_during_read_only_access = models.NullBooleanField(null=True,
                                                                  blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_role'


class IiiRoleCategory(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code = models.CharField(max_length=255, unique=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_role_category'


class IiiRoleCategoryName(ReadOnlyModel):
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    iii_role_category = models.OneToOneField(IiiRoleCategory, primary_key=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_role_category_name'


class IiiRoleName(ReadOnlyModel):
    iii_role = models.OneToOneField(IiiRole, primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_role_name'


# Sierra user details. Note: iii_user_group_code should be linkable to IiiUserGroup, as indicated
# in the commented-out relationship below. For some reason in our system the iii_user_group table
# is empty, so the relationship doesn't work. If your system is different (or if III fixes this
# in future releases), I've added a test to test and make sure the iii_user_group[_code] field
# and contents of iii_user_group table jive.
class IiiUser(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    roles = models.ManyToManyField(IiiRole, through='IiiUserIiiRole',
                                   blank=True)
    locations = models.ManyToManyField(Location, through='IiiUserLocation',
                                       blank=True)
    workflows = models.ManyToManyField('Workflow', through='IiiUserWorkflow',
                                       blank=True)
    name = models.CharField(max_length=255, unique=True, blank=True)
    location_group_port_number = models.IntegerField(null=True, blank=True)
    iii_user_group_code = models.CharField(max_length=255, blank=True)
    # iii_user_group = models.ForeignKey('IiiUserGroup', db_column='iii_user_group_code', to_field='code', null=True, blank=True)
    full_name = models.CharField(max_length=255, blank=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    accounting_unit = models.ForeignKey(AccountingUnit,
                                        db_column='account_unit',
                                        to_field='code_num', null=True,
                                        blank=True)
    statistic_group = models.ForeignKey(StatisticGroup,
                                        db_column='statistic_group_code_num',
                                        to_field='code_num', null=True,
                                        blank=True)
    system_option_group = models.ForeignKey('SystemOptionGroup',
                                            db_column='system_option_group_code_num',
                                            to_field='code_num', null=True,
                                            blank=True)
    timeout_warning_seconds = models.IntegerField(null=True, blank=True)
    timeout_logout_seconds = models.IntegerField(null=True, blank=True)
    scope_menu_id = models.IntegerField(null=True, blank=True)
    scope_menu_bitmask = models.CharField(max_length=2048, blank=True)
    is_new_account = models.NullBooleanField(null=True, blank=True)
    last_password_change_gmt = models.DateTimeField(null=True, blank=True)
    is_exempt = models.NullBooleanField(null=True, blank=True)
    is_suspended = models.NullBooleanField(null=True, blank=True)
    is_context_only = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_user'


# Apparently links III users and funds? Our iii_user_fund_master table is empty.
class IiiUserFundMaster(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    iii_user = models.ForeignKey(IiiUser, null=True, blank=True)
    fund_master = models.ForeignKey(FundMaster, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_user_fund_master'


# Don't know. Our iii_user_group table is empty.
class IiiUserGroup(ReadOnlyModel):
    id = models.BigIntegerField(primary_key=True)
    code = models.CharField(max_length=255, unique=True, blank=True)
    concurrent_max = models.IntegerField(null=True, blank=True)
    is_independent = models.NullBooleanField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_user_group'


# Links IiiUsers and IiiRoles.
class IiiUserIiiRole(ReadOnlyModel):
    iii_user = models.OneToOneField(IiiUser, primary_key=True)
    iii_role = models.ForeignKey(IiiRole, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_user_iii_role'


# Links IiiUsers and IiiLocations.
class IiiUserLocation(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    iii_user = models.ForeignKey(IiiUser, null=True, blank=True)
    location = models.ForeignKey(Location, db_column='location_code',
                                 to_field='code', null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_user_location'


# Links IiiUsers and Workflows.
class IiiUserWorkflow(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    iii_user = models.ForeignKey(IiiUser, null=True, blank=True, 
                                 db_column='iii_user_id')
    workflow = models.ForeignKey('Workflow', null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'iii_user_workflow'


# Option Groups
class SystemOptionGroup(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    code_num = models.IntegerField(unique=True, null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'system_option_group'


# Option Group Names
class SystemOptionGroupName(ReadOnlyModel):
    system_option_group = models.OneToOneField(SystemOptionGroup, primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'system_option_group_name'


# Sierra workflows, used to group Functions for users
class Workflow(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    display_order = models.IntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, unique=True, blank=True)
    functions = models.ManyToManyField(Function, through='WorkflowFunction',
                                       blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'workflow'


# Links workflows to functions.
class WorkflowFunction(ReadOnlyModel):
    id = models.IntegerField(primary_key=True)
    workflow = models.ForeignKey(Workflow, null=True, blank=True)
    function = models.ForeignKey(Function, null=True, blank=True)
    display_order = models.IntegerField(null=True, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'workflow_function'


# Workflow menu names
class WorkflowName(ReadOnlyModel):
    workflow = models.OneToOneField(Workflow, primary_key=True)
    iii_language = models.ForeignKey(IiiLanguage, null=True, blank=True)
    menu_name = models.CharField(max_length=255, blank=True)

    class Meta(ReadOnlyModel.Meta):
        db_table = 'workflow_name'
