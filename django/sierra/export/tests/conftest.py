"""
Contains pytest fixtures shared by Catalog API `export` app
"""

from __future__ import absolute_import

import decimal
from datetime import datetime

import pytest
import pytz
from django.conf import settings

from base import models as m
from export import sierramarc as sm


@pytest.fixture
def sierra_test_record(sierra_records_by_recnum_range):
    """
    Pytest fixture. Returns a test base.models record instance, based
    on the supplied `recnum_or_label`. If the value is a III recnum,
    then that record is fetched and returned. If it's key value in the
    `named_records` dict, then the corresponding record is returned.
    """
    def _sierra_test_record(recnum_or_label):
        named_records = {
            'bib_no_items': 'b5046625',
        }
        recnum = named_records.get(recnum_or_label, recnum_or_label)
        return sierra_records_by_recnum_range(recnum)[0]
    return _sierra_test_record


@pytest.fixture
def add_varfields_to_record(model_instance, setattr_model_instance):
    """
    Pytest fixture. Add one or more varfields using the specified
    `field_tag`, `marc tag`, `inds` values. `vals` is the list of field
    content; multiple varfield instances are created.

    `r` should be the record instance to add the varfields to.

    `start_occ_num` is the occ_num value for the first varfield
    instance created; occ_num for subsequent fields are incremented. If
    not specified, then the value of the last existing varfield of the
    same type + 1 is used.

    `overwrite_existing`: if True, any varfields matching the field tag
    and marc tag are temporarily modified so they are no longer part of
    that varfield set before the new varfields are created.

    Returns the record instance (`r`), with the varfields added.
    """
    def _add_varfields_to_record(r, field_tag, marc_tag, vals, inds='  ',
                                 start_occ_num=None, overwrite_existing=False):
        vf_id = m.Varfield.objects.all().order_by('-id')[0].id + 1
        if overwrite_existing:
            filt = {'marc_tag': marc_tag, 'varfield_type_code': field_tag}
            for existing_vf in r.record_metadata.varfield_set.filter(**filt):
                setattr_model_instance(existing_vf, 'marc_tag', None)
                setattr_model_instance(existing_vf, 'varfield_type_code', '!')
        if start_occ_num is None:
            filt = {'varfield_type_code': field_tag}
            vf_set = r.record_metadata.varfield_set.filter(**filt)
            start_occ_num = 0
            if len(vf_set) > 0:
                start_occ_num = vf_set.order_by('-occ_num')[0].occ_num + 1
        occ_num = start_occ_num
        for val in vals:
            final_attrs = {
                'id': vf_id, 'record_id': r.record_metadata.id,
                'varfield_type_code': field_tag, 'marc_tag': marc_tag,
                'marc_ind1': inds[0], 'marc_ind2': inds[1],
                'occ_num': occ_num, 'field_content': val
            }
            model_instance(m.Varfield, **final_attrs)
            vf_id += 1
            occ_num += 1
        r.refresh_from_db()
        return r
    return _add_varfields_to_record


@pytest.fixture
def params_to_fields():
    """
    Pytest fixture for creating a list of sierramarc.SierraMarcField
    objects given a list of parameter tuples:
    (tag, contents, indicators).

    `tag` can be a 3-digit numeric MARC tag ('245') or a 4-digit tag,
    where the III field group tag is prepended ('t245' is a t-tagged
    245 field).

    `indicators` is optional. If the MARC tag is 001 to 009, then a
    data field is created from `contents`. Otherwise `contents` is used
    as a list of subfields, and `indicators` defaults to blank, blank.
    """
    def _make_smarc_field(tag, contents, indicators='  '):
        group_tag = ''
        if len(tag) == 4:
            group_tag, tag = tag[0], tag[1:]
        if int(tag) < 10:
            return sm.SierraMarcField(tag, data=contents)
        return sm.SierraMarcField(tag, subfields=contents,
                                  indicators=indicators, group_tag=group_tag)

    def _make_smarc_fields(fparams):
        fields = []
        for fp in fparams:
            fields.append(_make_smarc_field(*fp))
        return fields
    return _make_smarc_fields


@pytest.fixture
def fieldstrings_to_fields():
    """
    Pytest fixture. Given a list of MARC field strings copied/pasted
    from the LC or OCLC website, returns a list of SierraMarcField
    objects.
    """
    def _fieldstrings_to_fields(field_strings):
        return [sm.SierraMarcField.make_from_string(s) for s in field_strings]
    return _fieldstrings_to_fields


@pytest.fixture
def add_marc_fields():
    """
    Pytest fixture for adding fields to the given `bib` (pymarc Record
    or sierramarc.SierraMarcRecord object). If `overwrite_existing` is
    True, which is the default, then all new MARC fields will overwrite
    existing fields with the same tag.

    `fields` must be a list of pymarc.Field or s2m.SierraMarcField
    objects.
    """
    def _add_marc_fields(bib, fields, overwrite_existing=True):
        if overwrite_existing:
            for f in fields:
                bib.remove_fields(f.tag)
        bib.add_field(*fields)
        return bib
    return _add_marc_fields


@pytest.fixture
def make_record_metadata_instance(model_instance):
    """
    Pytest fixture. Make a temporary test base.models.RecordMetadata
    instance using the provided `rectype` and `attrs`. Returns the
    model instance.
    """
    def _make_record_metadata_instance(rectype, attrs):
        rm_objs = m.RecordMetadata.objects.all()
        rec_id = rm_objs.order_by('-id')[0].id + 1
        these_rm_objs = rm_objs.filter(record_type_id=rectype)
        recnum = these_rm_objs.order_by('-id')[0].record_num + 1
        final_attrs = {
            'id': rec_id, 'record_type_id': rectype, 'record_num': recnum,
            'creation_date_gmt': attrs.get('creation_date_gmt',
                                           datetime.now(pytz.utc)),
            'deletion_date_gmt': attrs.get('deletion_date_gmt', None),
            'campus_code': attrs.get('campus_code', ''),
            'num_revisions': attrs.get('num_revisions', 0),
            'record_last_updated_gmt': attrs.get('record_last_updated_gmt',
                                                 datetime.now(pytz.utc)),
            'previous_last_updated_gmt': attrs.get('previous_last_updated_gmt',
                                                   None),
        }
        return model_instance(m.RecordMetadata, **final_attrs)
    return _make_record_metadata_instance


@pytest.fixture
def get_or_make_location_instances(model_instance, setattr_model_instance):
    """
    Pytest fixture that takes a list of dicts (`loc_info_list`) and
    ensures that Location model instances exist matching the provided
    details. (It creates or updates instances as needed.) It returns
    the list of location instances.
    """
    def _get_or_make_location_instances(loc_info_list):
        locations = []
        loc_objs = m.Location.objects.all()
        lname_objs = m.LocationName.objects.all()
        lang_id = m.IiiLanguage.objects.get(code=settings.III_LANGUAGE_CODE).id

        for supplied_info in loc_info_list:
            info = supplied_info.copy()
            code, name, lname = info['code'], info.pop('name', None), None
            try:
                loc = loc_objs.get(code=code)
            except m.Location.DoesNotExist:
                info['id'] = loc_objs.order_by('-id')[0].id + 1
                loc = model_instance(m.Location, **info)
            else:
                try:
                    lname = lname_objs.get(iii_language_id=lang_id,
                                           location_id=loc.id)
                except m.LocationName.DoesNotExist:
                    pass
                else:
                    if name and lname.name != name:
                        setattr_model_instance(lname, 'name', name)
                        loc.refresh_from_db()
            if lname is None:
                lname_info = {
                    'location_id': loc.id, 'iii_language_id': lang_id,
                    'name': name if name else 'Test'
                }
                lname = model_instance(m.LocationName, **lname_info)
                loc.refresh_from_db()
            locations.append(loc)
        return locations
    return _get_or_make_location_instances


@pytest.fixture
def add_items_to_bib(model_instance, make_record_metadata_instance,
                     add_varfields_to_record):
    """
    Pytest fixture that adds one or more item records to a bib record
    (base.models.ItemRecord and base.models.BibRecord instances).

    Provide the `bib` BibRecord instance you want to attach items to
    and an `info_for_items` data structure containing details about the
    items you want to create. Structure `item_info` as follows.

    info_for_items = [
        {
            'record_metadata': **RecordMetadata_model_attrs,
            'attrs': **ItemRecord_model_attrs,
            'varfields': [
                'c', '090', ['|aMT 1001 .C35 1992'], '  '
            ]
    ]

    The `varfields` element is a list of tuples:
        (field_tag, marc_tag, values_to_add, indicators)

    If adding a non-MARC varfield, marc_tag should be None. The 4th
    tuple value is optional and may be left off; it will default to
    '  ' (blank indicators).

    Any keys not provided are set to sensible defaults.
    """
    def _make_item(info):
        rm_attrs = info.get('record_metadata', {})
        attrs = info.get('attrs', {})
        varfields = info.get('varfields', [])
        record_metadata = make_record_metadata_instance('i', rm_attrs)
        item_attrs = {
            'id': record_metadata.id,
            'record_metadata_id': record_metadata.id,
            'icode1': attrs.get('icode1', 0),
            'icode2': attrs.get('icode2', '-'),
            'itype_id': attrs.get('itype_id', 1),
            'location_id': attrs.get('location_id', 'test'),
            'item_status_id': attrs.get('item_status_id', '-'),
            'is_inherit_loc': attrs.get('is_inherit_loc', False),
            'price': attrs.get('price', decimal.Decimal(0)),
            'last_checkin_gmt': attrs.get('last_checkin_gmt', None),
            'checkout_total': attrs.get('checkout_total', 0),
            'renewal_total': attrs.get('renewal_total', 0),
            'last_year_to_date_checkout_total':
                attrs.get('last_year_to_date_checkout_total', 0),
            'year_to_date_checkout_total':
                attrs.get('year_to_date_checkout_total', 0),
            'copy_num': attrs.get('copy_num', 0),
            'checkout_statistic_group_id':
                attrs.get('checkout_statistic_group_id', 0),
            'last_patron_record_id':
                attrs.get('last_patron_record_id', None),
            'checkin_statistics_group_id':
                attrs.get('checkin_statistics_group_id', 0),
            'use3_count': attrs.get('use3_count', 0),
            'last_checkout_gmt': attrs.get('last_checkout_gmt', None),
            'internal_use_count': attrs.get('internal_use_count', 0),
            'copy_use_count': attrs.get('copy_use_count', 0),
            'item_message_code': attrs.get('item_message_code', '-'),
            'opac_message_code': attrs.get('opac_message_code', '-'),
            'holdings_code': attrs.get('holdings_code', '-'),
            'save_itype_id': attrs.get('save_itype_id', None),
            'save_location_id': attrs.get('save_location_id', None),
            'save_checkout_total': attrs.get('save_checkout_total', None),
            'old_location_id': attrs.get('old_location_id', None),
            'is_suppressed': attrs.get('is_suppressed', False)
        }
        item = model_instance(m.ItemRecord, **item_attrs)
        for vf in varfields:
            field_tag, marc_tag, vals = vf[0:3]
            inds = vf[3] if len(vf) == 4 else '  '
            item = add_varfields_to_record(
                item, field_tag, marc_tag, vals, inds=inds,
                overwrite_existing=False
            )
        return item

    def _add_items_to_bib(bib, info_for_items, start_order_num=None,
                          overwrite_existing=False):
        if overwrite_existing:
            for link in bib.bibrecorditemrecordlink_set.all():
                setattr_model_instance(link, 'bib_record_id', None)
            bib.refresh_from_db()
        if start_order_num is None:
            link_set = bib.bibrecorditemrecordlink_set.all()
            start_order_num = 0
            if len(link_set) > 0:
                last_link = link_set.order_by('-item_display_order')[0]
                start_order_num = last_link.item_display_order + 1

        link_objs = m.BibRecordItemRecordLink.objects.all()
        link_id = link_objs.order_by('-id')[0].id + 1
        order_num = start_order_num
        for info in info_for_items:
            item = _make_item(info)
            link_attrs = {
                'id': link_id, 'bib_record_id': bib.id,
                'item_record_id': item.id, 'items_display_order': order_num,
                'bibs_display_order': 0
            }
            model_instance(m.BibRecordItemRecordLink, **link_attrs)
            order_num += 1
            link_id += 1
        bib.refresh_from_db()
        return bib
    return _add_items_to_bib


@pytest.fixture
def add_locations_to_bib(model_instance, setattr_model_instance):
    """
    Pytest fixture that adds one or more locations to a bib record.

    `bib` is the BibRecord model instance you want to add locations to.
    `locations` is a list of Location instances to attach to the bib.

    If `overwrite_existing` is True, then any preexisting locations
    attached to the bib will be overwritten (then restored after the
    test runs). Otherwise, new locations will be added to the existing
    ones.
    """
    def _add_locations_to_bib(bib, locations, overwrite_existing=False):
        if overwrite_existing:
            for bibloc in bib.bibrecordlocation_set.all():
                setattr_model_instance(bibloc, 'bib_record_id', None)
            bib.refresh_from_db()
        bibloc_objs = m.BibRecordLocation.objects.all()
        bibloc_id = bibloc_objs.order_by('-id')[0].id + 1
        for loc in locations:
            bibloc_attrs = {
                'id': bibloc_id, 'bib_record_id': bib.id,
                'location_id': loc.code, 'display_order': 0
            }
            model_instance(m.BibRecordLocation, **bibloc_attrs)
            bibloc_id += 1
        bib.refresh_from_db()
        return bib
    return _add_locations_to_bib


