'''
Contains code for Haystack to build search indexes for Sierra API.

IMPORTANT: All Solr indexes should be managed by Haystack EXCEPT any
SolrMarc indexes. We don't want Haystack indexing documents or even
generating schemas for our SolrMarc indexes. But we do want to allow
Haystack to search them. In order to do that, we have to include a
BibIndex class with the bare minimum attributes to make it a valid
Haystack SearchIndex class.
'''
from __future__ import unicode_literals
import re

from haystack import indexes

from django.core.exceptions import ObjectDoesNotExist

from export import sierra2marc as s2m
from . import models as sierra_models
from utils import helpers

import logging
# set up logger, for debugging
logger = logging.getLogger('sierra.custom')


def cat_fields(data, include=(), exclude=()):
    '''
    Takes a 'prepared_data' structure from an indexes.SearchIndex-
    derived object and unnests it, returning a space-joined string.
    Specify 'include' to include only certain fields or 'exclude' to
    include all fields except the exclusions. This is useful for
    deriving the Haystack full-text document field ("text") for
    Haystack indexes. Just call this using the index object's "prepare"
    method so you're using the prepare_FOO version of each field.
    '''
    values = []
    for i in data:
        if data[i] is not None and ((include and i in include) 
                                    or (exclude and i not in exclude)):
            if type(data[i]) is list or type(data[i]) is tuple:
                values.append(' '.join([unicode(j) for j in data[i]]))
            else:
                values.append(unicode(data[i]))
    return ' '.join(values)


class CustomQuerySetIndex(indexes.SearchIndex):
    '''
    Custom implementation of SearchIndex class that lets us pass
    in a custom queryset at initialization for update purposes--lets
    us tell Haystack to index this particular queryset when an update()
    is issued. To do this, use kwarg queryset. THIS IS TOTALLY
    OPTIONAL. Not specifying a default queryset is totes okay.

    This also overrides the update(), clear(), and reindex() methods to
    expose a "commit" option, which allows you to perform an update
    without committing it to Solr.

    Finally, this provides commit() and optimize() methods, which allow
    you to commit changes and optimize the index manually. May only
    work with the solr backend.
    '''
    def __init__(self, *args, **kwargs):
        default_queryset = kwargs.pop('queryset', None)
        super(indexes.SearchIndex, self).__init__(*args, **kwargs)
        if default_queryset is not None:
            self.default_queryset = default_queryset

    def index_queryset(self, using=None):
        try:
            return self.default_queryset
        except AttributeError:
            return self.get_model()._default_manager.all()

    def update(self, using=None, commit=True):
        backend = self.get_backend(using)

        if backend is not None:
            backend.update(self, self.index_queryset(), commit=commit)

    def clear(self, using=None, commit=True):
        backend = self.get_backend(using)

        if backend is not None:
            backend.clear(models=[self.get_model()], commit=commit)

    def reindex(self, using=None, commit=True):
        self.clear(using=using, commit=commit)
        self.update(using=using, commit=commit)

    def update_object(self, instance, using=None, commit=True, **kwargs):
        if self.should_update(instance, **kwargs):
            backend = self.get_backend(using)

            if backend is not None:
                backend.update(self, [instance], commit=commit)

    def commit(self, using=None):
        backend = self.get_backend(using)

        if backend is not None:
            backend.conn.commit()

    def optimize(self, using=None):
        backend = self.get_backend(using)

        if backend is not None:
            backend.conn.optimize()


class BibIndex(CustomQuerySetIndex, indexes.Indexable):
    '''
    WARNING: This is a total hack to force Haystack to register our
    BibRecord model as being indexed by Haystack. This is the only way
    to get it to search our non-Haystack-managed SolrMarc index(es).
    '''
    text = indexes.CharField(document=True)
    
    def get_model(self):
        return sierra_models.BibRecord


class MarcIndex(CustomQuerySetIndex, indexes.Indexable):
    '''
    Class to index MARC in Solr so that it's searchable by field and
    subfield.
    '''
    id = indexes.IntegerField()
    text = indexes.CharField(document=True)
    record_number = indexes.FacetCharField()
    leader = indexes.FacetCharField(stored=False)
    mf_001 = indexes.FacetCharField(stored=False)
    mf_003 = indexes.FacetCharField(stored=False)
    mf_005 = indexes.FacetCharField(stored=False)
    mf_007 = indexes.FacetCharField(stored=False)
    mf_008 = indexes.FacetCharField(stored=False)
    json = indexes.FacetCharField()

    def __init__(self, *args, **kwargs):
        super(MarcIndex, self).__init__(*args, **kwargs)
        for mf in range(10,1000):
            setattr(self, 'mf_{:03d}'.format(mf),
                    indexes.FacetCharField(stored=False))
            for sf in ['0','1','2','3','4','5','6','7','8','9','a','b','c','d',
                       'e','f','g','h','i','j','k','l','m','n','o','p','q','r',
                       's','t','u','v','w','x','y','z']:
                setattr(self, 'sf_{:03d}{}'.format(mf, sf),
                        indexes.FacetCharField(stored=False))

    def get_model(self):
        return sierra_models.BibRecord

    def prepare_id(self, obj):
        return int(obj.pk)

    def prepare_record_number(self, obj):
        return obj.record_metadata.get_iii_recnum(True)

    def prepare(self, obj):
        self.prepared_data = super(MarcIndex, self).prepare(obj)
        try:
            record = s2m.S2MarcBatch(obj).to_marc()[0]
        except IndexError:
            pass
        else:
            self.prepared_data['json'] = record.as_json()
            self.prepared_data['leader'] = record.leader
            for field in record.get_fields():
                mf_name = 'mf_{}'.format(field.tag)
                if field.is_control_field():
                    self.prepared_data[mf_name] = field.data
                else:
                    data = self.prepared_data.get(mf_name, [])
                    data.append(unicode(field)[6:])
                    self.prepared_data[mf_name] = data
                    sf = field.subfields
                    for s_tag, s_data in [sf[n:n+2] for n in range(0,len(sf),2)]:
                        sf_name = 'sf_{}{}'.format(field.tag, s_tag)
                        data = self.prepared_data.get(sf_name, [])
                        data.append(s_data)
                        self.prepared_data[sf_name] = data

        return self.prepared_data



class MetadataBaseIndex(CustomQuerySetIndex, indexes.Indexable):
    '''
    Subclassable class for creating III "metadata" indexes -- 
    Locations, Itypes, etc. (E.g. admin parameters.) Most of them are
    just key/value pairs (code/label), so they follow a predictable
    pattern. In each subclass just be sure to set your model and
    type_name and any fields and/or prepare methods that need to be
    customized. The prepare_label method will always need to be
    overridden. See LocationIndex, etc., below for examples.
    '''
    model = None
    type_name = ''
    text = indexes.CharField(document=True, use_template=False)
    code = indexes.FacetCharField(model_attr='code')
    label = indexes.FacetCharField()
    type = indexes.FacetCharField()
    
    def get_model(self):
        return self.model

    def prepare_label(self, obj):
        return None

    def prepare_type(self, obj):
        return self.type_name

    def prepare(self, obj):
        self.prepared_data = super(MetadataBaseIndex, self).prepare(obj)
        include = ('code', 'label', 'type')
        self.prepared_data["text"] = cat_fields(self.prepared_data,
                                                include=include)
        return self.prepared_data


class LocationIndex(MetadataBaseIndex):
    model = sierra_models.Location
    type_name = 'Location'
    
    def prepare_label(self, obj):
        return obj.locationname.name


class ItypeIndex(MetadataBaseIndex):
    model = sierra_models.ItypeProperty
    type_name = 'Itype'
    code = indexes.CharField(model_attr='code_num')
    
    def prepare_label(self, obj):
        return obj.itypepropertyname.name


class ItemStatusIndex(MetadataBaseIndex):
    model = sierra_models.ItemStatusProperty
    type_name = 'ItemStatus'

    def prepare_label(self, obj):
        return obj.itemstatuspropertyname.name


class ItemIndex(CustomQuerySetIndex, indexes.Indexable):
    type_name = 'Item'
    id = indexes.IntegerField()
    text = indexes.CharField(document=True, use_template=False)
    type = indexes.FacetCharField()
    record_number = indexes.FacetCharField()
    parent_bib_id = indexes.IntegerField()
    parent_bib_record_number = indexes.CharField()
    parent_bib_title = indexes.CharField()
    parent_bib_main_author = indexes.CharField()
    parent_bib_publication_year = indexes.IntegerField()
    call_number = indexes.FacetCharField(null=True)
    call_number_sort = indexes.FacetCharField(null=True)
    call_number_search = indexes.FacetCharField(null=True)
    call_number_type = indexes.FacetCharField(null=True)
    volume = indexes.CharField(null=True)
    volume_sort = indexes.FacetCharField(null=True)
    copy_number = indexes.IntegerField(model_attr='copy_num', null=True)
    barcode = indexes.FacetCharField(null=True)
    long_messages = indexes.MultiValueField(null=True)
    internal_notes = indexes.MultiValueField(null=True)
    public_notes = indexes.MultiValueField(null=True)
    local_code1 = indexes.IntegerField(model_attr='icode1', null=True)
    number_of_renewals = indexes.IntegerField(model_attr='renewal_total',
        null=True)
    item_type_code = indexes.FacetCharField(null=True)
    price = indexes.DecimalField(model_attr='price', null=True)
    # NOTE: item_message_code and opac_message_code are left out
    # because their corresponding labels are not available in the
    # Sierra database.
    #item_message_code = indexes.CharField(model_attr='item_message_code',
    #    null=True)
    #opac_message_code = indexes.CharField(model_attr='opac_message_code',
    #    null=True)
    internal_use_count = indexes.IntegerField(model_attr='internal_use_count',
        null=True)
    copy_use_count = indexes.IntegerField(model_attr='copy_use_count',
        null=True)
    iuse3_count = indexes.IntegerField(model_attr='use3_count', null=True)
    total_checkout_count = indexes.IntegerField(model_attr='checkout_total',
        null=True)
    total_renewal_count = indexes.IntegerField(model_attr='renewal_total',
        null=True)
    year_to_date_checkout_count = indexes.IntegerField(
        model_attr='year_to_date_checkout_total', null=True)
    last_year_to_date_checkout_count = indexes.IntegerField(
        model_attr='last_year_to_date_checkout_total', null=True)
    location_code = indexes.FacetCharField(null=True)
    status_code = indexes.FacetCharField(model_attr='item_status', null=True)
    due_date = indexes.DateTimeField(null=True)
    checkout_date = indexes.DateTimeField(null=True)
    last_checkin_date = indexes.DateTimeField(model_attr='last_checkin_gmt',
        null=True)
    overdue_date = indexes.DateTimeField(null=True)
    recall_date = indexes.DateTimeField(null=True)
    record_creation_date = indexes.DateTimeField(
        model_attr='record_metadata__creation_date_gmt', null=True)
    record_last_updated_date = indexes.DateTimeField(
        model_attr='record_metadata__record_last_updated_gmt', null=True)
    record_revision_number = indexes.IntegerField(
        model_attr='record_metadata__num_revisions', null=True)
    suppressed = indexes.BooleanField()
    _version_ = indexes.IntegerField()
    
    def get_model(self):
        return sierra_models.ItemRecord

    def get_call_number(self, obj):
        return obj.get_shelving_call_number_tuple()

    def prepare_type(self, obj):
        return self.type_name

    def prepare_id(self, obj):
        return int(obj.pk)

    def prepare_record_number(self, obj):
        return obj.record_metadata.get_iii_recnum(True)

    def prepare_parent_bib_id(self, obj):
        try:
            bib = obj.bibrecorditemrecordlink_set.all()[0].bib_record
            return bib.record_metadata.id
        except IndexError:
            return None
    
    def prepare_parent_bib_record_number(self, obj):
        try:
            bib = obj.bibrecorditemrecordlink_set.all()[0].bib_record
            return bib.record_metadata.get_iii_recnum(True)
        except IndexError:
            return None

    def prepare_parent_bib_title(self, obj):
        try:
            bib = obj.bibrecorditemrecordlink_set.all()[0].bib_record
            return bib.bibrecordproperty_set.all()[0].best_title
        except IndexError:
            return None

    def prepare_parent_bib_main_author(self, obj):
        try:
            bib = obj.bibrecorditemrecordlink_set.all()[0].bib_record
            return bib.bibrecordproperty_set.all()[0].best_author
        except IndexError:
            return None

    def prepare_parent_bib_publication_year(self, obj):
        try:
            bib = obj.bibrecorditemrecordlink_set.all()[0].bib_record
            return bib.bibrecordproperty_set.all()[0].publish_year
        except IndexError:
            return None

    def prepare_call_number(self, obj):
        '''
        Prepare call_number field. We only want one call number per
        item, so we use the item call number (if present) or the bib
        call number.
        '''
        (cn, ctype) = self.get_call_number(obj)
        return cn

    def prepare_call_number_type(self, obj):
        '''
        Prepare call_number_type field. This determines the "type" of
        call number on an item: lc, dewey, sudoc, or other. (Different
        types of call numbers sort differently.)
        '''
        (cn, ctype) = self.get_call_number(obj)
        return ctype
    
    def prepare_call_number_sort(self, obj):
        '''
        Prepare call_number_sort field. This prepares a version of each
        call_number that should sort correctly when sorted as a string. 
        LPCD 100,000 --> LPCD!0000100000,
        PT8142.Z5 A5613 1988 --> PT!0000008142!Z5!A!0000005613
        !0000001988. 
        '''
        (cn, ctype) = self.get_call_number(obj)
        if cn is not None:
            try:
                cn = helpers.NormalizedCallNumber(cn, ctype).normalize()
            except helpers.CallNumberError:
                cn = helpers.NormalizedCallNumber(cn, 'other').normalize()
        return cn

    def prepare_call_number_search(self, obj):
        '''
        Prepare call_number_search field. This prepares a version of a
        call number that's normalized for searching. (Should not
        depend on call number type.)
        '''
        (cn, ctype) = self.get_call_number(obj)
        if cn is not None:
            cn = helpers.NormalizedCallNumber(cn, 'search').normalize()
        return cn

    def prepare_volume(self, obj):
        '''
        Prepare the volume number; grab it from the varfields on this
        item record.
        '''
        item_vf_set = obj.record_metadata.varfield_set.all()
        return helpers.get_varfield_vals(item_vf_set, 'v')

    def prepare_volume_sort(self, obj):
        '''
        Prepares a sortable volume field, like call_number_sort.
        '''
        vol = self.prepare_volume(obj)
        if vol is not None:
            try:
                vol = helpers.NormalizedCallNumber(vol).normalize()
            except helpers.CallNumberError:
                vol = None
        return vol

    def prepare_barcode(self, obj):
        '''
        Prepare the barcode; grab it from the varfields on this item
        record.
        '''
        item_vf_set = obj.record_metadata.varfield_set.all()
        return helpers.get_varfield_vals(item_vf_set, 'b')

    def prepare_long_messages(self, obj):
        '''
        Prepare the "long_messages" (or m-tagged varfields)
        '''
        item_vf_set = obj.record_metadata.varfield_set.all()
        return helpers.get_varfield_vals(item_vf_set, 'm', many=True)

    def prepare_internal_notes(self, obj):
        '''
        Prepare the internal_notes (or x- and n-tagged varfields)
        '''
        item_vf_set = obj.record_metadata.varfield_set.all()
        inotes = helpers.get_varfield_vals(item_vf_set, 'x', many=True)
        inotes.extend(helpers.get_varfield_vals(item_vf_set, 'n', many=True))
        return inotes

    def prepare_public_notes(self, obj):
        '''
        Prepare public_notes (or i-tagged varfields)
        '''
        item_vf_set = obj.record_metadata.varfield_set.all()
        return helpers.get_varfield_vals(item_vf_set, 'p', many=True)

    def prepare_item_type_code(self, obj):
        '''
        Prepare item_type_code field--convert from int to str
        '''
        return str(obj.itype.code_num)

    def prepare_location_code(self, obj):
        '''
        Prepare item location--just grab the code from the location.
        We're not using the location name here because we don't want
        to have to reindex items just because a location label changes.
        '''
        code = ''
        try:
            code = obj.location.code
        except (sierra_models.Location.DoesNotExist, AttributeError):
            pass
        return code
    
    def prepare_due_date(self, obj):
        '''
        Due date is from any checkout records attached to the item.
        '''
        try:
            return obj.checkout.due_gmt
        except ObjectDoesNotExist:
            return None
    
    def prepare_checkout_date(self, obj):
        try:
            return obj.checkout.checkout_gmt
        except ObjectDoesNotExist:
            return None

    def prepare_overdue_date(self, obj):
        try:
            return obj.checkout.overdue_gmt
        except ObjectDoesNotExist:
            return None

    def prepare_recall_date(self, obj):
        try:
            return obj.checkout.recall_gmt
        except ObjectDoesNotExist:
            return None

    def prepare_suppressed(self, obj):
        '''
        We want suppressed to be true if the bib OR item is suppressed.
        '''
        if (obj.icode2 != '-'):
            return True
        else:
            bib = obj.bibrecorditemrecordlink_set.all()
            if (bib and bib[0].bib_record.is_suppressed):
                return True
            else:
                return False
    
    def prepare(self, obj):
        self.prepared_data = super(ItemIndex, self).prepare(obj)
        include = ('record_number', 'parent_bib_record_number', 'call_number',
                   'call_number_parts', 'volume', 'barcode', 'long_messages',
                   'internal_notes', 'public_notes', 'opac_message',
                   'location_code', 'parent_bib_title',
                   'parent_bib_main_author', 'parent_bib_publication_year')
        self.prepared_data["text"] = cat_fields(self.prepared_data,
                                                include=include)
        return self.prepared_data


class ElectronicResourceIndex(CustomQuerySetIndex, indexes.Indexable):
    type_name = 'eResource'
    h_lists = {}
    id = indexes.IntegerField()
    text = indexes.CharField(document=True, use_template=False)
    type = indexes.FacetCharField()
    record_number = indexes.FacetCharField()
    eresource_type = indexes.FacetCharField(null=True)
    publisher = indexes.CharField(null=True)
    title = indexes.CharField(null=True)
    alternate_titles = indexes.MultiValueField(null=True)
    subjects = indexes.MultiValueField(null=True)
    summary = indexes.CharField(null=True)
    internal_notes = indexes.MultiValueField(null=True)
    public_notes = indexes.MultiValueField(null=True)
    alert = indexes.CharField(null=True)
    holdings = indexes.MultiValueField(null=True)
    external_url = indexes.CharField(null=True, indexed=False)
    record_creation_date = indexes.DateTimeField(
        model_attr='record_metadata__creation_date_gmt', null=True)
    record_last_updated_date = indexes.DateTimeField(
        model_attr='record_metadata__record_last_updated_gmt', null=True)
    record_revision_number = indexes.IntegerField(
        model_attr='record_metadata__num_revisions', null=True)
    suppressed = indexes.BooleanField()
    _version_ = indexes.IntegerField()
    
    def get_model(self):
        return sierra_models.ResourceRecord

    def prepare_id(self, obj):
        return int(obj.pk)

    def prepare_type(self, obj):
        return self.type_name

    def prepare_record_number(self, obj):
        return obj.record_metadata.get_iii_recnum(True)

    def prepare_eresource_type(self, obj):
        ret_val = None
        try:
            ret_val = obj.record_metadata.varfield_set.filter(
                varfield_type_code='v')[0].field_content
        except IndexError:
            pass
        return ret_val

    def prepare_publisher(self, obj):
        ret_val = None
        try:
            ret_val = obj.access_provider.record_metadata.varfield_set.filter(
                varfield_type_code='t')[0].field_content
        except Exception:
            pass
        return ret_val

    def prepare_title(self, obj):
        ret_val = None
        try:
            ret_val = obj.record_metadata.varfield_set.filter(
                varfield_type_code='t')[0].field_content
        except IndexError:
            pass
        return ret_val

    def prepare_alternate_titles(self, obj):
        full_set = obj.record_metadata.varfield_set.filter(
            varfield_type_code='x')
        ret_val = set([x.field_content for x in full_set])

        try:
            res_id = obj.record_metadata.varfield_set.filter(
               varfield_type_code='p')[0].field_content
        except IndexError:
            pass
        else:
            ret_val.add(res_id)

        return list(ret_val)

    def prepare_subjects(self, obj):
        full_set = obj.record_metadata.varfield_set.filter(
            varfield_type_code='d')
        ret_val = [x.field_content for x in full_set]
        return ret_val

    def prepare_summary(self, obj):
        ret_val = None
        try:
            ret_val = obj.record_metadata.varfield_set.filter(
                varfield_type_code='e')[0].field_content
        except IndexError:
            pass
        return ret_val

    def prepare_internal_notes(self, obj):
        full_set = obj.record_metadata.varfield_set.filter(
            varfield_type_code='n')
        ret_val = [x.field_content for x in full_set]
        return ret_val

    def prepare_public_notes(self, obj):
        full_set = obj.record_metadata.varfield_set.filter(
            varfield_type_code='f')
        ret_val = [x.field_content for x in full_set]
        return ret_val

    def prepare_alert(self, obj):
        ret_val = None
        try:
            ret_val = obj.record_metadata.varfield_set.filter(
                varfield_type_code='k')[0].field_content
        except IndexError:
            pass
        return ret_val

    def prepare_holdings(self, obj):
        ret_val = []
        data_map = []
        rec_num = obj.record_metadata.get_iii_recnum(True)

        for h in obj.holding_records.all():
            try:
                h_rec_num = h.record_metadata.get_iii_recnum(True)
                bib_vf = h.bibrecord_set.all()[0].record_metadata.\
                            varfield_set.all()
            except IndexError:
                pass
            else:
                title = helpers.get_varfield_vals(bib_vf, 't', '245',
                            cm_kw_params={'subfields': 'a'},
                            content_method='display_field_content')
                ret_val.append(title)
                data_map.append(h_rec_num)

        self.h_lists[rec_num] = data_map
        return ret_val

    def prepare_external_url(self, obj):
        return obj.get_url()

    def prepare_suppressed(self, obj):
        if (obj.suppress_code != '-'):
            return True
        else:
            return False

    def prepare(self, obj):
        self.prepared_data = super(ElectronicResourceIndex, self).prepare(obj)
        # remove the title from the Alternate Titles list, if it's there
        title = self.prepared_data['title']
        if title in self.prepared_data['alternate_titles']:
            self.prepared_data['alternate_titles'].remove(title)

        include = ('record_number', 'eresource_type', 'publisher', 'title',
                   'alternate_titles', 'subjects', 'summary', 'holdings')
        self.prepared_data['text'] = cat_fields(self.prepared_data,
                                                include=include)
        return self.prepared_data
