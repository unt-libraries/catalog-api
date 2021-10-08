"""
Contains various utility functions and classes for Sierra API project
code.
"""
from __future__ import unicode_literals
from __future__ import absolute_import
import operator
import re

from django.db.models import Q
from six import text_type
from functools import reduce


def reduce_filter_kwargs(filters):
    """
    Given a list of filter_kwargs, where each list should be 'ORed'
    with each other, this function returns a Q filter that can be
    passed straight into a QuerySet.filter() call.
    """
    return reduce(operator.or_,
                  [reduce(operator.and_,
                          [Q(**{key: filter[key]}) for key in filter])
                   for filter in filters])


def get_varfield_vals(vf_set, tag, marc_tags=['*'], many=False,
                      content_method=None, cm_kw_params={}):
    """
    This method lets us get varfield data from a Django ORM object
    easily without triggering another DB query (e.g. by using a
    queryset.filter).

    It just loops through the varfield set and returns field(s)
    matching the provided tag. If many is False it just returns the
    first match as a string (or None); if many is True it returns
    an array of all matching values (or an empty array). If
    content_method is specified, then it uses that method (e.g.
    model method) to pull out the content, otherwise it just uses
    vf.field_content.

    Note that you can specify one, multiple, or None marc_tags to
    match in addition to the III field tag. * (default) is a
    wildcard that ignores the marc tag and matches only based on
    III field tag; None means the VF has no MARC tag and is
    therefore non-MARC data.
    """
    values = [] if many else None
    if not isinstance(marc_tags, (list, tuple)):
        marc_tags = [marc_tags]

    if vf_set is not None:
        for vf in vf_set:
            if (vf.varfield_type_code == tag and
                    (marc_tags == ['*'] or vf.marc_tag in marc_tags)):
                if content_method is not None:
                    content = getattr(vf, content_method)(**cm_kw_params)
                else:
                    content = vf.field_content
                if many:
                    values.append(content)
                else:
                    return content
    return values


class CallNumberError(Exception):
    pass


class NormalizedCallNumber(object):
    """
    Class to normalize a call number string--e.g., to make it sortable.
    Intended to handle multiple types of call numbers, such as dewey,
    library of congress (lc), gov docs (sudoc), etc.

    Example Usage:

    >>> callnumber = 'M12.B12 B3 1921'
    >>> ncn = NormalizedCallNumber(callnumber, 'lc').normalize()
    >>> ncn
    u'M!0000000012!B12!B3!0000001921'
    >>> ncn = NormalizedCallNumber(callnumber, 'search').normalize()
    >>> ncn
    u'M12B12B31921'

    Specify what kind of normalization you want to do using the "kind"
    parameter upon init. If you want to add new kinds, simply add a
    _process_{kind} method, and then initialize new objects using that
    kind string. Use the normalize method to get the normalized string.
    """
    space_char = '!'

    def __init__(self, call, kind='default'):
        self.kind = kind
        self.call = call
        self.normalized_call = None

    def normalize(self):
        """
        Parses the call number in self.call based on the string in
        self.kind. Stores it in self.normalized_call and returns it.
        """
        kind = self.kind
        call = self.call
        process_it = getattr(self, '_process_{}'.format(kind),
                             self._process_default)
        try:
            call = process_it(text_type(call))
        except CallNumberError:
            raise

        self.normalized_call = re.sub(r' ', self.space_char, call)
        return self.normalized_call

    def _process_sudoc(self, call=None):
        """
        Processes sudoc (gov docs) numbers.
        """
        call = self.call if call is None else call
        call = call.upper()
        call = self._normalize_spaces(call)

        # stem and suffix, separated by :, sort independently, so we
        # need to split it, then parse each portion separately.
        try:
            stem, suffix = call.split(':')
        except ValueError:
            # if there's no colon, treat the whole thing as the stem
            # with a blank suffix
            stem, suffix = (call, '')

        # need to ensure stems all have the same format so that the
        # sort compares stem to stem correctly. Stems may or may not
        # have, e.g., /7-1 at the end. We add whatever is missing.
        stem = re.sub(r'\.', ' ', stem)
        if not re.search(r'/', stem):
            stem = '{}/0'.format(stem)
        if not re.search(r'-', stem):
            stem = '{}-0'.format(stem)

        # For suffixes: years (which, pre-2000, left off the leading 1)
        # sort first. Letters sort next. Non-year numbers sort third.
        # So to force sorting first, we add a period to the beginning
        # of years. (Letters are taken care of below.)
        suffix = re.sub(r'\.(\d)', r' \1', suffix)
        suffix = re.sub(r'(^|\D)(9\d\d)($|\D)', r'\1.\2\3', suffix)
        suffix = re.sub(r'(^|\D)(2\d\d\d)($|\D)', r'\1.\2\3', suffix)

        # Now we reattach the stem and the suffix and process the whole
        # thing as a string. We want numbers--but not numbers after
        # decimal points--to be converted to zero-padded strings.
        ret = '{}:{}'.format(stem, suffix)
        ret = self._separate_numbers(ret)
        ret = self._numbers_to_sortable_strings(ret, decimals=False)
        # Here's where we add the periods to the beginning of letters.
        # Note that letters that belong to the first part of the stem
        # don't get periods, while others do.
        ret = re.sub(r'([^A-Z .])([A-Z]+)', r'\1.\2', ret)
        # Finally, we ensure things are spaced out reasonably.
        ret = re.sub(r'([^.])(\.)', r'\1 \2', ret)
        ret = re.sub(r'[/\-:]', ' ', ret)
        ret = self._normalize_spaces(ret)
        return ret

    def _process_dewey(self, call=None):
        """
        Processes Dewey Decimal call numbers.
        """
        call = self.call if call is None else call
        call = call.upper()
        call = self._normalize_spaces(call)
        call = self._separate_numbers(call)
        call = self._numbers_to_sortable_strings(call)
        return call

    def _process_lc(self, call=None):
        """
        Processes Library of Congress call numbers.
        """
        call = self.call if call is None else call
        call = self._normalize_spaces(call)
        call = re.sub(r'([a-z])\.\s*', r'\1 ', call)
        call = call.upper()
        call = self._normalize_numbers(call)
        call = self._normalize_decimals(call)
        # separate the digit that follows the 1st set of letters
        call = re.sub(r'^([A-Z]+)(\d)', r'\1 \2', call)
        # separate non-digits after numbers
        call = re.sub(r'(\d)([^ .\d])', r'\1 \2', call)
        call = self._numbers_to_sortable_strings(call)
        return call

    def _process_other(self, call=None):
        """
        Processes local (other) call numbers. Example: LPCD100,000 or
        LPCD 100,000 or LPCD 100000 and all other permutations become
        LPCD 0000100000.
        """
        call = self.call if call is None else call
        call = self._normalize_spaces(call)
        call = self._normalize_numbers(call)
        call = self._separate_numbers(call)
        call = self._numbers_to_sortable_strings(call)
        return call.upper()

    def _process_search(self, call=None):
        """
        This is for doing normalization of call numbers for searching.
        """
        call = self.call if call is None else call
        call = call.upper()
        call = re.sub(r'[\s./,?\-]', r'', call)
        return call

    def _process_default(self, call=None):
        """
        Default processor, used for things like copy numbers and volume
        numbers, which might have things like: V1 or Vol 1 etc., where
        we only care about the numeric portion for sorting.
        """
        call = self.call if call is None else call
        call = self._normalize_spaces(call)
        call = self._remove_labels(call, case_sensitive=False)
        call = self._normalize_numbers(call)
        call = self._normalize_decimals(call)
        call = self._separate_numbers(call)
        call = self._numbers_to_sortable_strings(call)
        return call.upper()

    def _normalize_spaces(self, data=None):
        """
        Normalzes spaces: trims left/right spaces and condenses
        multiple spaces.
        """
        data = self.call if data is None else data
        data = re.sub(r'^\s*(.*)\s*$', r'\1', data)
        data = re.sub(r'\s{2,}', ' ', data)
        return data

    def _normalize_numbers(self, data=None):
        """
        Removes commas delineating thousands and normalizes number
        ranges to remove the hyphen and the second number.
        """
        data = self.call if data is None else data
        data = re.sub(r'(\d{1,3}),(\d)', r'\1\2', data)
        data = re.sub(r'(\d)\s*\-+\s*\d+', r'\1', data)
        return data

    def _remove_labels(self, data=None, case_sensitive=True):
        """
        Removes textual content that might be immaterial to the sort.
        The case_sensitive version tries to get the "vol." and "no."
        abbreviations that slip into LC call numbers while retaining
        the capital letters that are actually part of the call number.
        But it might miss a few abbreviations, like "V. 1". The case
        insensitive version removes most of the non-numeric content,
        unless there's no non-numeric content.
        """
        data = self.call if data is None else data
        if case_sensitive:
            data = re.sub(r'(^|\s+)[A-Za-z]?[a-z]+[. ]*(\d)', r'\1\2', data)
        else:
            data = re.sub(r'(^|\s+)[A-Za-z]+[. ]*(\d)', r'\1\2', data)
        return data

    def _normalize_decimals(self, data=None):
        """
        Removes decimal points before non-digits and spaces out
        decimals that do involve digits.
        """
        data = self.call if data is None else data
        data = re.sub(r'([^ \d])\.', r'\1 .', data)
        data = re.sub(r'\.([^ \d])', r'\1', data)
        return data

    def _separate_numbers(self, data=None):
        """
        Separates numbers/decimals from non-numbers, using spaces.
        """
        data = self.call if data is None else data
        data = re.sub(r'([^ .\d])(\d)', r'\1 \2', data)
        data = re.sub(r'(\d)([^ .\d])', r'\1 \2', data)
        return data

    def _numbers_to_sortable_strings(self, data=None, decimals=True):
        """
        Formats numeric components so they'll sort as strings.
        """
        data = self.call if data is None else data
        parts = []
        for x in data.split(' '):
            if ((decimals and re.search(r'^\d*\.?\d+$', x))
                    or (not decimals and re.search(r'^\d+$', x))):
                x = '{:010d}{}'.format(
                    int(float(x)), text_type(float(x) % 1)[1:])
                x = re.sub(r'\.0$', '', x)
            parts.append(x)
        return ' '.join(parts)
