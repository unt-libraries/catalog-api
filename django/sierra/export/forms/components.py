from __future__ import absolute_import

import re
from datetime import date

from base import models as sierra_models
from django import forms
from django.core.exceptions import ValidationError


# Validators
def validate_date_not_future(my_date):
    if my_date > date.today():
        raise ValidationError('{} is a future date.'.format(my_date))


def validate_iii_record_num(rec_num):
    if re.match(r'^[a-c,e,g,i,j,l,n-p,s,t,v]\d{5,10}$', rec_num) is None:
        raise ValidationError('Not a valid III record number.')


def validate_iii_location_codes(user_codes):
    system_codes = sierra_models.Location.objects.values('code')
    for ucode in user_codes:
        if {'code': ucode} not in system_codes:
            msg = '`{}` is not valid III location code.'.format(ucode)
            raise ValidationError(msg)


# Custom Fields
class PastDateField(forms.DateField):
    default_validators = [validate_date_not_future]


class IiiRecordNumField(forms.CharField):
    default_validators = [validate_iii_record_num]


class IiiLocationCodesField(forms.CharField):
    default_validators = [validate_iii_location_codes]

    def clean(self, value):
        value = self.to_python(value)
        self.validate(value)
        if value:
            value = value.split(',')
            self.run_validators(value)
        return value
