import re
from datetime import date

from django import forms
from django.core.exceptions import ValidationError
from base import models as sierra_models


# Validators
def validate_date_not_future(my_date):
    if my_date > date.today():
       raise ValidationError('{} is a future date.'.format(my_date))

def validate_iii_record_num(rec_num):
    if re.match(r'^[a-c,e,g,i,j,l,n-p,s,t,v]\d{5,10}$', rec_num) is None:
        raise ValidationError('Not a valid III record number.')

def validate_iii_location_code(code):
    if {'code': code} not in sierra_models.Location.objects.values('code'):
        raise ValidationError('Not a valid III location code.')


# Custom Fields
class PastDateField(forms.DateField):
    default_validators = [validate_date_not_future]


class IiiRecordNumField(forms.CharField):
    default_validators = [validate_iii_record_num]


class IiiLocationCodeField(forms.CharField):
    default_validators = [validate_iii_location_code]