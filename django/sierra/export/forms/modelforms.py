import re
from datetime import date

from django import forms

from ..models import ExportType, ExportFilter, ExportInstance
from .components import PastDateField, IiiRecordNumField, IiiLocationCodesField


class ExportForm(forms.ModelForm):
    date_range_from = PastDateField(required=False)
    date_range_to = PastDateField(required=False)
    record_range_from = IiiRecordNumField(required=False)
    record_range_to = IiiRecordNumField(required=False)
    location_code = IiiLocationCodesField(required=False)
    export_filter = forms.ModelChoiceField(
            queryset=ExportFilter.objects.order_by('order'), empty_label=None)
    export_type = forms.ModelChoiceField(
            queryset=ExportType.objects.order_by('order'), empty_label=None)
    
    def clean(self):
        cleaned_data = super(ExportForm, self).clean()  # validated data
        data = self.data  # original data
        filter = cleaned_data.get('export_filter')
        if re.search(r'date_range$', filter.pk):
            if not data.get('date_range_from'):
                self._errors['date_range_from'] = self.error_class(['Date '
                        'Range "From" field cannot be blank.']) 

            if not data.get('date_range_to'):
                cleaned_data['date_range_to'] = date.today()

            date_from = cleaned_data.get('date_range_from')
            date_to = cleaned_data.get('date_range_to')
            if date_from and date_to and date_from > date_to:
                self._errors['date_range_from'] = self.error_class(['Date '
                        'Range "From" field cannot be greater than Date '
                        'Range "To" field.'])
                del cleaned_data['date_range_from']

        if re.search(r'record_range$', filter.pk):
            if not data.get('record_range_from'):
                self.errors['record_range_from'] = self.error_class(['Record '
                        'Range "From" field cannot be blank.'])

            if not data.get('record_range_to'):
                self.errors['record_range_to'] = self.error_class(['Record '
                        'Range "To" field cannot be blank.'])

            record_from = cleaned_data.get('record_range_from')
            record_to = cleaned_data.get('record_range_to')
            if record_from and record_to:
                if record_from[0] != record_to[0]:
                    self.errors['record_range_from'] = self.error_class([
                            'Record Range "From" and "To" fields must be the '
                            'same III record type.'])
                if int(record_from[1:]) > int(record_to[1:]):
                    self.errors['record_range_from'] = self.error_class([
                            'Record Range "From" field cannot be greater than '
                            'Record Range "To" field.'])

        if filter.pk == 'location':
            if not data.get('location_code'):
                self.errors['location_code'] = self.error_class(['Location '
                        'Code field cannot be blank.'])

        return cleaned_data

    def stringify_params(self):
        """
        Returns a string from any filter parameters given--e.g., a date
        range or record range. The string should be put in the
        ExportInstance.filter_params field.
        """
        params = ''
        data = self.cleaned_data
        if re.search(r'date_range', data.get('export_filter').pk):
            params = '{} to {}'.format(str(data.get('date_range_from')),
                                       str(data.get('date_range_to')))
        elif re.search(r'record_range', data.get('export_filter').pk):
            params = '{} to {}'.format(data.get('record_range_from'),
                                       data.get('record_range_to'))
        elif data.get('export_filter').pk == 'location':
            params = ','.join(data.get('location_code', []))
        return params
    
    class Meta:
        model = ExportInstance
        fields = ('export_filter', 'export_type')
