from django.contrib import admin
from django.http import HttpResponseRedirect, HttpResponse
from django.core.urlresolvers import reverse
from django.shortcuts import get_object_or_404, render
from django.utils import timezone as tz

from .models import ExportType, ExportFilter, ExportInstance, Status
from .forms.modelforms import ExportForm
from .tasks import trigger_export

def process_export_form(request):
    '''
    Takes a request object and validates/parses/processes POSTed form
    data from an ExportForm. Returns the validated form object.
    '''
    form = ExportForm(request.POST)
    post = request.POST
    if form.is_valid():
        params = form.stringify_params()
        export_instance = form.save(commit=False)
        export_instance.user = request.user
        export_instance.filter_params = params
        export_instance.timestamp = tz.now()
        export_instance.status = Status.objects.get(pk='in_progress')
        export_instance.save()
    return form


class ExportFilterAdmin(admin.ModelAdmin):
    list_display = ('code', 'label', 'order')
    ordering = ('order',)


class ExportTypeAdmin(admin.ModelAdmin):
    list_display = ('code', 'path', 'label', 'order')
    ordering = ('order',)


class ExportInstanceAdmin(admin.ModelAdmin):
    list_display = ('export_filter', 'export_type', 'user', 'timestamp',
                    'status', 'errors', 'warnings')
    readonly_fields = ('status', 'export_type', 'export_filter',
                        'filter_params', 'user', 'timestamp', 'errors',
                        'warnings')
    list_filter = ('user', 'status', 'export_type',)
    ordering = ('-timestamp',)
    change_list_template = 'admin/export_instance_changelist.html'
    change_form_template = 'admin/export_instance_changeform.html'
    class Media:
        css = {
            'all': ('export/admin_styles.css',)
        }

    def add_view(self, request, form_url='', extra_content=None):
        model = self.model
        opts = model._meta
        if request.method == 'GET':
            form = ExportForm()
            response = render(request, 'admin/trigger_export.html', {
                'form': form,
                'opts': opts
            })
        elif request.method == 'POST':
            form = process_export_form(request)
            if form.is_valid():
                data = form.cleaned_data
                export_filter = data['export_filter'].pk
                export_type = data['export_type'].pk
                del data['export_filter']
                del data['export_type']
                trigger_export(form.instance, export_filter,
                               export_type, data)
                reverse_url = 'admin:{}_{}_change'.format(
                    self.model._meta.app_label,
                    self.model._meta.model_name
                )
                response = HttpResponseRedirect(reverse(reverse_url,
                    args=(form.instance.pk, )))
            else:
                response = render(request, 'admin/trigger_export.html', {
                    'form': form,
                    'opts': opts
                })
        return response


class StatusAdmin(admin.ModelAdmin):
    list_display = ('code', 'label')


admin.site.register(ExportType, ExportTypeAdmin)
admin.site.register(ExportFilter, ExportFilterAdmin)
admin.site.register(ExportInstance, ExportInstanceAdmin)
admin.site.register(Status, StatusAdmin)