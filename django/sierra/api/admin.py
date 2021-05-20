from __future__ import absolute_import
from django.contrib import admin
from . import models


class APIUserAdmin(admin.ModelAdmin):
    list_display = ('user',)
    ordering = ('user__username',)
    fields = ('user', 'permissions',)


admin.site.register(models.APIUser, APIUserAdmin)
