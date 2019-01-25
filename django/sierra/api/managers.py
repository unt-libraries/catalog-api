"""
Custom manager for creating and updating APIUsers.
"""

import csv
import re

from django.db import models
from django.contrib.auth.models import User


class APIUserManagerError(Exception):
    pass


class UserExists(APIUserManagerError):
    pass


class UserDoesNotExist(APIUserManagerError):
    pass


class APIUserManager(models.Manager):

    def get_api_user(self, username):
        """
        Get an APIUser object by the user.username.

        Pass in a `username` string. Returns the APIUser object, or
        raises a APIUser.DoesNotExist if the user cannot be found.
        """
        return self.get(user__username=username)

    def create_user(self, username, secret_text, permissions_dict=None,
                    password=None, email='', first_name='', last_name=''):
        """
        Create, save, and return a new APIUser object.
        """
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            user = User.objects.create_user(username=username)
        try:
            api_user = user.apiuser
        except self.model.DoesNotExist:
            api_user = self.model(user=user, secret_text=secret_text,
                                  permissions_dict=permissions)
            api_user.update_and_save()
            return api_user
        raise UserExists('APIUser {} already exists.'.format(username))
