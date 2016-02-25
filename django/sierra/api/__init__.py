import csv
import re

from .models import APIUser
from django.contrib.auth.models import User


class APIUserManagerError(Exception):
    pass


class UserExists(APIUserManagerError):
    pass


class UserDoesNotExist(APIUserManagerError):
    pass


def get_api_user(username):
    api_user = None
    try:
        api_user = APIUser.objects.get(user__username=username)
    except APIUser.DoesNotExist:
        raise APIUserManagerError('APIUser {} does not exist.'
                                  ''.format(username))
        
    return api_user


def create_api_user(username, secret, permissions=None, default=False, 
                    email='', django_password=None, first_name='',
                    last_name=''):
    user = None
    api_user = None

    try:
        api_user = get_api_user(username)
    except APIUserManagerError:
        pass
    else:
        raise UserExists('APIUser {} already exists.'.format(username))

    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        if not django_password:
            raise UserDoesNotExist('Django User {} does not exist and '
                                   'must be created. You must supply a '
                                   'django_password argument.'
                                   ''.format(username))
        user = User.objects.create_user(username, email, django_password)
        user.first_name = first_name
        user.last_name = last_name
        user.save()

    api_user = APIUser(user=user)
    api_user.set_secret(secret)
    api_user.set_permissions(permissions=permissions, default=default)
    return api_user


def update_api_user(username, secret=None, permissions=None, default=False,
                    email='', django_password=None, first_name='',
                    last_name=''):
    try:
        api_user = get_api_user(username)
    except APIUserManagerError:
        raise UserDoesNotExist('API User {} does not exist.'.format(username))

    if (secret):
        api_user.set_secret(secret)
    if (permissions):
        api_user.set_permissions(permissions=permissions, default=default)
        
    django_user_changed = False
    if (email and email != api_user.user.email):
        api_user.user.email = email
        django_user_changed = True
    if (first_name and first_name != api_user.user.first_name):
        api_user.user.first_name = first_name
        django_user_changed = True
    if (last_name and last_name != api_user.user.last_name):
        api_user.user.last_name = last_name
        django_user_changed = True
    if (django_password and not api_user.user.check_password(django_password)):
        api_user.user.set_password(django_password)
        django_user_changed = True

    if django_user_changed:
        api_user.user.save()
    return api_user


def set_api_user_secret(username, secret):
    api_user = get_api_user(username)
    api_user.set_secret(secret)
    return api_user


def set_api_user_permissions(username, permissions, default=False):
    api_user = get_api_user(username)
    api_user.set_permissions(permissions=permissions, default=default)
    return api_user


def batch_create_update_api_users(filepath, default=None):
    """
    This will open a csv file provided by filepath and batch create API
    users based on its contents. Column names should be provided that
    match the args to create_api_user (except default). If the API user
    already exists, then this will attempt to update the secret and the
    permissions for that user.
    """
    data = []
    with open(filepath, 'r') as csvfile:
        permreader = csv.DictReader(csvfile)
        for row in permreader:
            row_data = {}
            try:
                row_data['username'] = row['username']
            except KeyError:
                raise APIUserManagerError('Your csv file must include a '
                                          '"username" column.')
            row_data['secret'] = row.get('secret', None)
            row_data['django_password'] = row.get('django_password', None)
            row_data['first_name'] = row.get('first_name', None)
            row_data['last_name'] = row.get('last_name', None)
            row_data['email'] = row.get('email', None)

            permissions = {}

            for key, val in row.iteritems():
                if re.match(r'^permission_', key):
                    if val in ('True', 'true', 'TRUE', 'T', 't', '1'):
                        val = True
                    else:
                        val = False
                    permissions[re.sub(r'^permission_', '', key)] = val

            if not permissions and default is not None:
                permissions = default

            row_data['permissions'] = permissions
            data.append(row_data)

    for row in data:
        try:
            create_api_user(row['username'], row['secret'], 
                            permissions=row['permissions'],
                            email=row['email'],
                            django_password=row['django_password'],
                            first_name=row['first_name'],
                            last_name=row['last_name'], default=default)
        except UserExists:
            update_api_user(row['username'], row['secret'],
                            permissions=row['permissions'],
                            email=row['email'],
                            django_password=row['django_password'],
                            first_name=row['first_name'],
                            last_name=row['last_name'], default=default)
        except UserDoesNotExist:
            print ('User {} does not exist and no django_password was '
                    'provided. Skipping.'.format(row['username']))
