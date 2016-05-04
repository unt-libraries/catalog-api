from .base import *

required_test_db_setting_suffixes = ('USER', 'PASSWORD', 'HOST')
for suffix in required_test_db_setting_suffixes:
    setting = 'TEST_SIERRA_DB_{}'.format(suffix)
    if local_settings.get(setting, None) is None:
        raise_setting_error(setting)

DEBUG = True

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    },
    'sierra': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'iii_test',
        'USER': local_settings['TEST_SIERRA_DB_USER'],
        'PASSWORD': local_settings['TEST_SIERRA_DB_PASSWORD'],
        'HOST': local_settings['TEST_SIERRA_DB_HOST'],
        'PORT': local_settings.get('TEST_SIERRA_DB_PORT', None),
        'TEST': {
            'NAME': 'iii_test'
        }
    }
}
