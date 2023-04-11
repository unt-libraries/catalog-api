from .base import *

# Check required TEST settings
required = ['TEST_SIERRA_DB_USER', 'TEST_SIERRA_DB_PASSWORD',
            'TEST_DEFAULT_DB_USER', 'TEST_DEFAULT_DB_PASSWORD']

for setting in required:
    if get_env_variable(setting) is None:
        raise_setting_error(setting)

DEBUG = True
TESTING = True

TIME_ZONE = 'America/Chicago'

ALLOWED_HOSTS += (
    'testserver',
)

INSTALLED_APPS += (
    'base.tests.vcftestmodels',
    'sierra.tests.testmodels',
    'sierra'
)

DATABASES = {
    'sierra': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': get_env_variable('TEST_SIERRA_DB_NAME', 'sierra_test'),
        'USER': get_env_variable('TEST_SIERRA_DB_USER'),
        'PASSWORD': get_env_variable('TEST_SIERRA_DB_PASSWORD'),
        'HOST': get_env_variable('TEST_SIERRA_DB_HOST', '127.0.0.1'),
        'PORT': get_env_variable('TEST_SIERRA_DB_PORT', '5432'),
        'TEST': {
            'NAME': get_env_variable('TEST_SIERRA_DB_NAME', 'sierra_test')
        }
    },
    'default': {
        'ENGINE': get_env_variable('TEST_DEFAULT_DB_ENGINE',
                                   'django.db.backends.mysql'),
        'NAME': get_env_variable('TEST_DEFAULT_DB_NAME', 'capi_test'),
        'USER': get_env_variable('TEST_DEFAULT_DB_USER'),
        'PASSWORD': get_env_variable('TEST_DEFAULT_DB_PASSWORD'),
        'HOST': get_env_variable('TEST_DEFAULT_DB_HOST', '127.0.0.1'),
        'PORT': get_env_variable('TEST_DEFAULT_DB_PORT', '3307'),
        'TEST': {
            'NAME': get_env_variable('TEST_DEFAULT_DB_NAME', 'capi_test')
        }
    }
}

# CELERY settings
REDIS_CELERY_PORT = get_env_variable('TEST_REDIS_CELERY_PORT', '6279')
REDIS_CELERY_HOST = get_env_variable('TEST_REDIS_CELERY_HOST', '127.0.0.1')
redis_celery_url = f'redis://{REDIS_CELERY_HOST}:{REDIS_CELERY_PORT}/0'
CELERY_BROKER_URL = redis_celery_url
CELERY_RESULT_BACKEND = redis_celery_url
CELERY_WORKER_SEND_TASK_EVENTS = True

# Other Redis settings
REDIS_CONNECTION = {
    'host': get_env_variable('TEST_REDIS_APPDATA_HOST', '127.0.0.1'),
    'port': get_env_variable('TEST_REDIS_APPDATA_PORT', '6280'),
    'db': get_env_variable('TEST_REDIS_APPDATA_DATABASE', 0)
}

# Solr settings
SOLR_PORT = get_env_variable('TEST_SOLR_PORT', '8883')
SOLR_HOST = get_env_variable('TEST_SOLR_HOST', '127.0.0.1')
solr_base_url = f'http://{SOLR_HOST}:{SOLR_PORT}/solr'
solr_haystack_url = get_env_variable('SOLR_HAYSTACK_URL',
                                     f'{solr_base_url}/haystack')
solr_d01_url = f'{solr_base_url}/discover-01'
solr_d02_url = f'{solr_base_url}/discover-02'

HAYSTACK_CONNECTIONS['default']['URL'] = solr_haystack_url
HAYSTACK_CONNECTIONS['haystack']['URL'] = solr_haystack_url
HAYSTACK_CONNECTIONS['discover-01']['URL'] = solr_d01_url
HAYSTACK_CONNECTIONS['discover-02']['URL'] = solr_d02_url
