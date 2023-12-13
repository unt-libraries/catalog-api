from __future__ import absolute_import

import hashlib
# set up logger, for debugging
import logging

from rest_framework import authentication
from rest_framework import exceptions
from six import ensure_str
from utils import redisobjs as ro

from . import models

logger = logging.getLogger('sierra.custom')


class SimpleSignatureAuthentication(authentication.BaseAuthentication):

    def authenticate(self, request):
        username = request.META.get('HTTP_X_USERNAME', None)
        timestamp = request.META.get('HTTP_X_TIMESTAMP', None)
        client_signature = request.META.get(
            'HTTP_AUTHORIZATION', 'Basic '
        ).split('Basic ')[1]
        body = ensure_str(request.body)
        bad_credentials_msg = 'Incorrect username or password'
        invalid_timestamp_msg = 'Timestamp invalid.'

        if username and timestamp and client_signature:
            try:
                api_user = models.APIUser.objects.get(user__username=username)
            except models.APIUser.DoesNotExist:
                raise exceptions.AuthenticationFailed(bad_credentials_msg)
            user_timestamp = ro.RedisObject('user_timestamp', username)
            last_ts = user_timestamp.get() or 0
            if float(last_ts) >= float(timestamp):
                raise exceptions.AuthenticationFailed(invalid_timestamp_msg)
            server_signature = hashlib.sha256(
                f'{username}{api_user.secret}{timestamp}{body}'.encode('utf-8')
            ).hexdigest()
            if server_signature == client_signature:
                user_timestamp.set(str(timestamp))
                return (api_user.user, None)
            raise exceptions.AuthenticationFailed(bad_credentials_msg)
