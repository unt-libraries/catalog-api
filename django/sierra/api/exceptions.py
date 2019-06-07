"""
Implements REST API exceptions.
"""
from rest_framework import exceptions
from rest_framework import views


def sierra_exception_handler(exc, context):
    """
    Custom exception handler. Defines what gets returned in the
    response when clients encounter errors.
    """
    response = views.exception_handler(exc, context)
    if response is not None:
        response.data['status'] = response.status_code
    return response


class BadQuery(exceptions.APIException):
    status_code = 400
    default_detail = ('One of the query parameters was not correctly '
                      'formatted.')


class BadUpdate(exceptions.APIException):
    status_code = 400
    default_detail = ('The requested resource could not be updated because '
                      'the content received was invalid.')


class ReadOnlyEdit(exceptions.APIException):
    status_code = 400
    default_detail = ('The requested resource could not be updated because '
                      'the request attempted to update read-only content.')
