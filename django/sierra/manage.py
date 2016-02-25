#!/usr/bin/env python
import os
import sys

import ujson

if __name__ == '__main__':
    with open('sierra/settings/settings.json') as f:
        local_settings = ujson.loads(f.read())
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 
                          local_settings.get('SETTINGS_MODULE'))

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
