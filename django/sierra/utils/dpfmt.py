"""
This includes some simple utilities for helping format parsed/rendered
data from blacklight/discover indexing code. This is specifically for
creating readable displays for Discover Indexing presentations.
"""

from blacklight import sierra2marc as s2m


def parse_fstr(fstr, parser_class):
    ut = s2m.MarcUtils()
    obj = parser_class(ut.fieldstring_to_field(fstr))
    look_for_ms = True
    for tag, val in obj.field:
        if look_for_ms:
            if tag == '3':
                ms_val = obj.parse_materials_specified(val)
                obj.materials_specified.append(ms_val)
            else:
                look_for_ms = False
        if obj.parse_subfield(tag, val):
            break
    return obj


def render_val(val, label='', indent=0):
    rendered = []
    if isinstance(val, (list, tuple)):
        if len(val) == 0:
            rendered.append('[]')
        else:
            nest_indent = len(label) + indent + 2
            for item in val:
                rendered_item = render_val(item, indent=nest_indent)
                rendered.append(''.join([rendered_item, ',']))
            rendered[0] = ''.join(['[ ', rendered[0].lstrip()])
            rendered[-1] = ''.join([rendered[-1][:-1], ' ]'])
    elif isinstance(val, dict):
        if len(val) == 0:
            rendered.append('{}')
        else:
            nest_indent = len(label) + indent + 2
            for key, item in val.items():
                rkey = "'{}': ".format(key)
                rendered_item = render_val(item, rkey, indent=nest_indent)
                rendered.append(''.join([rendered_item, ',']))
            rendered[0] = ''.join(['{ ', rendered[0].lstrip()])
            rendered[-1] = ''.join([rendered[-1][:-1], ' }'])
    elif isinstance(val, (str, unicode)):
        rendered.append("'{}'".format(val))
    else:
        rendered.append(str(val))
    rendered[0] = ''.join([' ' * indent, label, rendered[0].lstrip()])
    return '\n'.join(rendered)


def format_obj_state(obj, ignore=None):
    rendered = []
    ignore = ignore or []
    ignore.extend(['utils', 'field'])
    for attr in sorted(obj.__dict__.keys()):
        if attr not in set(ignore):
            label = 'self.{} => '.format(attr)
            rendered.append(render_val(getattr(obj, attr), label))
    return '\n'.join(rendered)


def do_format_sfparse(fstr, ignore=None):
    pcls = s2m.LinkingFieldParser
    obj = parse_fstr(fstr, pcls)
    render = [fstr, '', format_obj_state(obj, ignore)]
    return '\n'.join(render)

