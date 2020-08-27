import datetime
import uuid
from decimal import Decimal

from flask import abort, jsonify


def getGUID():
    return str(uuid.uuid1()).replace('-', '')


def getdict(obj):
    pr = {}
    for name in dir(obj):
        value = getattr(obj, name)
        if not name.startswith('__') and not callable(value):
            pr[name] = value

    return pr


def getStr(obj):
    if obj == None:
        return ''

    return str(obj).strip()


def getDate(s):
    try:
        return datetime.datetime.strptime(s, '%Y-%m-%d')  # %H:%M:%S')
    except Exception as e:
        raise e


def getInt(o):
    if getStr(o) == '':
        return 0
    try:
        return int(o)
    except Exception as e:
        raise e


def getNumber(*d):
    # li = tuple([Decimal(str(x)) if getStr(x) != '' else 0 for x in d])
    # return li if len(li) > 1 else li[0]
    if getStr(d) == '':
        return 0
    return Decimal(str(d))

def Error(error):
    raise RuntimeError(error)

def ErrorExit(error):
    abort(jsonify({'status': 500, 'error': error}))
