import datetime
import uuid
from decimal import Decimal
import random
import math

from flask import abort, jsonify


def getGUID():
    s4 = str(uuid.uuid4()).replace('-', '')
    s = s4[random.randint(0,31):][0:12]
    s += s4[0:(12 - len(s))]

    return str(uuid.uuid1()).replace('-', '')[0:20] + s

def getdict(obj):
    return [{k: getVal(getattr(l, k)) for k in l.keys() if getattr(l, k)} for l in obj]

def getdict_del(obj):
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
        return datetime.date(*map(int, s.split('-')))
        # return datetime.date.strptime(s, '%Y-%m-%d')  # %H:%M:%S')
    except Exception as e:
        raise e


def getInt(o):
    if getStr(o) == '':
        return 0
    try:
        return int(o)
    except Exception as e:
        raise e


def getNumber(d):
    if not d or math.isnan(d):
        return 0
    return Decimal(str(d))

def getVal(s):
    if isinstance(s,str):
        return getStr(s)
    if isinstance(s,Decimal):
        return float(getNumber(s))
    # if isinstance(s,datetime.date):
    #     return getDate(s)
    return s

def Error(error):
    raise RuntimeError(error)

def ErrorExit(error):
    abort(jsonify({'status': 500, 'error': error}))

def DataFrameSetNan(df):
    if df.empty:
        return df

    for s in df.columns[df.isna().any()]:
        df[s].fillna(0 if s in df.select_dtypes(include='number').columns else '', inplace=True)

    return df