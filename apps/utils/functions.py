import datetime
import uuid
from contextlib import contextmanager
from decimal import Decimal
import random
import math

import pandas as pd
from pandas import DataFrame
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper


def getOrderNo():
    return datetime.datetime.now().strftime('%y%m%d%H%M%S') + str(random.randint(1,10000) + 10000)[1:]

def getGUID():
    # 修改MAC的值
    s4 = str(uuid.uuid4()).replace('-', '')
    s = s4[random.randint(0,31):][0:12]
    s += s4[0:(12 - len(s))]

    return str(uuid.uuid1()).replace('-', '')[0:20] + s

def getdict(obj, KeepKeys=[]):
    if isinstance(obj,DataFrame):
        return [{k: getVal(v) for k,v in l.items() if k in KeepKeys or  v or (isinstance(v,bool) and getStr(v) !='') }
                for l in obj.to_dict('records')]
    if isinstance(obj,dict):
        return {k:v for k,v in obj.items() if k in KeepKeys or pd.notnull(v)}

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

# str -> date
# date -> str
def getDateTime(d):
    try:
        format = ['%Y-%m-%d',' %H',':%M',':%S','.%f']
        if isinstance(d, datetime.date) and not isinstance(d, datetime.datetime):
            return d.strftime(''.join(format[:1]))

        if isinstance(d, datetime.datetime):
            x = len(format)
            if d.microsecond==0:
                x = x - 1
                if d.second==0:
                    x = x - 1
                    if d.minute==0:
                        x = x - 1
                        if d.hour==0:
                            return getDateTime(d.date())
            return d.strftime(''.join(format[:x]))

        for x in range(1,len(format)+1):
            try:
                sd = datetime.datetime.strptime(d,''.join(format[:x]))
                if x == 1:
                    sd = sd.date()
                return sd
            except:
                pass
    except Exception as e:
        raise e

def getWeekDay(name):
    weekdays = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
    return weekdays[name.capitalize()]

def isSameWeek(date1,date2):
    if date1 > date2:
        date1, date2 = date2, date1

    return (date2-date1).days < 7 and date2.weekday()>=date1.weekday()

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
    if isinstance(s,datetime.date):
        return getDateTime(s)
    if isinstance(s,str):
        return getStr(s)
    if isinstance(s,bool):
        return s
    if isinstance(s,int):
        return getInt(s)
    if isinstance(s,Decimal):
        return float(getNumber(s))

    if s == None:
        return getStr(s)

    return s

def DataFrameSetNan(df):
    if df.empty:
        return df

    for s in df.columns[df.isna().any()]:
        df[s].fillna(0 if s.lower()=='id' or s in df.select_dtypes(include='number').columns else '', inplace=True)

    return df

def DataFrameDoubleColName(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup
                                                         for i in range(sum(cols == dup))]
    df.columns = cols
    return df

def getModel(table, engine):
    """根据name创建并return一个新的model类
    name:数据库表名
    engine:create_engine返回的对象，指定要操作的数据库连接，from sqlalchemy import create_engine
    """
    Base = declarative_base()
    Base.metadata.reflect(engine)
    # table = Base.metadata.tables[name]
    t = type(table.name,(object,),dict())
    mapper(t, table)
    Base.metadata.clear()
    return t

def Error(error):
    raise RuntimeError(error)

@contextmanager
def RunApp():
    try:
        yield
    except Exception as e:
        raise e
