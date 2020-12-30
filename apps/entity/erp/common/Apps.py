# -*- coding: utf-8 -*-
from copy import copy
from pandas import merge

import pandas as pd
from flask import g
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm.base import manager_of_class

from ....utils.functions import *
from ...erp import erp, db

class Apps(erp):
    __tablename__ = 'tblApps'

    Guid = db.Column()
    PGuid = db.Column()
    _AppName = db.Column('AppName')
    _AppNameEN = db.Column('AppNameEN')
    _AppNameZH = db.Column('AppNameZH')
    ReportGuid = db.Column()
    Action = db.Column()
    Sort = db.Column()
    ClassName = db.Column()
    Status = db.Column(db.Boolean)

    @hybrid_method
    def AppName(cls, lang):
        column = '_AppName' + lang.upper()
        if column[1:] in manager_of_class(cls).mapper.mapped_table.columns.keys():
            return getattr(cls, column)

        return cls._AppName

    @classmethod
    def ParentApps(cls, guid=None):
        li = pd.read_sql(cls.query.filter(cls.Status==True).
                         with_entities(cls.Guid,
                                       cls.AppName(g.get('LangCode','ZH')).label('AppName'),
                                       cls.PGuid).order_by(cls.Sort).statement,
                         cls.getBind())
        if li.empty: return None

        if not guid: return li

        x = 0
        guids = [{'index': x,'guid': guid}]
        tmpguid = getStr(guid)
        while True:
            tmp = li.loc[(li['Guid']==tmpguid)&(li['PGuid'].notnull()),'PGuid'].tolist()
            if not tmp:
                break

            x = x + 1
            tmpguid = tmp[0]
            guids.append({'index':x, 'guid': tmpguid})

        return guids

    @classmethod
    def Struct(cls):
        tmpApps = cls.ParentApps()
        DataFrameSetNan(tmpApps)
        guids = ['']
        x = 0
        id = 0
        ret = pd.DataFrame([])
        while True:
            tmp = tmpApps[tmpApps['PGuid'].isin(guids)]
            if tmp.empty:
                break

            if ret.empty:
                ret = copy(tmp[['Guid', 'AppName']])
            else:
                id += len(ret)
                ret = merge(ret, tmp, left_on='Guid%s' % (x), right_on='PGuid').drop('PGuid', axis=1)
            guids = ret['Guid'].tolist()
            x += 1
            ret.rename(columns={'Guid': 'Guid%s' % (x), 'AppName': 'Name%s' % (x)}, inplace=True)
            ret.reset_index(inplace=True)
            ret['id%s' % (x)] = id + ret.index

        def _list(index, data):
            return [{'id': getStr(g1[0]), 'guid': g1[1], 'label': g1[2],
                     **({'children': _list(index + 1, data[data['Guid%s' % (index)] == g1[1]])}
                        if 'Guid%s' % (index + 1) in data.columns else {})}
                    for g1, g2 in data.groupby(['id%s' % (index), 'Guid%s' % (index), 'Name%s' % (index)])]

        return _list(1, ret)