# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.base import manager_of_class

from ...erp import erp, db

class Apps(erp):
    __tablename__ = 'tblApps'
    lang = 'EN'

    Guid = db.Column()
    PGuid = db.Column()
    _AppName = db.Column('AppName')
    _AppNameEN = db.Column('AppNameEN')
    _AppNameZH = db.Column('AppNameZH')
    ReportGuid = db.Column()
    Action = db.Column()
    ClassName = db.Column()
    Status = db.Column(db.Boolean)

    @hybrid_property
    def AppName(self):
        column = '_AppName' + Apps.lang
        if column[1:] in manager_of_class(Apps).mapper.mapped_table.columns.keys():
            return getattr(self, column)

        return self._AppName

    @classmethod
    def ParentApps(cls, guid):
        li = pd.read_sql(cls.query.filter(cls.Status==True).with_entities(cls.Guid, cls.PGuid).statement,cls.getBind())
        if li.empty:
            return None

        x = 0
        guids = [{'index': x,'guid': guid}]
        tmpguid = guid
        while True:
            tmp = li.loc[(li['Guid']==tmpguid)&(li['PGuid'].notnull()),'PGuid'].tolist()
            if not tmp:
                break

            x = x + 1
            tmpguid = tmp[0]
            guids.append({'index':x, 'guid': tmpguid})

        return guids