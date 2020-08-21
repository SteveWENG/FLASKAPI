# -*- coding: utf-8 -*-

import pandas as pd
from ...erp import erp, db

class Apps(erp):
    __tablename__ = 'tblApps'

    Guid = db.Column()
    PGuid = db.Column()
    #AppName = db.Column('AppNameEN')
    ReportGuid = db.Column()
    Action = db.Column()
    ClassName = db.Column()
    Status = db.Column(db.Boolean)

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