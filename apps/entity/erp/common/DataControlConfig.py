# -*- coding: utf-8 -*-
import datetime
import pandas as pd
from sqlalchemy import func, case

from ... import SaveDB
from ....utils.functions import *
from .LangMast import lang
from ...erp import erp, db


class DataControlConfig(erp):
    __tablename__ = 'tblDataControlConfig'

    Type = db.Column()
    Val1 = db.Column()
    Val2 = db.Column()
    Val3 = db.Column()
    Val4 = db.Column()
    Val5 = db.Column()
    Val6 = db.Column()
    StartDate = db.Column(db.Date)
    EndDate = db.Column(db.Date)

    @classmethod
    def list(cls,types):
        if not types:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))
        filters = [func.coalesce(cls.StartDate,'2000-1-1')<=datetime.date.today(),
                   func.coalesce(cls.EndDate,'2222-12-31')>=datetime.date.today()]
        if isinstance(types,str):
            filters.append(cls.Type==types)
        else:
            filters.append((cls.Type.in_(types)))

        sql = cls.query.filter(*filters)
        return pd.read_sql(sql.statement,cls.getBind())

    @classmethod
    def getPONumber(cls):
        ym = datetime.date.today().strftime('%y%m')
        with SaveDB() as session:
            qry = session.query(cls).filter(func.coalesce(cls.Val2, '')=='0B1ADC15-6217-4FE6-8C9A-A55BA0228BBA')
            qry.update({'Val4': case([(func.coalesce(cls.Val3,'')==ym,
                                       func.coalesce(cls.Val4,0)+1)],
                                     else_=1),
                         'Val3':ym},synchronize_session=False)
            tmp = qry.with_entities(cls.Val4).first().Val4

            return 'PO%s' %(getInt(ym) * 100000 + getInt(tmp))