# -*- coding: utf-8 -*-
import datetime
import pandas as pd
from sqlalchemy import func

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