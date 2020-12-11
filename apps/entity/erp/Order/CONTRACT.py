# -*- coding: utf-8 -*
from sqlalchemy import func
import pandas as pd

from ...erp.common.LangMast import lang
from ....utils.functions import *
from ...erp import erp, db

class CONTRACT(erp):
    __tablename__= 'DM_D_ERP_CONTRACT'

    CostCenterCode = db.Column()
    LineNum = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column()
    Description = db.Column()
    Price = db.Column('UnitPrice')
    Unit = db.Column()
    StartDate = db.Column(db.Date)
    EndDate = db.Column(db.Date)

    @classmethod
    def list(cls, costCenterCode, date):
        if not costCenterCode or not date:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))

        qry = cls.query.filter(cls.CostCenterCode==costCenterCode,func.coalesce(cls.StartDate,'2000-1-1')<=date,
                               func.coalesce(cls.EndDate,'2222-12-31')>=date)\
            .with_entities(cls.LineNum,cls.ItemCode,
                           (cls.ItemName+' ' +cls.Description).label('ItemName'),
                           cls.Price,cls.Unit)
        return pd.read_sql(qry.statement, cls.getBind())