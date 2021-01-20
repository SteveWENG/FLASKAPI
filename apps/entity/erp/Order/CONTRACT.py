# -*- coding: utf-8 -*
from sqlalchemy import func
import pandas as pd

from ..common.DataControlConfig import DataControlConfig
from ...erp.common.LangMast import lang
from ....utils.functions import *
from ...erp import erp, db

class CONTRACT(erp):
    __tablename__= 'DM_D_ERP_CONTRACT'

    CostCenterCode = db.Column()
    LineNum = db.Column()
    guid = db.Column()
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
            .with_entities(cls.guid,cls.ItemCode,
                           (cls.ItemName+' ' +cls.Description).label('ItemName'),
                           cls.Price,cls.Unit)
        return pd.read_sql(qry.statement, cls.getBind())

    @classmethod
    def meals(cls, costCenterCode, startDate, endDate):
        if not costCenterCode or not startDate or not endDate:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))

        sql = DataControlConfig.list('MICReport')['Val1']
        sql = cls.query.filter(cls.CostCenterCode == costCenterCode,cls.ItemCode.in_(sql),
                               cls.StartDate <= endDate, cls.EndDate >= startDate)\
            .with_entities(cls.guid, cls.ItemName.label('SOItemName'),
                           cls.Description.label('SOItemDesc'),
                           cls.StartDate, cls.EndDate)
        return pd.read_sql(sql.statement,cls.getBind())