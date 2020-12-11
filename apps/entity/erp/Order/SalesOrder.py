# -*- coding: utf-8 -*-
from sqlalchemy import func

from ....utils.functions import *
from ...erp import erp, db

class SalesOrderHead(erp):
    __tablename__ = 'SalesOrderHead'

    HeadGuid = db.Column()
    Status = db.Column() # not in (0,9)
    StartDate = db.Column(db.Date)
    EndDate = db.Column(db.Date)
    ExpiredDate = db.Column('ExpiryDate',db.Date)

    @classmethod
    def list(cls,costCenterCode,date):
        qry = cls.query.join(SalesOrderItem, cls.HeadGuid==SalesOrderItem.HeadGuid)\
            .filter(SalesOrderItem.CostCenterCode==costCenterCode, cls.Status !=0, SalesOrderItem.Status != 0,
                    cls.StartDate<=date, func.coalesce(cls.ExpiredDate,cls.EndDate)>=date,
                    SalesOrderItem.StartDate <= date,
                    func.coalesce(SalesOrderItem.ExpiredDate,SalesOrderItem.EndDate)>=date)\
            .with_entities(SalesOrderItem.Guid.label('orderLineGuid'),
                           SalesOrderItem.ItemCode.label('itemCode'),SalesOrderItem.ItemName.label('itemName'),
                           SalesOrderItem.Price.label('itemPrice')).all()

        return getdict(qry)

class SalesOrderItem(erp):
    __tablename__ = 'SalesOrderItem'

    HeadGuid = db.Column()
    Guid = db.Column('ItemGuid')
    CostCenterCode = db.Column()
    ItemCode = db.Column('ProductCode')
    ItemName = db.Column('ProductDesc')
    PriceUnitCode = db.Column()
    Price = db.Column(db.Numeric)
    StartDate = db.Column(db.Date)
    EndDate = db.Column(db.Date)
    ExpiredDate = db.Column('ExpiryDate', db.Numeric)
    Status = db.Column() # not in (0,9)

class PriceUnitData(erp):
    __tablename__ = 'PriceUnitData'

    name = db.Column('Name_EN')