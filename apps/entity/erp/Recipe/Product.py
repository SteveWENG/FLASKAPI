# -*- coding: utf-8 -*-

import pandas as pd

from ..Item.PriceList import PriceList
from ..common.CostCenter import CostCenter
from ...erp import erp, db
from .ItemBOM import ItemBOM

class Product(erp):
    __tablename__ = 'tblProduct'

    Division = db.Column('company')
    Guid = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column('ItemName_ZH')
    CategoriesClassGUID = db.Column()
    Status = db.Column()

    BOMs = db.relationship('ItemBOM', primaryjoin='Product.Guid == foreign(ItemBOM.ProductGuid)',
                          lazy='joined')

    @classmethod
    def list(cls,division=None, costCenterCode=None, date=None):
        if not division:
            division = CostCenter.GetDivision(costCenterCode)
        items = PriceList.list(division,costCenterCode,date,'Food')
        sql = cls.query.filter(cls.Division==division, cls.Status=='active',
                               ItemBOM.DeleteTime==None)\
            .join(ItemBOM,cls.Guid==ItemBOM.ProductGuid)\
            .with_entities(cls.Guid.label('ProductGuid'),cls.ItemCode.label('ProductCode'),
                           cls.ItemName.label('ProductName'),ItemBOM.ItemCode,
                           ItemBOM.OtherName,ItemBOM.Qty)
        df = pd.read_sql(sql.statement,cls.getBind())
        x = 1