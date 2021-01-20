# -*- coding: utf-8 -*-
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
        tmp = cls.query.filter(cls.Division==division, cls.Status=='active',
                               cls.BOMs.any(ItemBOM.DeleteTime==None))\
            .with_entities(cls.Guid,cls.BOMs).all()
        x = 1