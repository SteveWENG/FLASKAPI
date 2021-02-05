# -*- coding: utf-8 -*-

import datetime

from ...erp import erp, db

class ItemBOM(erp):
    __tablename__ = 'tblItemBOM'

    CostCenterCode = db.Column()
    ProductGuid = db.Column()
    ItemCode = db.Column()
    OtherName = db.Column()
    ItemCost = db.Column()
    ConversionRate = db.Column()
    UOM = db.Column()
    Qty = db.Column()
    Type = db.Column()
    PurchasePolicy = db.Column()
    CreatedUser = db.Column()
    CreatedTime = db.Column(server_default='getdate()')
    ChangedUser = db.Column()
    ChangedTime = db.Column(default=datetime.datetime.now, onupdate=datetime.datetime.now)
    DeleteTime = db.Column()

    @classmethod
    def listCols(cls, fortype):
        fields = [cls.CostCenterCode,cls.ItemCode,
                  cls.OtherName,cls.Qty,cls.PurchasePolicy]
        if fortype == 'recipe':
            fields += [cls.ItemCost, cls.UOM,cls.ConversionRate]
        else:
            fields.append(cls.Id)

        return fields