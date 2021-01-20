# -*- coding: utf-8 -*-

from ...erp import erp, db

class ItemBOM(erp):
    __tablename__ = 'tblItemBOM'

    CostCenterCode = db.Column()
    ProductGuid = db.Column()
    ItemCode = db.Column()
    OtherName = db.Column()
    ItemCost = db.Column()
    UOM = db.Column()
    Qty = db.Column()
    Type = db.Column()
    PurchasePolicy = db.Column()
    CreateUser = db.Column()
    DeleteTime = db.Column()