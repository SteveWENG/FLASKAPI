# -*- coding: utf-8 -*-

from ...erp import erp, db

class MenuOrderRM(erp):
    __tablename__ = 'tblMenuOrderRMs'

    FGGuid = db.Column()
    ItemCode = db.Column()
    ItemUnit = db.Column()
    RequiredQty = db.Column()
    ItemCost = db.Column()
    PurchasePolicy = db.Column()
    PurBOMConversion = db.Column()
    CreatedUser = db.Column()
