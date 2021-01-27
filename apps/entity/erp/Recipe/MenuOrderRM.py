# -*- coding: utf-8 -*-

from ...erp import erp, db

class MenuOrderRM(erp):
    __tablename__ = 'tblMenuOrderRMs'

    FGGuid = db.Column()
    ItemCode = db.Column()
    RequiredQty = db.Column()
    ItemPrice = db.Column()
    PurchasePolicy = db.Column()
    PurBOMConversion = db.Column()
    PurUnit = db.Column()
    BOMQty = db.Column()
    BOMUnit = db.Column()
    CreatedUser = db.Column()
