# -*- coding: utf-8 -*-
from sqlalchemy.ext.hybrid import hybrid_property

from .MenuOrderRM import MenuOrderRM
from ...erp import erp, db

class MenuOrderFG(erp):
    __tablename__ = 'tblMenuOrderFGs'

    HeadGuid = db.Column()
    FGGuid = db.Column()
    ItemGuid = db.Column()
    ItemCode = db.Column()
    ItemUnit = db.Column()
    RequiredQty = db.Column()
    ItemCost = db.Column()
    ItemColor = db.Column()
    PurchasePolicy = db.Column()
    CreatedUser = db.Column()

    RMs = db.relationship('MenuOrderRM', primaryjoin='MenuOrderFG.FGGuid == foreign(MenuOrderRM.FGGuid)',
                          lazy='joined')