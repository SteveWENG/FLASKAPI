# -*- coding: utf-8 -*-

import datetime

from sqlalchemy.ext.hybrid import hybrid_property

from .MenuOrderRM import MenuOrderRM
from ...erp import erp, db, CurrentUser


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
    CreatedUser = db.Column(default=CurrentUser)
    ChangedUser = db.Column(default=CurrentUser, onupdate=CurrentUser)
    ChangedTime = db.Column(default=datetime.datetime.now, onupdate=datetime.datetime.now)

    RMs = db.relationship('MenuOrderRM', primaryjoin='MenuOrderFG.FGGuid == foreign(MenuOrderRM.FGGuid)',
                          lazy='joined')