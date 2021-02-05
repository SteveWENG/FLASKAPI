# -*- coding: utf-8 -*-
import datetime

from ...erp import erp, db, CurrentUser


class MenuOrderRM(erp):
    __tablename__ = 'tblMenuOrderRMs'

    FGGuid = db.Column()
    ItemCode = db.Column()
    RequiredQty = db.Column()
    PurPrice = db.Column()
    PurchasePolicy = db.Column()
    PurBOMConversion = db.Column()
    PurUnit = db.Column()
    BOMQty = db.Column()
    BOMUnit = db.Column()
    CreatedUser = db.Column(default=CurrentUser)
    ChangedUser = db.Column(default=CurrentUser, onupdate=CurrentUser)
    ChangedTime = db.Column(default=datetime.datetime.now, onupdate=datetime.datetime.now)
