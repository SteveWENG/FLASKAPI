# -*- coding: utf-8 -*-

from ...erp import erp, db

class OrderLine(erp):
    __tablename__ = 'tblOrderLines'

    HeadGuid = db.Column()
    Guid = db.Column()
    ItemCode = db.Column()
    Qty = db.Column(db.Numeric)
    RemainQty = db.Column(db.Numeric)
    DeleteTime = db.Column(db.DateTime)