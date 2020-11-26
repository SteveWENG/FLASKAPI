# -*- coding: utf-8 -*-
import datetime

from ...erp import erp, db

class OrderLine(erp):
    __tablename__ = 'tblOrderLines'

    HeadGuid = db.Column()
    SupplierCode = db.Column()
    ItemCode = db.Column()
    ItemTax = db.Column(db.Numeric)
    PurchasePrice = db.Column('ItemPrice',db.Numeric)
    PurStk_Conversion = db.Column(db.Numeric)
    PurchaseUnit = db.Column('UOM')
    StockUnit = db.Column('Stock_Unit')
    Qty = db.Column(db.Numeric)
    AdjQty = db.Column(db.Numeric)
    RemainQty = db.Column(db.Numeric)
    Remark = db.Column()
    CreateTime = db.Column(db.DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    Guid = db.Column(server_default='newid()')
    Status = db.Column(default='Created')
    DeleteTime = db.Column(db.DateTime)

