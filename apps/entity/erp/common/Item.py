# -*- coding: utf-8 -*-

from ...erp import erp, db

class Item(erp):
    __tablename__ = 'DM_D_ERP_ITEM'

    ItemCode = db.Column()
    ItemName = db.Column('ItemName_ZH')
    Category01 = db.Column()
    Category02 = db.Column()
    Category03 = db.Column()
