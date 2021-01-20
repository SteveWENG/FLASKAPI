# -*- coding: utf-8 -*-

from ...erp import erp, db

class Product(erp):
    __tablename__ = 'tblProduct'

    GUID = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column('ItemName_ZH')
    CategoriesClassGUID = db.Column()
