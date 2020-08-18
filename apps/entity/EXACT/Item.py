# -*- coding: utf-8 -*-

from ..EXACT import db, EXACT

class Item(EXACT):
    __tablename__ = 'items'

    Code = db.Column('itemcode')
    Assortment = db.Column()
    GLAccountDistribution = db.Column()